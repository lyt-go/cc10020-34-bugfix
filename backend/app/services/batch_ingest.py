"""批量接入调度领域服务：定时批量拉取、分页控制、断点续传与任务编排。

支持基于Cron表达式的定时拉取、分页遍历、并行批次调度和
断点续传能力。拉取的数据经过转换后流入数据湖。
跨模块关联：api_endpoint（拉取源端点）、data_transform（数据转换）、data_lake_sink（落湖）、incre_cursor（增量标记）。
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

from app.persistence import json_store
from app.services import transform_release
from app.services.base import BaseModuleService


class BatchIngestService(BaseModuleService):
    MODULE = "batch_ingest"
    ENTITY = "接入任务"
    OPERATION_TERM = "入库"
    LIST_FIELDS = ["任务编号", "关联端点", "Cron表达式", "分页策略", "并行批次", "断点续传", "失败动作", "任务版本"]
    REQUIRED_FIELDS = ["任务编号", "关联端点", "Cron表达式"]
    STATUS_ORDER = ["草稿", "待校验", "运行中", "已暂停"]
    ACTION_RULES = {"提交校验": "待校验", "启动任务": "运行中", "暂停任务": "已暂停"}
    NEGATIVE_ACTIONS = []

    def __init__(self) -> None:
        super().__init__()
        self._checkpoints: dict[int, int] = {}
        self._execution_history: list[dict[str, Any]] = []

    def execute_batch(
        self,
        task_id: int,
        max_pages: int = 5,
        page_size: int = 20,
        *,
        transform_rule_id: int | None = None,
        batch_id: str | None = None,
        source_records: list[dict[str, Any]] | None = None,
        retry: bool = False,
    ) -> dict[str, Any]:
        """执行批量拉取任务。

        完整流程：
        1. 验证任务状态，确定转换规则
        2. 从上次断点续传位置开始，逐页拉取数据
        3. 对每页数据做转换 + 质量校验，留下转换放行决策
        4. 只有“已放行”的决策才写入正式数据（数据湖）
        5. 用转换放行决策核算本批成功数，并和质量报告的失败数对账
        6. 更新断点位置和增量水位

        Returns:
            { ok, pages_processed, records_fetched, records_sunk, release_summary, ... }
        """
        task = json_store.get_by_id(self.MODULE, task_id)
        if task is None:
            return {"ok": False, "message": f"任务 {task_id} 不存在", "pages_processed": 0}

        if task.get("status") != "运行中":
            return {"ok": False, "message": f"任务状态{task['status']}，无法执行", "pages_processed": 0}

        rule_id = self._resolve_transform_rule(task, transform_rule_id)
        identity_rule = rule_id is None
        if identity_rule and transform_rule_id is not None:
            return {
                "ok": False,
                "message": f"转换规则 {transform_rule_id} 不存在或未生效，无法批量转换",
                "pages_processed": 0,
                "records_fetched": 0,
                "records_sunk": 0,
            }

        endpoint_id = int(task.get("关联端点", "0") or "0")
        enable_checkpoint = str(task.get("断点续传", "否")) in ("是", "true", "True", "Y")
        on_failure = str(task.get("失败动作", "记录并继续"))
        batch_id = batch_id or transform_release.new_batch_id("TCBATCH")

        start_page = self._checkpoints.get(task_id, 1) if enable_checkpoint else 1

        total_fetched = 0
        total_sunk = 0
        pages_processed = 0
        errors = []
        submitted = 0

        pages = (
            [(1, source_records)]
            if source_records is not None
            else [(page, None) for page in range(start_page, start_page + max_pages)]
        )

        for page, explicit_records in pages:
            pages_processed += 1

            raw_data = explicit_records if explicit_records is not None else self._mock_fetch_page(endpoint_id, page, page_size)
            if not raw_data:
                break

            total_fetched += len(raw_data)
            prepared = self._prepare_records(raw_data, endpoint_id)
            submitted += len(prepared)

            transform_result = self._transform_batch(rule_id, prepared, batch_id, retry=retry)
            if not transform_result.get("ok") and not transform_result.get("summary", {}).get("total"):
                errors.append(transform_result.get("message", "批量转换失败"))
                if on_failure == "中断任务":
                    break

            for result in transform_result.get("results", []):
                if not result.get("released"):
                    continue  # 阻断行只留在决策台账，不进入正式数据
                decision_id = result.get("decision_id")
                if not transform_release.claim_formal_write(decision_id):
                    continue  # 并发提交时同一份合格结果只落湖一次
                write = self._sink_decision(endpoint_id, result)
                if write.get("ok"):
                    transform_release.mark_formal_written(decision_id, {
                        "file_path": write.get("file_path"),
                        "partition": write.get("partition"),
                    })
                    total_sunk += 1
                else:
                    transform_release.release_formal_claim(decision_id)
                    errors.append(f"落湖失败: {write.get('message')}")
                    if on_failure == "中断任务":
                        break

            if enable_checkpoint and explicit_records is None:
                self._checkpoints[task_id] = page + 1

            if explicit_records is None:
                time.sleep(0.01)

        if source_records is None:
            self._update_incre_cursor(task_id, endpoint_id, total_fetched)

        summary = transform_release.batch_summary(batch_id)
        prior = transform_release.get_batch_record(batch_id) or {}
        transform_release.upsert_batch(batch_id, {
            "task_id": task_id,
            "rule_id": rule_id,
            "endpoint_id": endpoint_id,
            "status": "已完成",
            # total_submitted 由 batch_transform 按实际提交量累加，这里只补拉取/落湖统计。
            "records_fetched": int(prior.get("records_fetched", 0) or 0) + total_fetched,
            "records_sunk": int(prior.get("records_sunk", 0) or 0) + total_sunk,
            "retry": retry,
        })
        summary = transform_release.batch_summary(batch_id)

        reconciliation = self._reconcile(batch_id, summary)
        contaminated = summary["blocked"] and summary["formal_written"] > 0
        if contaminated:
            errors.append(
                f"正式数据里发现 {summary['formal_written']} 条写入记录中含阻断行，需回查"
            )
        if reconciliation["mis_counted_source_rows"]:
            errors.append(
                f"质量报告失败数与批次成功明细不一致，可回查 "
                f"{len(reconciliation['mis_counted_source_rows'])} 条错误算入成功的源行"
            )

        json_store.update(self.MODULE, task_id, {
            "last_batch_id": batch_id,
            "last_executed_at": datetime.now(timezone.utc).isoformat(),
            "total_processed": int(task.get("total_processed", 0)) + total_fetched,
        })

        self._execution_history.append({
            "task_id": task_id,
            "batch_id": batch_id,
            "pages": pages_processed,
            "fetched": total_fetched,
            "sunk": total_sunk,
            "released": summary["released"],
            "blocked": summary["blocked"],
            "errors": len(errors),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

        return {
            "ok": summary["blocked"] == 0 and len([e for e in errors if "落湖失败" in e]) == 0,
            "message": (
                f"批量接入完成: {pages_processed} 页, 拉取 {total_fetched} 条, "
                f"放行 {summary['released']} 条, 阻断 {summary['blocked']} 条, 落湖 {total_sunk} 条"
            ),
            "batch_id": batch_id,
            "批次编号": batch_id,
            "pages_processed": pages_processed,
            "records_fetched": total_fetched,
            "records_sunk": total_sunk,
            "success": summary["released"],
            "failed": summary["blocked"],
            "blocked": summary["blocked"],
            "release_summary": summary,
            "quality_report_failed": reconciliation["quality_report_failed"],
            "batch_success": reconciliation["batch_success"],
            "mis_counted_source_rows": reconciliation["mis_counted_source_rows"],
            "errors": errors,
        }

    def _resolve_transform_rule(self, task: dict[str, Any], explicit: int | None) -> int | None:
        if explicit is not None:
            rule = json_store.get_by_id("data_transform", explicit)
            return int(rule["id"]) if rule else None
        for field in ("转换规则", "转换规则ID", "transform_rule_id"):
            raw = task.get(field)
            if raw not in (None, ""):
                try:
                    rule = json_store.get_by_id("data_transform", int(raw))
                except (TypeError, ValueError):
                    rule = None
                if rule and rule.get("status") == "已生效":
                    return int(rule["id"])
        for rule in json_store.get_all("data_transform"):
            if rule.get("status") == "已生效":
                return int(rule["id"])
        return None

    def _prepare_records(self, raw_data: list[dict[str, Any]], endpoint_id: int) -> list[dict[str, Any]]:
        """把一页原始数据展开成主记录 + 明细的关联记录。"""
        prepared: list[dict[str, Any]] = []
        for record in raw_data:
            if str(record.get("记录类型") or ""):
                prepared.append({**record, "source_endpoint": endpoint_id})
                continue
            prepared.extend(self._split_linked_record(record, endpoint_id))
        return prepared

    def _split_linked_record(self, record: dict[str, Any], endpoint_id: int) -> list[dict[str, Any]]:
        """订单类数据拆成“订单主记录 + 订单明细”两份关联记录。"""
        order_no = str(record.get("order_id") or record.get("订单号") or record.get("id"))
        master = {
            **record,
            "source_endpoint": endpoint_id,
            "记录类型": "订单主记录",
            "order_id": order_no,
            "business_key": order_no,
        }
        detail = {
            **record,
            "id": f"{record.get('id')}-D1",
            "source_endpoint": endpoint_id,
            "记录类型": "订单明细",
            "order_id": order_no,
            "business_key": order_no,
            "明细行号": "1",
            "quantity": record.get("quantity", 1),
        }
        return [master, detail]

    def _sink_decision(self, endpoint_id: int, result: dict[str, Any]) -> dict[str, Any]:
        try:
            from app.services.data_lake_sink import DataLakeSinkService
            sink_svc = DataLakeSinkService()
            decision = result.get("decision") or {}
            return sink_svc.write_event(
                f"{result.get('batch_id')}:{result.get('decision_id')}",
                "batch_ingest",
                {
                    **result.get("data", {}),
                    "记录类型": result.get("record_type"),
                    "order_id": result.get("business_key") or result.get("data", {}).get("order_id"),
                    "_release": {
                        "decision_id": result.get("decision_id"),
                        "decision_no": result.get("decision_no"),
                        "batch_id": result.get("batch_id"),
                        "record_type": result.get("record_type"),
                        "business_key": result.get("business_key"),
                        "field_checks": decision.get("field_checks"),
                        "quality_check": result.get("quality_check"),
                    },
                },
                source_channel_id=endpoint_id,
            )
        except Exception as e:
            return {"ok": False, "message": str(e)}

    def _reconcile(self, batch_id: str, summary: dict[str, Any]) -> dict[str, Any]:
        decisions = transform_release.list_decisions(batch_id=batch_id)
        quality_failed = [
            row for row in decisions
            if int((row.get("quality_check") or {}).get("failed", 0) or 0) > 0
        ]
        mis_counted = [
            row for row in quality_failed
            if row.get("released") and row.get("counted", True) is not False
            and bool((row.get("quality_check") or {}).get("blocked"))
        ]
        return {
            "batch_id": batch_id,
            "quality_report_failed": len(quality_failed),
            "batch_success": summary["released"],
            "mis_counted_source_rows": [
                {
                    "decision_id": row.get("id"),
                    "record_type": row.get("record_type"),
                    "business_key": row.get("business_key"),
                    "source_input": row.get("source_input"),
                    "quality_check": row.get("quality_check"),
                }
                for row in mis_counted
            ],
        }

    def rework_batch(self, batch_id: str, *, rule_id: int | None = None) -> dict[str, Any]:
        """对批次里被阻断的记录补做转换。

        补做沿用原决策的历史字段映射，并带回原字段校验结论和原输入；
        只有补做后放行的记录才写入正式数据，原批次数据保持可追溯。
        """
        blocked = transform_release.list_decisions(batch_id=batch_id, blocked=True)
        blocked = [row for row in blocked if not row.get("superseded_by")]
        if not blocked:
            return {
                "ok": True,
                "message": "该批次没有需要补做的阻断记录",
                "batch_id": batch_id,
                "reworked": 0,
                "released": 0,
                "still_blocked": 0,
            }

        from app.services.data_transform import DataTransformService
        transform_svc = DataTransformService()
        effective_rule = rule_id or next((row.get("rule_id") for row in blocked if row.get("rule_id")), None)
        if effective_rule is None:
            return {"ok": False, "message": "无法确定补做使用的转换规则", "batch_id": batch_id}

        retry_batch = transform_release.new_batch_id("TCREWORK")
        reworked = []
        released = 0
        still_blocked = 0
        endpoint_id = self._batch_endpoint(batch_id)
        for original in blocked:
            result = transform_svc.apply_transform(
                int(effective_rule),
                original.get("source_input") or {},
                record_type=original.get("record_type"),
                business_key=original.get("business_key"),
                batch_id=retry_batch,
                retry_of=original.get("id"),
            )
            reworked.append(result)
            if result.get("released"):
                # 补做可能命中已有的合格结果（duplicate），这时不重复计数。
                if not result.get("duplicate"):
                    released += 1
                decision_id = result.get("decision_id")
                if transform_release.claim_formal_write(decision_id):
                    write = self._sink_decision(endpoint_id, result)
                    if write.get("ok"):
                        transform_release.mark_formal_written(decision_id, write)
                    else:
                        transform_release.release_formal_claim(decision_id)
            else:
                still_blocked += 1

        transform_release.upsert_batch(retry_batch, {
            "rule_id": effective_rule,
            "total_submitted": len(blocked),
            "status": "已完成",
            "retry": True,
            "retry_of_batch_id": batch_id,
        })
        summary = transform_release.batch_summary(retry_batch)
        return {
            "ok": still_blocked == 0,
            "message": f"补做完成: 放行 {released}/{len(blocked)}",
            "batch_id": retry_batch,
            "retry_of_batch_id": batch_id,
            "reworked": len(blocked),
            "released": released,
            "still_blocked": still_blocked,
            "summary": summary,
            "results": reworked,
        }

    def _batch_endpoint(self, batch_id: str) -> int:
        record = transform_release.get_batch_record(batch_id) or {}
        if record.get("endpoint_id") not in (None, ""):
            return int(record["endpoint_id"])
        task = json_store.get_by_id(self.MODULE, int(record.get("task_id", 0) or 0))
        return int((task or {}).get("关联端点", "0") or "0")

    def get_batch_release(self, batch_id: str) -> dict[str, Any]:
        from app.services.data_transform import DataTransformService
        return DataTransformService().get_batch_release(batch_id)

    def _mock_fetch_page(self, endpoint_id: int, page: int, size: int) -> list[dict[str, Any]]:
        """模拟从API拉取一页订单数据（主记录金额可能超限，明细按自己的规则校验）。"""
        if page > 10:
            return []
        import random
        rows = []
        for i in range(min(size, 20)):
            seq = (page - 1) * size + i + 1
            rows.append({
                "id": seq,
                "order_id": f"ORD-{endpoint_id}-{page:02d}-{i:02d}",
                "name": f"record_{endpoint_id}_{page}_{i}",
                "type": "api_data",
                "amount": random.randint(1, 1000) if i % 5 else 20000 + i,
                "quantity": random.randint(1, 5),
                "category": random.choice(["A", "B", "C"]),
                "status": random.choice(["active", "pending", "done"]),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })
        return rows

    def _transform_batch(
        self,
        rule_id: int,
        records: list[dict[str, Any]],
        batch_id: str,
        *,
        retry: bool = False,
    ) -> dict[str, Any]:
        """批量转换数据，返回带放行决策的结果。"""
        try:
            from app.services.data_transform import DataTransformService
            transform_svc = DataTransformService()
            return transform_svc.batch_transform(
                rule_id=rule_id,
                source_records=records,
                batch_id=batch_id,
                retry=retry,
            )
        except Exception as e:
            return {"ok": False, "message": f"批量转换异常: {e}", "results": [], "summary": {}}

    def _update_incre_cursor(self, task_id: int, endpoint_id: int, fetched_count: int) -> None:
        """更新增量水位。"""
        try:
            from app.services.incre_cursor import IncreCursorService
            cursor_svc = IncreCursorService()
            cursor_svc.update_position(endpoint_id, fetched_count)
        except Exception:
            pass

    def get_task_progress(self, task_id: int) -> dict[str, Any]:
        """获取任务的执行进度。"""
        task = json_store.get_by_id(self.MODULE, task_id)
        if task is None:
            return {"ok": False, "message": "任务不存在"}

        checkpoint = self._checkpoints.get(task_id, 0)
        history = [h for h in self._execution_history if h["task_id"] == task_id]

        return {
            "ok": True,
            "task_id": task_id,
            "task_no": task.get("任务编号"),
            "status": task.get("status"),
            "checkpoint_page": checkpoint,
            "total_processed": task.get("total_processed", 0),
            "execution_count": len(history),
            "last_execution": history[-1] if history else None,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        """输出批量接入调度的数据质量快照。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "批量接入调度数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "total_executions": len(self._execution_history),
            "active_tasks": sum(1 for r in rows if r.get("status") == "运行中"),
            "operation": self.OPERATION_TERM,
        }