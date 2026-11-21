"""接入质量校验领域服务：字段级校验、完整性检查、阈值告警与阻断策略。

提供转换后数据的质量校验能力，包括必填字段检查、类型校验、
数值范围校验和完整性评分。
跨模块关联：data_transform（转换后校验）、batch_ingest（批量质量检查）。
"""
from __future__ import annotations

import json
import re
import threading
import uuid
from datetime import datetime, timezone
from typing import Any

from app.persistence import json_store
from app.services.base import BaseModuleService

VALIDATION_KEY = "data_quality_validation"
_VALIDATION_LOCK = threading.RLock()


def _as_record_types(raw: Any) -> set[str]:
    """解析质量规则适用的记录类型（历史规则为空表示适用全部）。"""
    if isinstance(raw, (list, tuple, set)):
        values = raw
    else:
        text = str(raw or "").strip()
        if not text:
            return set()
        try:
            parsed = json.loads(text)
            values = parsed if isinstance(parsed, list) else text.split(",")
        except json.JSONDecodeError:
            values = re.split(r"[,，、|]", text)
    normalized = {str(value).strip().casefold() for value in values if str(value).strip()}
    if normalized & {"全部", "所有", "all", "*"}:
        return set()
    return normalized


def _rule_aliases(rule: dict[str, Any]) -> dict[str, str]:
    raw = rule.get("字段别名") or rule.get("别名映射")
    if isinstance(raw, dict):
        return {str(k): str(v) for k, v in raw.items()}
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return {str(k): str(v) for k, v in parsed.items()}
    return {}


class DataQualityRuleService(BaseModuleService):
    MODULE = "data_quality_rule"
    ENTITY = "质量规则"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["规则编号", "数据来源", "校验字段", "校验表达式", "阈值下界", "阈值上界", "阻断策略", "规则版本", "记录类型"]
    REQUIRED_FIELDS = ["规则编号", "数据来源", "校验字段"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用规则": "已停用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    def __init__(self) -> None:
        super().__init__()
        self._validation_history: list[dict[str, Any]] = []

    def validate_record(
        self,
        record: dict[str, Any],
        source_module: str = "",
        *,
        record_type: str = "",
        source_input: dict[str, Any] | None = None,
        persist: bool = True,
    ) -> dict[str, Any]:
        """对单条记录执行质量校验。

        校验流程：
        1. 加载与该来源、记录类型相关的已生效规则
        2. 逐条规则执行校验表达式（校验字段缺失时回退到原输入字段）
        3. 检查数值范围（阈值上下界）
        4. 根据阻断策略决定是否拒绝，并把阻断结论写入质量校验台账

        Returns:
            { passed, checks, failed, details, score, blocked, blocked_rules, block_reasons }
        """
        rules = json_store.get_all(self.MODULE)
        matching = [
            r for r in rules
            if r.get("status") == "已生效"
            and self._source_matches(r, source_module)
            and self._type_matches(r, record_type)
        ]

        if not matching:
            result = {
                "passed": True, "checks": 0, "failed": 0, "passed_checks": 0,
                "details": [], "score": 100, "blocked": False,
                "blocked_rules": [], "block_reasons": [], "record_type": record_type,
            }
            if persist:
                result["validation_no"] = self._persist_validation(record, source_module, record_type, result)
            return result

        results = []
        total_passed = 0
        total_failed = 0
        blocked_rules: list[str] = []
        block_reasons: list[str] = []
        field_checks: dict[str, dict[str, Any]] = {}
        blocked = False

        for rule in matching:
            field = str(rule.get("校验字段", ""))
            expression = str(rule.get("校验表达式", ""))
            block_policy = str(rule.get("阻断策略", "警告"))
            try:
                lower = float(rule.get("阈值下界", "0") or "0")
                upper = float(rule.get("阈值上界", "0") or "0")
            except ValueError:
                lower = 0
                upper = 0

            resolved = self._resolve_field(record, source_input or {}, field, _rule_aliases(rule))
            check_result = self._execute_check(resolved["record"], field, expression)
            range_check = self._check_range(resolved["record"], field, lower, upper)
            # 校验字段在原输入里也不存在时，按缺失处理，不能默认通过。
            if resolved["origin"] == "missing":
                check_result = {
                    "passed": False,
                    "message": f"字段 {field} 在转换结果和原输入中均不存在",
                }

            passed = check_result.get("passed", True) and range_check.get("passed", True)
            detail = {
                "rule_no": rule.get("规则编号"),
                "field": field,
                "resolved_field": resolved["name"],
                "value_origin": resolved["origin"],
                "expression": expression,
                "passed": passed,
                "expr_result": check_result,
                "range_result": range_check,
                "block_policy": block_policy,
                "record_types": sorted(_as_record_types(rule.get("记录类型") or rule.get("适用记录类型"))),
            }
            results.append(detail)

            previous = field_checks.get(field)
            field_checks[field] = {
                "field": resolved["name"],
                "value": resolved["record"].get(field),
                "passed": passed if previous is None else (previous["passed"] and passed),
                "origin": resolved["origin"],
                "checks": [*(previous["checks"] if previous else []), detail],
            }

            if passed:
                total_passed += 1
            else:
                total_failed += 1
                if block_policy == "阻断":
                    blocked = True
                    rule_no = str(rule.get("规则编号") or field)
                    if rule_no not in blocked_rules:
                        blocked_rules.append(rule_no)
                    reason = range_check.get("message") if not range_check.get("passed", True) else check_result.get("message")
                    block_reasons.append(f"{rule_no}({field}): {reason}")

        result = {
            "passed": total_failed == 0,
            "checks": len(matching),
            "failed": total_failed,
            "passed_checks": total_passed,
            "details": results,
            "field_checks": field_checks,
            "score": max(0, 100 - total_failed * 10),
            "blocked": blocked,
            "blocked_rules": blocked_rules,
            "block_reasons": block_reasons,
            "record_type": record_type,
        }
        if persist:
            result["validation_no"] = self._persist_validation(record, source_module, record_type, result)
            self._validation_history.append({
                "validation_no": result["validation_no"],
                "record_preview": str(record)[:100],
                "record_type": record_type,
                "checks": len(matching),
                "passed": total_passed,
                "failed": total_failed,
                "blocked": blocked,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })
        return result

    def _source_matches(self, rule: dict[str, Any], source_module: str) -> bool:
        """数据来源匹配；历史规则的空来源/ALL/* 表示全部来源。

        其他取值支持逗号分隔以及“模块.记录类型”写法。
        """
        text = str(rule.get("数据来源", "")).strip()
        if not text or not source_module:
            return True
        if text in ("ALL", "*"):
            return True
        candidates = {part.strip() for part in re.split(r"[,，、|;；\s]+", text) if part.strip()}
        if source_module in candidates:
            return True
        return any(part.split(".")[0] == source_module for part in candidates)

    def _type_matches(self, rule: dict[str, Any], record_type: str) -> bool:
        types = _as_record_types(rule.get("记录类型") or rule.get("适用记录类型"))
        if not types:
            return True
        return str(record_type or "").casefold() in types

    def _resolve_field(
        self,
        record: dict[str, Any],
        source_input: dict[str, Any],
        field: str,
        aliases: dict[str, str],
    ) -> dict[str, Any]:
        """校验字段缺失时回退到原输入字段，兼容历史字段映射。"""
        if field in record:
            return {"record": record, "name": field, "origin": "transformed"}
        for candidate in (aliases.get(field), field):
            if candidate and candidate in source_input:
                return {"record": source_input, "name": candidate, "origin": "source_input"}
        return {"record": record, "name": field, "origin": "missing"}

    def _persist_validation(
        self,
        record: dict[str, Any],
        source_module: str,
        record_type: str,
        result: dict[str, Any],
    ) -> str:
        validation_no = f"VAL-{uuid.uuid4().hex[:12].upper()}"
        with _VALIDATION_LOCK:
            json_store.insert(VALIDATION_KEY, {
                "validation_no": validation_no,
                "source_module": source_module,
                "record_type": record_type,
                "passed": bool(result.get("passed")),
                "blocked": bool(result.get("blocked")),
                "failed": int(result.get("failed", 0) or 0),
                "checks": int(result.get("checks", 0) or 0),
                "score": result.get("score", 100),
                "blocked_rules": result.get("blocked_rules", []),
                "block_reasons": result.get("block_reasons", []),
                "details": result.get("details", []),
                "record_snapshot": record,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })
        return validation_no

    def _execute_check(self, record: dict[str, Any], field: str, expression: str) -> dict[str, Any]:
        """执行校验表达式。

        支持的表达式操作：
        - "not_empty": 非空检查
        - "is_numeric": 是否为数字
        - "pattern:xxx": 正则匹配
        - "in:val1,val2": 值域检查
        - "len_gt:N": 长度大于N
        """
        value = record.get(field)
        field_present = field in record

        if expression == "not_empty":
            if not field_present or value is None or str(value).strip() == "":
                return {"passed": False, "message": f"字段 {field} 为空"}
            return {"passed": True, "message": "通过"}

        if expression == "is_numeric":
            try:
                float(str(value))
                return {"passed": True, "message": "通过"}
            except (ValueError, TypeError):
                return {"passed": False, "message": f"字段 {field} 不是数字"}

        if expression.startswith("pattern:"):
            pattern = expression.split(":", 1)[1]
            if not re.match(pattern, str(value)):
                return {"passed": False, "message": f"字段 {field} 不匹配模式 {pattern}"}
            return {"passed": True, "message": "通过"}

        if expression.startswith("in:"):
            allowed = expression.split(":", 1)[1].split(",")
            if str(value) not in allowed:
                return {"passed": False, "message": f"字段 {field} 不在允许值 {allowed} 中"}
            return {"passed": True, "message": "通过"}

        if expression.startswith("len_gt:"):
            min_len = int(expression.split(":", 1)[1])
            if len(str(value or "")) < min_len:
                return {"passed": False, "message": f"字段 {field} 长度小于 {min_len}"}
            return {"passed": True, "message": "通过"}

        return {"passed": True, "message": "未识别表达式，默认通过"}

    def _check_range(self, record: dict[str, Any], field: str, lower: float, upper: float) -> dict[str, Any]:
        """检查字段值是否在阈值范围内。"""
        if lower == 0 and upper == 0:
            return {"passed": True, "message": "无范围限制"}

        value = record.get(field)
        try:
            num_value = float(str(value))
        except (ValueError, TypeError):
            return {"passed": True, "message": "非数值字段跳过范围检查"}

        if lower > 0 and num_value < lower:
            return {"passed": False, "message": f"值 {num_value} 低于阈值 {lower}"}
        if upper > 0 and num_value > upper:
            return {"passed": False, "message": f"值 {num_value} 超过阈值 {upper}"}

        return {"passed": True, "message": f"值 {num_value} 在范围 [{lower}, {upper}] 内"}

    def validate_batch(
        self,
        records: list[dict[str, Any]],
        source_module: str = "",
        *,
        record_types: list[str] | None = None,
    ) -> dict[str, Any]:
        """批量质量校验；同一批次里的主记录与明细按各自的记录类型校验。"""
        results = []
        passed_count = 0
        failed_count = 0
        blocked_count = 0

        for index, record in enumerate(records):
            record_type = ""
            if record_types and index < len(record_types):
                record_type = str(record_types[index] or "")
            r = self.validate_record(record, source_module, record_type=record_type)
            if r.get("blocked"):
                blocked_count += 1
                failed_count += 1
            elif r["passed"]:
                passed_count += 1
            else:
                failed_count += 1
            results.append(r)

        avg_score = sum(r.get("score", 0) for r in results) / max(len(results), 1)

        return {
            "ok": blocked_count == 0,
            "total": len(records),
            "passed": passed_count,
            "failed": failed_count,
            "blocked": blocked_count,
            "average_score": round(avg_score, 2),
            "results": results,
        }

    def get_quality_report(self) -> dict[str, Any]:
        """获取质量校验报告。

        失败/阻断统计直接来自质量校验台账，与转换放行决策的批次成功数可逐条对账。
        """
        history = self._validation_history[-100:]
        stored = [
            row for row in json_store.get_all(VALIDATION_KEY)
        ]
        rules = json_store.get_all(self.MODULE)
        active_rules = sum(1 for r in rules if r.get("status") == "已生效")

        by_batch: dict[str, dict[str, int]] = {}
        try:
            from app.services import transform_release
            for decision in transform_release.list_decisions():
                quality = decision.get("quality_check") or {}
                if int(quality.get("failed", 0) or 0) <= 0:
                    continue
                batch_id = str(decision.get("batch_id") or "")
                bucket = by_batch.setdefault(batch_id, {"failed": 0, "blocked": 0})
                bucket["failed"] += 1
                if quality.get("blocked"):
                    bucket["blocked"] += 1
        except Exception:
            pass

        return {
            "active_rules": active_rules,
            "total_checks": len(history),
            "stored_validations": len(stored),
            "recent_failures": sum(1 for h in history if h.get("failed", 0) > 0),
            "recent_blocks": sum(1 for h in history if h.get("blocked")),
            "total_failures": sum(1 for row in stored if int(row.get("failed", 0) or 0) > 0),
            "total_blocked": sum(1 for row in stored if row.get("blocked")),
            "failures_by_batch": by_batch,
        }

    def get_validation_detail(self, validation_no: str) -> dict[str, Any] | None:
        """按校验编号回查单条质检详情。"""
        for row in json_store.get_all(VALIDATION_KEY):
            if str(row.get("validation_no")) == str(validation_no):
                return dict(row)
        return None

    def domain_snapshot(self) -> dict[str, Any]:
        """输出接入质量校验的数据质量快照。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "接入质量校验数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "total_validations": len(self._validation_history),
            "active_rules": sum(1 for r in rows if r.get("status") == "已生效"),
            "operation": self.OPERATION_TERM,
        }