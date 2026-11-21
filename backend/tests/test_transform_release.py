"""转换放行决策链路测试：阻断字段、补做、幂等、批次核算与对账。

覆盖用户反馈的核心问题：
1. 批量转换入口的成功结果里不能残留超限金额（阻断行不进成功结果/正式数据）；
2. 单条质检详情应显示已阻断，且阻断结论回写到转换清单；
3. 补做同时核对原字段校验结论和原输入，兼容历史字段映射；
4. 并发提交同一输入只确认一份合格结果；
5. 批次成功数按转换放行决策核算，质量报告失败可回查到源行；
6. 同一批次的主记录与明细互不牵连，主记录阻断时明细仍可单独使用。
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.persistence import JsonStore
import app.persistence

import app.services.transform_release as release_mod
import app.services.data_transform as transform_mod
import app.services.data_quality_rule as quality_mod
import app.services.batch_ingest as batch_mod
import app.services.data_lake_sink as sink_mod

from app.services import transform_release
from app.services.batch_ingest import BatchIngestService
from app.services.data_lake_sink import DataLakeSinkService
from app.services.data_quality_rule import DataQualityRuleService
from app.services.data_transform import DataTransformService


class ReleaseCase(unittest.TestCase):
    """公共夹具：独立存储 + 一条转换规则 + 主记录/明细两条质量规则。"""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self._original_store = app.persistence.json_store
        self.store = JsonStore(base_dir=self._tmp.name)
        app.persistence.json_store = self.store
        for module in (release_mod, transform_mod, quality_mod, batch_mod, sink_mod):
            module.json_store = self.store

        self.transform_svc = DataTransformService()
        self.quality_svc = DataQualityRuleService()
        self.batch_svc = BatchIngestService()
        self.sink_svc = DataLakeSinkService()
        self.sink_svc._lake_base = os.path.join(self._tmp.name, "lake")

        self.rule = self.store.insert("data_transform", {
            "规则编号": "TR-RELEASE-001",
            "源Schema": "json",
            "目标Schema": "json",
            "字段映射": json.dumps({
                "order_id": "order_id",
                "amount": "amount",
                "quantity": "quantity",
            }),
            "转换函数": "passthrough",
            "异常策略": "跳过",
            "规则版本": "v1",
            "status": "已生效",
        })
        self.store.insert("data_quality_rule", {
            "规则编号": "Q-MAIN-AMOUNT",
            "数据来源": "data_transform",
            "记录类型": "订单主记录",
            "校验字段": "amount",
            "校验表达式": "is_numeric",
            "阈值下界": "0",
            "阈值上界": "10000",
            "阻断策略": "阻断",
            "规则版本": "v1",
            "status": "已生效",
        })
        self.store.insert("data_quality_rule", {
            "规则编号": "Q-DETAIL-QTY",
            "数据来源": "data_transform",
            "记录类型": "订单明细",
            "校验字段": "quantity",
            "校验表达式": "is_numeric",
            "阈值下界": "0",
            "阈值上界": "100",
            "阻断策略": "阻断",
            "规则版本": "v1",
            "status": "已生效",
        })
        self.task = self.store.insert("batch_ingest", {
            "任务编号": "BI-RELEASE-001",
            "关联端点": "7",
            "Cron表达式": "* * * * *",
            "分页策略": "逐页",
            "并行批次": "1",
            "断点续传": "否",
            "失败动作": "记录并继续",
            "任务版本": "v1",
            "status": "运行中",
            "转换规则": str(self.rule["id"]),
        })

    def tearDown(self):
        app.persistence.json_store = self._original_store
        for module in (release_mod, transform_mod, quality_mod, batch_mod, sink_mod):
            module.json_store = self._original_store
        self._tmp.cleanup()

    def over_limit_order(self):
        return [
            {"id": 1, "order_id": "ORD-1001", "amount": 25000, "quantity": 2, "记录类型": "订单主记录"},
            {"id": 2, "order_id": "ORD-1001", "amount": 25000, "quantity": 2, "记录类型": "订单明细"},
        ]


class TestBlockedRowNeverSucceeds(ReleaseCase):
    def test_over_limit_master_is_not_in_success_results(self):
        result = self.batch_svc.execute_batch(self.task["id"], source_records=self.over_limit_order())

        self.assertEqual(result["records_fetched"], 2)
        self.assertEqual(result["success"], 1)
        self.assertEqual(result["blocked"], 1)
        self.assertEqual(result["records_sunk"], 1)

        summary = result["release_summary"]
        self.assertEqual(len(summary["release_decision_ids"]), 2)
        self.assertEqual(len(summary["released_decision_ids"]), 1)
        self.assertEqual(len(summary["blocked_decision_ids"]), 1)

        released_ids = set(transform_release.batch_summary(summary["batch_id"])["released_decision_ids"])
        for decision_id in summary["release_decision_ids"]:
            decision = transform_release.find_decision(decision_id)
            if decision["record_type"] == "订单主记录":
                # 主记录超限：只留在阻断明细里，不能出现在成功结果里
                self.assertNotIn(decision_id, released_ids)
                self.assertGreater(decision["transformed_data"]["amount"], 10000)
            else:
                self.assertIn(decision_id, released_ids)

    def test_single_record_quality_detail_is_blocked(self):
        master = self.over_limit_order()[0]
        applied = self.transform_svc.apply_transform(self.rule["id"], master)

        self.assertFalse(applied["ok"])
        self.assertTrue(applied["blocked"])
        self.assertEqual(applied["release_status"], "已阻断")
        self.assertGreater(applied["quality_check"]["failed"], 0)
        self.assertTrue(applied["quality_check"]["blocked"])

        detail = self.quality_svc.get_validation_detail(applied["quality_check"]["validation_no"])
        self.assertIsNotNone(detail)
        self.assertTrue(detail["blocked"])
        self.assertEqual(detail["record_type"], "订单主记录")

    def test_blocking_result_is_written_back_to_release_ledger(self):
        master = self.over_limit_order()[0]
        applied = self.transform_svc.apply_transform(self.rule["id"], master)
        decision = transform_release.find_decision(applied["decision_id"])

        self.assertTrue(decision["blocked"])
        self.assertFalse(decision["released"])
        self.assertTrue(decision["block_reasons"])
        self.assertIn("Q-MAIN-AMOUNT", decision["blocked_rules"])
        self.assertEqual(decision["quality_check"]["blocked"], True)


class TestBatchAccountingAndReconciliation(ReleaseCase):
    def test_success_count_comes_from_release_decisions(self):
        result = self.batch_svc.execute_batch(self.task["id"], source_records=self.over_limit_order())
        summary = result["release_summary"]

        self.assertEqual(summary["released"], 1)
        self.assertEqual(summary["blocked"], 1)
        self.assertEqual(summary["total"], 2)
        # 质量报告失败数与批次阻断数一致
        report = self.quality_svc.get_quality_report()
        batch_failures = report["failures_by_batch"][result["batch_id"]]
        self.assertEqual(batch_failures["failed"], 1)
        self.assertEqual(batch_failures["blocked"], 1)

    def test_reconcile_finds_rows_wrongly_counted_as_success(self):
        # 人为伪造一条“质量失败但被算成成功”的历史决策
        transform_release.record_decision({
            "rule_id": self.rule["id"],
            "batch_id": "HIST-BATCH-1",
            "record_type": "订单主记录",
            "business_key": "ORD-OLD",
            "source_input": {"order_id": "ORD-OLD", "amount": 99999},
            "transformed_data": {"order_id": "ORD-OLD", "amount": 99999},
            "field_mapping": {"amount": "amount"},
            "released": True,
            "blocked": False,
            "release_status": "已放行",
            "counted": True,
            "quality_check": {"passed": False, "blocked": True, "failed": 1},
        })

        result = self.transform_svc.reconcile_batch("HIST-BATCH-1")
        self.assertFalse(result["ok"])
        self.assertEqual(result["mismatched"], 1)
        rows = result["batches"][0]["mis_counted_source_rows"]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["business_key"], "ORD-OLD")
        self.assertEqual(rows[0]["source_input"]["amount"], 99999)


class TestRetryVerification(ReleaseCase):
    def test_retry_checks_original_checks_and_input(self):
        master = self.over_limit_order()[0]
        original = self.transform_svc.apply_transform(self.rule["id"], master)
        self.assertTrue(original["blocked"])

        fixed = {**master, "amount": 8000}
        retry = self.transform_svc.retry_transform(self.rule["id"], fixed)
        self.assertTrue(retry["released"])
        verification = retry["retry_verification"]
        self.assertEqual(verification["original_decision_id"], original["decision_id"])
        self.assertFalse(verification["input_matched"])
        self.assertEqual(verification["changed_fields"], ["amount"])
        self.assertTrue(verification["original_field_checks"])
        self.assertEqual(verification["original_source_input"]["amount"], 25000)
        self.assertTrue(verification["original_block_reasons"])
    def test_retry_uses_historical_field_mapping(self):
        legacy = transform_release.record_decision({
            "rule_id": self.rule["id"],
            "batch_id": "LEGACY-BATCH",
            "record_type": "订单明细",
            "business_key": "ORD-LEGACY",
            "source_input": {"order_id": "ORD-LEGACY", "amount": 100, "quantity": 1},
            "transformed_data": {"order_id": "ORD-LEGACY", "amount": 100, "quantity": 1},
            "field_mapping": {"order_id": "order_id", "amount": "amount", "quantity": "quantity"},
            "released": False,
            "blocked": True,
            "quality_check": {"passed": False, "blocked": True, "failed": 1},
        })[0]

        # 当前规则的映射里没有 quantity，补做时应沿用原决策的历史映射
        self.store.update("data_transform", self.rule["id"], {
            "字段映射": json.dumps({"order_id": "order_id", "amount": "amount"}),
        })
        retry = self.transform_svc.apply_transform(
            self.rule["id"],
            {"order_id": "ORD-LEGACY", "amount": 100, "quantity": 100},
            retry_of=legacy["id"],
        )
        self.assertEqual(retry["mapping_source"], "retry_original")
        self.assertIn("quantity", retry["decision"]["field_mapping"])
        self.assertTrue(retry["released"])

    def test_historical_mapping_shapes_are_accepted(self):
        parser = self.transform_svc._parse_mapping
        self.assertEqual(parser('{"a": "b"}'), {"a": "b"})
        self.assertEqual(parser('[{"target": "t", "source": "s"}]'), {"t": "s"})
        self.assertEqual(parser('[["src", "tgt"]]'), {"tgt": "src"})
        self.assertEqual(parser('src->tgt'), {"tgt": "src"})


class TestConcurrentSubmission(ReleaseCase):
    def test_same_input_confirms_only_one_result(self):
        record = {"id": 9, "order_id": "ORD-CONC", "amount": 100, "quantity": 1, "记录类型": "订单明细"}
        outputs = []

        def submit():
            outputs.append(self.transform_svc.apply_transform(
                self.rule["id"], record, batch_id="CONC-BATCH"))

        threads = [threading.Thread(target=submit) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        decision_ids = {item["decision_id"] for item in outputs}
        self.assertEqual(len(decision_ids), 1)
        self.assertEqual(sum(1 for item in outputs if item["released"]), 8)
        self.assertEqual(sum(1 for item in outputs if item["duplicate"]), 7)
        decisions = transform_release.list_decisions(batch_id="CONC-BATCH")
        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0]["duplicate_hits"], 7)

    def test_concurrent_formal_write_happens_once(self):
        record = {"id": 10, "order_id": "ORD-SINK", "amount": 100, "quantity": 1, "记录类型": "订单明细"}
        applied = self.transform_svc.apply_transform(self.rule["id"], record, batch_id="SINK-BATCH")
        claims = []

        def claim():
            claims.append(transform_release.claim_formal_write(applied["decision_id"]))

        threads = [threading.Thread(target=claim) for _ in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(sum(1 for value in claims if value), 1)

    def test_duplicate_submission_does_not_hide_new_block(self):
        record = {"id": 11, "order_id": "ORD-DUP", "amount": 100, "quantity": 1, "记录类型": "订单明细"}
        first = self.transform_svc.apply_transform(self.rule["id"], record)
        self.assertTrue(first["released"])

        # 规则收紧后用同一输入再提交：不能因为已有合格结果就把阻断结论吞掉。
        self.store.update("data_quality_rule", 2, {"校验字段": "quantity", "校验表达式": "len_gt:500"})
        second = self.transform_svc.apply_transform(self.rule["id"], record)
        self.assertTrue(second["duplicate"])
        self.assertTrue(second["released"])
        self.assertTrue(second["blocked"])

        decision = transform_release.find_decision(first["decision_id"])
        self.assertTrue(decision["blocked_recheck"])
        self.assertEqual(len(decision["alternate_blocks"]), 1)
        self.assertIn("Q-DETAIL-QTY", decision["alternate_blocks"][0]["blocked_rules"])


class TestLinkedRecords(ReleaseCase):
    def test_detail_usable_alone_while_master_blocked(self):
        result = self.batch_svc.execute_batch(self.task["id"], source_records=self.over_limit_order())
        batch_id = result["batch_id"]

        release_view = self.batch_svc.get_batch_release(batch_id)
        self.assertEqual(len(release_view["linked_records"]), 1)
        linked = release_view["linked_records"][0]
        self.assertEqual(linked["business_key"], "ORD-1001")
        self.assertTrue(linked["main_record_blocked"])
        self.assertTrue(linked["main_block_reasons"])
        self.assertEqual([item["record_type"] for item in linked["released"]], ["订单明细"])
        self.assertTrue(linked["released"][0]["usable_alone"])
        self.assertEqual(linked["released"][0]["data"]["quantity"], 2)

        # 主记录的实际阻断原因可查
        self.assertIn("Q-MAIN-AMOUNT", release_view["blocked"][0]["blocked_rules"])

    def test_formal_lake_never_contains_over_limit_master(self):
        self.batch_svc.execute_batch(self.task["id"], source_records=self.over_limit_order())
        lake = self.sink_svc.query_lake_data("batch_ingest")
        record_types = [entry["payload"].get("记录类型") for entry in lake["records"]]
        self.assertNotIn("订单主记录", record_types)
        self.assertIn("订单明细", record_types)
        for entry in lake["records"]:
            self.assertGreaterEqual(entry["payload"].get("amount", 0), 0)
            self.assertLessEqual(entry["payload"].get("amount", 0), 25000)
        # 放行记录里保留决策追溯信息
        release_meta = lake["records"][0]["payload"]["_release"]
        self.assertTrue(release_meta["decision_id"])

    def test_rework_keeps_original_batch_traceable(self):
        result = self.batch_svc.execute_batch(self.task["id"], source_records=self.over_limit_order())
        original_batch = result["batch_id"]
        original_blocked = transform_release.list_decisions(batch_id=original_batch, blocked=True)[0]

        # 原输入修正后再补做
        fixed_input = {**original_blocked["source_input"], "amount": 9000}
        self.store.update("data_quality_rule", 1, {"阈值上界": "0"})  # 放开主记录规则以模拟数据修正
        retried = self.transform_svc.apply_transform(
            self.rule["id"], fixed_input, retry_of=original_blocked["id"])

        self.assertTrue(retried["released"])
        original_after = transform_release.find_decision(original_blocked["id"])
        self.assertEqual(original_after["retry_count"], 1)
        self.assertEqual(original_after["last_retry_decision_id"], retried["decision_id"])
        # 原批次仍然保留阻断记录和字段校验结论
        original_view = self.transform_svc.get_batch_release(original_batch)
        self.assertEqual(original_view["summary"]["blocked"], 1)
        self.assertTrue(original_blocked["field_checks"])
        self.assertEqual(original_view["blocked"][0]["block_reasons"], original_blocked["block_reasons"])

    def test_batch_rework_endpoint_releases_blocked_rows(self):
        result = self.batch_svc.execute_batch(self.task["id"], source_records=self.over_limit_order())
        batch_id = result["batch_id"]

        # 修正规则后补做整批阻断记录
        self.store.update("data_quality_rule", 1, {"阈值上界": "0"})
        rework = self.batch_svc.rework_batch(batch_id)

        self.assertEqual(rework["reworked"], 1)
        self.assertEqual(rework["released"], 1)
        self.assertEqual(rework["still_blocked"], 0)
        self.assertEqual(rework["retry_of_batch_id"], batch_id)

        lake = self.sink_svc.query_lake_data("batch_ingest")
        record_types = [entry["payload"].get("记录类型") for entry in lake["records"]]
        self.assertIn("订单主记录", record_types)
        summary = rework["summary"]
        self.assertEqual(summary["released"], 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
