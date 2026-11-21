"""管道入口的路由与行为测试：转换入口、批次查询、对账接口不能被通用资源路由吞掉。"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from starlette.routing import Match

from app.main import app as fastapi_app
from app.persistence import JsonStore
import app.persistence

import app.services.transform_release as release_mod
import app.services.data_transform as transform_mod
import app.services.data_quality_rule as quality_mod
import app.services.batch_ingest as batch_mod
import app.services.data_lake_sink as sink_mod

from app.services import transform_release
from app.services.data_transform import DataTransformService
from app.services.batch_ingest import BatchIngestService


def resolve_path(method: str, path: str) -> str | None:
    """按 Starlette 的实际匹配顺序找出第一个命中的路由模板。"""
    scope = {
        "type": "http", "method": method, "path": path, "headers": [],
        "query_string": b"", "root_path": "", "scheme": "http",
        "server": ("test", 80), "client": ("test", 1),
    }

    def walk(routes):
        for route in routes:
            if hasattr(route, "effective_candidates"):
                found = walk(route.effective_candidates())
                if found:
                    return found
                continue
            match, _ = route.matches(scope)
            if match == Match.FULL:
                return getattr(route, "path", None)
        return None

    return walk(fastapi_app.routes)


class TestPipelineRouting(unittest.TestCase):
    def test_pipeline_static_paths_are_not_shadowed(self):
        cases = {
            ("GET", "/api/pipeline/health-summary"): "/api/pipeline/health-summary",
            ("GET", "/api/pipeline/dedup-stats"): "/api/pipeline/dedup-stats",
            ("GET", "/api/pipeline/transform-reconcile"): "/api/pipeline/transform-reconcile",
            ("GET", "/api/pipeline/transform-batches/B1"): "/api/pipeline/transform-batches/{batch_id}",
            ("POST", "/api/pipeline/transform-retry"): "/api/pipeline/transform-retry",
        }
        for (method, path), expected in cases.items():
            with self.subTest(path=path):
                self.assertEqual(resolve_path(method, path), expected)

    def test_module_detail_route_still_works(self):
        self.assertEqual(
            resolve_path("GET", "/api/data_transform/1"), "/api/{module}/{entry_id}")


class TestPipelineTransformEntry(unittest.TestCase):
    """直接调用管道函数，验证入口返回的成功结果里没有阻断行。"""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self._original = app.persistence.json_store
        store = JsonStore(base_dir=self._tmp.name)
        app.persistence.json_store = store
        for module in (release_mod, transform_mod, quality_mod, batch_mod, sink_mod):
            module.json_store = store
        self.store = store

        self.rule = store.insert("data_transform", {
            "规则编号": "TR-PIPE-001",
            "源Schema": "json",
            "目标Schema": "json",
            "字段映射": json.dumps({"order_id": "order_id", "amount": "amount"}),
            "转换函数": "passthrough",
            "异常策略": "跳过",
            "规则版本": "v1",
            "status": "已生效",
        })
        store.insert("data_quality_rule", {
            "规则编号": "Q-PIPE-AMOUNT",
            "数据来源": "data_transform",
            # 只约束订单主记录；订单明细按自己的字段规则校验。
            "记录类型": "订单主记录",
            "校验字段": "amount",
            "校验表达式": "is_numeric",
            "阈值下界": "0",
            "阈值上界": "10000",
            "阻断策略": "阻断",
            "规则版本": "v1",
            "status": "已生效",
        })

    def tearDown(self):
        app.persistence.json_store = self._original
        for module in (release_mod, transform_mod, quality_mod, batch_mod, sink_mod):
            module.json_store = self._original
        self._tmp.cleanup()

    def test_transform_validate_endpoint_returns_only_released_rows(self):
        from app.routers.pipeline import TransformRequest, pipeline_transform_validate

        result = pipeline_transform_validate(TransformRequest(
            rule_id=self.rule["id"],
            record_type="订单主记录",
            source_data=[
                {"order_id": "ORD-1", "amount": 30000},
                {"order_id": "ORD-2", "amount": 500},
            ],
        ))

        self.assertEqual(result["total"], 2)
        self.assertEqual(result["success"], 1)
        self.assertEqual(result["failed"], 1)
        released = [row for row in result["decisions"] if row["released"]]
        self.assertEqual([row["business_key"] for row in released], ["ORD-2"])
        self.assertTrue(all(row["transformed_data"]["amount"] <= 10000 for row in released))

    def test_batch_release_view_explains_master_block(self):
        from app.routers.pipeline import (
            BatchExecuteRequest,
            pipeline_batch_ingest_to_lake,
            pipeline_transform_batch,
        )

        task = self.store.insert("batch_ingest", {
            "任务编号": "BI-PIPE-001",
            "关联端点": "3",
            "Cron表达式": "* * * * *",
            "分页策略": "逐页",
            "并行批次": "1",
            "断点续传": "否",
            "失败动作": "记录并继续",
            "任务版本": "v1",
            "status": "运行中",
            "转换规则": str(self.rule["id"]),
        })
        from app.services.batch_ingest import BatchIngestService
        from app.services.data_lake_sink import DataLakeSinkService
        DataLakeSinkService()._lake_base = os.path.join(self._tmp.name, "lake")

        result = BatchIngestService().execute_batch(task["id"], source_records=[
            {"id": 1, "order_id": "ORD-P", "amount": 30000, "记录类型": "订单主记录"},
            {"id": 2, "order_id": "ORD-P", "amount": 30000, "记录类型": "订单明细"},
        ])
        view = pipeline_transform_batch(result["batch_id"])
        linked = view["linked_records"][0]
        self.assertTrue(linked["main_record_blocked"])
        self.assertEqual([row["record_type"] for row in linked["released"]], ["订单明细"])
        self.assertIn("Q-PIPE-AMOUNT", view["blocked"][0]["blocked_rules"])

    def test_reconcile_endpoint_flags_miscounted_source_row(self):
        from app.routers.pipeline import pipeline_transform_reconcile

        transform_release.record_decision({
            "rule_id": self.rule["id"],
            "batch_id": "HIST-2",
            "record_type": "订单主记录",
            "business_key": "ORD-BAD",
            "source_input": {"order_id": "ORD-BAD", "amount": 88888},
            "transformed_data": {"order_id": "ORD-BAD", "amount": 88888},
            "field_mapping": {"amount": "amount"},
            "released": True,
            "blocked": False,
            "counted": True,
            "quality_check": {"passed": False, "blocked": True, "failed": 1},
        })
        result = pipeline_transform_reconcile("HIST-2")
        self.assertFalse(result["ok"])
        self.assertEqual(
            result["batches"][0]["mis_counted_source_rows"][0]["business_key"], "ORD-BAD")


if __name__ == "__main__":
    unittest.main(verbosity=2)
