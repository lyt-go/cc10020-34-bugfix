"""跨模块数据流管道API：暴露完整业务链路端点。

将领域服务的能力串联为可测试的数据流管道，每条管道对应
一个跨模块数据流链路。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/api/pipeline", tags=["跨模块数据流"])


class WebhookPayload(BaseModel):
    event_type: str = "default_event"
    data: dict[str, Any] = {}
    headers: dict[str, str] | None = None
    signature: str | None = None


class BatchExecuteRequest(BaseModel):
    task_id: int
    max_pages: int = 5
    transform_rule_id: int | None = None
    batch_id: str | None = None


class DlqReplayRequest(BaseModel):
    dlq_ids: list[int]


class BackfillExecuteRequest(BaseModel):
    job_id: int


class EndpointTestRequest(BaseModel):
    endpoint_id: int


class TransformRequest(BaseModel):
    rule_id: int
    source_data: list[dict[str, Any]]
    batch_id: str | None = None
    record_type: str | None = None
    retry: bool = False


class TransformRetryRequest(BaseModel):
    rule_id: int
    source_data: dict[str, Any]
    original_decision_id: int | str | None = None
    batch_id: str | None = None
    record_type: str | None = None


class BatchReworkRequest(BaseModel):
    batch_id: str
    rule_id: int | None = None


class ValidationRequest(BaseModel):
    records: list[dict[str, Any]]
    source_module: str = ""
    record_types: list[str] | None = None


@router.post("/webhook-to-lake")
def pipeline_webhook_to_lake(channel_id: int, payload: WebhookPayload) -> dict[str, Any]:
    """链路1: Webhook接收 → 事件解析 → 去重 → 分发路由 → 数据落湖。

    完整覆盖：webhook_channel → event_schema → dedup_filter → dispatch_rule → data_lake_sink。
    """
    from app.services.webhook_channel import WebhookChannelService
    svc = WebhookChannelService()
    result = svc.receive_webhook(
        channel_id=channel_id,
        payload={"type": payload.event_type, **payload.data},
        signature=payload.signature,
        headers=payload.headers,
    )
    return result


@router.post("/batch-ingest-to-lake")
def pipeline_batch_ingest_to_lake(request: BatchExecuteRequest) -> dict[str, Any]:
    """链路2: 批量拉取 → 数据转换 → 质量校验 → 数据落湖。

    完整覆盖：batch_ingest → data_transform → data_quality_rule → data_lake_sink。
    只有转换放行决策为“已放行”的记录才会落湖，成功数按决策核算。
    """
    from app.services.batch_ingest import BatchIngestService
    svc = BatchIngestService()
    result = svc.execute_batch(
        task_id=request.task_id,
        max_pages=request.max_pages,
        transform_rule_id=request.transform_rule_id,
        batch_id=request.batch_id,
    )
    return result


@router.post("/dlq-replay")
def pipeline_dlq_replay(request: DlqReplayRequest) -> dict[str, Any]:
    """链路3: 死信队列 → 分析 → 重放 → 追踪。

    完整覆盖：dlq_handler → mq_consumer → call_trace_log。
    """
    from app.services.dlq_handler import DlqHandlerService
    svc = DlqHandlerService()

    if len(request.dlq_ids) == 1 and request.dlq_ids[0] == -1:
        # 全部待处置记录重放
        analysis = svc.analyze_failure_reasons()
        if analysis["total"] == 0:
            return {"ok": False, "message": "无死信记录", "replayed": 0}
        from app.persistence import json_store
        all_dlq = json_store.get_all("dlq_handler")
        pending_ids = [int(r["id"]) for r in all_dlq if r.get("status") in ("草稿", "待处置")]
        return svc.batch_replay(pending_ids)

    return svc.batch_replay(request.dlq_ids)


@router.post("/backfill-execute")
def pipeline_backfill_execute(request: BackfillExecuteRequest) -> dict[str, Any]:
    """链路4: 历史回补 → 水位查询 → 批量拉取 → 数据落湖。

    完整覆盖：backfill_job → incre_cursor → batch_ingest → data_lake_sink。
    """
    from app.services.backfill_job import BackfillJobService
    svc = BackfillJobService()
    result = svc.execute_backfill(job_id=request.job_id)
    return result


@router.post("/endpoint-test")
def pipeline_endpoint_test(request: EndpointTestRequest) -> dict[str, Any]:
    """链路5: 端点连接测试 → 认证验证 → 调用追踪。

    完整覆盖：api_endpoint → api_auth_store → call_trace_log。
    """
    from app.services.api_endpoint import ApiEndpointService
    svc = ApiEndpointService()
    result = svc.test_connection(endpoint_id=request.endpoint_id)
    return result


@router.post("/transform-validate")
def pipeline_transform_validate(request: TransformRequest) -> dict[str, Any]:
    """链路6: 数据转换 → 质量校验 → 转换放行决策 → 结果追踪。

    完整覆盖：data_transform → data_quality_rule → data_lake_sink。
    含阻断字段的转换数据不会出现在成功结果里，而是留在决策台账中。
    """
    from app.services.data_transform import DataTransformService
    transform_svc = DataTransformService()
    batch_result = transform_svc.batch_transform(
        rule_id=request.rule_id,
        source_records=request.source_data,
        batch_id=request.batch_id,
        record_type=request.record_type,
        retry=request.retry,
    )
    return batch_result


@router.post("/transform-retry")
def pipeline_transform_retry(request: TransformRetryRequest) -> dict[str, Any]:
    """链路6a: 转换补做（重试）→ 核对原字段结论与原输入 → 放行决策。"""
    from app.services.data_transform import DataTransformService
    return DataTransformService().retry_transform(
        rule_id=request.rule_id,
        source_data=request.source_data,
        original_decision_id=request.original_decision_id,
        batch_id=request.batch_id,
        record_type=request.record_type,
    )


@router.get("/transform-batches/{batch_id}")
def pipeline_transform_batch(batch_id: str) -> dict[str, Any]:
    """链路6b: 按批次取已放行的转换结果，以及主记录/明细的阻断原因。"""
    from app.services.data_transform import DataTransformService
    return DataTransformService().get_batch_release(batch_id)


@router.get("/transform-decisions/{decision_id}")
def pipeline_transform_decision(decision_id: str) -> dict[str, Any]:
    """链路6c: 回查单条转换放行决策及其补做记录。"""
    from app.services.data_transform import DataTransformService
    decision = DataTransformService().get_decision(decision_id)
    if decision is None:
        raise HTTPException(status_code=404, detail=f"转换放行决策 {decision_id} 不存在")
    return decision


@router.get("/transform-reconcile")
def pipeline_transform_reconcile(batch_id: str | None = None) -> dict[str, Any]:
    """链路6d: 质量报告失败数与批次成功明细对账，回查错误算入成功的源行。"""
    from app.services.data_transform import DataTransformService
    return DataTransformService().reconcile_batch(batch_id)


@router.post("/batch-rework")
def pipeline_batch_rework(request: BatchReworkRequest) -> dict[str, Any]:
    """链路6e: 对批次里被阻断的记录补做，原批次校验结论和来源保持可追溯。"""
    from app.services.batch_ingest import BatchIngestService
    return BatchIngestService().rework_batch(request.batch_id, rule_id=request.rule_id)


@router.post("/batch-validate")
def pipeline_validate_batch(request: ValidationRequest) -> dict[str, Any]:
    """链路7: 质量批量校验 → 统计报告。

    完整覆盖：data_quality_rule → integration_monitor。
    """
    from app.services.data_quality_rule import DataQualityRuleService
    svc = DataQualityRuleService()
    result = svc.validate_batch(
        records=request.records,
        source_module=request.source_module,
        record_types=request.record_types,
    )
    return result


@router.get("/health-summary")
def pipeline_health_summary() -> dict[str, Any]:
    """链路8: 全局监控指标聚合 → 健康度评分。

    完整覆盖：integration_monitor → call_trace_log → dlq_handler → data_lake_sink。
    """
    from app.services.integration_monitor import IntegrationMonitorService
    svc = IntegrationMonitorService()
    return svc.collect_global_metrics()


@router.get("/dedup-stats")
def pipeline_dedup_stats() -> dict[str, Any]:
    """链路9: 去重统计查询。"""
    from app.services.dedup_filter import DedupFilterService
    svc = DedupFilterService()
    return svc.get_dedup_stats()


@router.post("/archive-execute")
def pipeline_archive_execute(policy_id: int) -> dict[str, Any]:
    """链路10: 归档策略执行 → 压缩 → 过期清理。"""
    from app.services.event_archive import EventArchiveService
    svc = EventArchiveService()
    return svc.execute_archive(policy_id=policy_id)