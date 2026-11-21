"""转换放行决策台账：记录每条转换输入的放行/阻断结论、校验快照与来源。

数据转换入口在完成字段转换和质量校验后，必须留下一条“转换放行决策”：
- 放行：转换结果可以进入正式数据（数据湖）；
- 阻断：质量规则命中阻断策略，转换结果只能留在台账用于排查，不能进入正式数据。

台账同时承担幂等职责：同一规则、同一记录类型、同一份原输入只会确认一份
合格结果，并发提交也只会写入一条决策。补做（重试）会引用原决策、原批次、
原字段校验结论和原输入，历史字段映射也会随决策一起保存。
"""
from __future__ import annotations

import hashlib
import json
import threading
import uuid
from datetime import datetime, timezone
from typing import Any

from app.persistence import json_store

DECISION_KEY = "data_transform_release"
BATCH_KEY = "data_transform_release_batch"

# 同一输入并发提交时，必须把“查重 + 写入决策”当成一个原子动作。
_DECISION_LOCK = threading.RLock()

_RELEASED_WORDS = {"已放行", "放行", "通过", "released", "pass", "passed", "ok", "success", "true", "是", "1"}
_BLOCKED_WORDS = {"已阻断", "阻断", "拒绝", "blocked", "reject", "rejected", "fail", "failed", "false", "否", "0"}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, default=str)


def input_fingerprint(source_input: Any) -> str:
    return hashlib.sha256(canonical_json(source_input).encode("utf-8")).hexdigest()


def decision_scope(rule_id: Any, record_type: str | None, source_input: Any) -> str:
    """同一份输入的唯一范围键。

    只由规则、记录类型和原输入决定，不包含字段映射：字段映射可能随规则版本
    演进，但同一份原始输入不能被重复确认成两份合格结果。
    """
    payload = {
        "rule": "" if rule_id is None else str(rule_id),
        "record_type": record_type or "",
        "input": input_fingerprint(source_input),
    }
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def new_batch_id(prefix: str = "BATCH") -> str:
    return f"{prefix}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"


def _first(data: dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in data and data[key] is not None:
            return data[key]
    return default


def _as_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip()
    if not text:
        return None
    lowered = text.casefold()
    if lowered in _RELEASED_WORDS:
        return True
    if lowered in _BLOCKED_WORDS:
        return False
    return None


def _quality_of(record: dict[str, Any]) -> dict[str, Any]:
    quality = _first(record, "quality_check", "质量校验", "quality", default={})
    return quality if isinstance(quality, dict) else {}


def normalize_decision(record: dict[str, Any]) -> dict[str, Any]:
    """兼容历史决策字段映射，统一成当前台账结构。"""
    decision = dict(record)
    quality = _quality_of(record)

    status = _first(record, "release_status", "放行状态", "状态")
    released = _as_bool(_first(record, "released", "放行", "是否放行", "ok", "success"))
    blocked = _as_bool(_first(record, "blocked", "阻断", "是否阻断", "is_blocked"))

    if blocked is None and isinstance(status, str):
        if any(word in status for word in ("阻断", "拒绝", "blocked", "reject")):
            blocked = True
        elif any(word in status for word in ("放行", "通过", "released", "pass")):
            blocked = False
    if blocked is None and quality:
        blocked = bool(quality.get("blocked"))
    if released is None and quality:
        released = bool(quality.get("passed")) and not blocked
    if released is None and blocked is not None:
        released = not blocked
    if blocked is None:
        blocked = not bool(released)
    if released is None:
        released = not blocked

    source_input = _first(record, "source_input", "原输入", "original_input", "input_data", "input")
    transformed = _first(record, "transformed_data", "转换结果", "data", "output")
    mapping = _first(record, "field_mapping", "字段映射", "mapping", default={})
    batch_id = _first(record, "batch_id", "批次编号", "批次")
    rule_id = _first(record, "rule_id", "规则ID", "转换规则")
    decision_no = _first(record, "decision_no", "决策编号", "release_no")
    record_type = _first(record, "record_type", "记录类型")
    business_key = _first(record, "business_key", "关联键", "业务键")
    fingerprint = _first(record, "input_fingerprint", "输入指纹")
    if not fingerprint and source_input is not None:
        fingerprint = input_fingerprint(source_input)
    scope_key = _first(record, "scope_key")
    if not scope_key and source_input is not None:
        scope_key = decision_scope(rule_id, record_type, source_input)

    decision.update({
        "released": bool(released),
        "blocked": bool(blocked) or not bool(released),
        "release_status": "已放行" if released and not blocked else "已阻断",
        "source_input": source_input,
        "transformed_data": transformed if isinstance(transformed, dict) else {},
        "field_mapping": mapping if isinstance(mapping, dict) else {},
        "quality_check": quality,
        "batch_id": batch_id,
        "rule_id": rule_id,
        "decision_no": decision_no,
        "record_type": record_type,
        "business_key": business_key,
        "input_fingerprint": fingerprint,
        "scope_key": scope_key,
    })
    decision.setdefault("block_reasons", _first(record, "阻断原因", "block_reason", "reasons", default=[]))
    decision.setdefault("blocked_rules", _first(record, "阻断规则", "blocked_by", default=[]))
    decision.setdefault("mapping_source", _first(record, "映射来源", default=""))
    decision.setdefault("formal_written", _as_bool(_first(record, "已落湖", "formal", "sunk")) or False)
    # counted 表示该决策参与本批核算；放行与阻断都要计入，成功数只取 released。
    decision.setdefault("counted", _first(record, "计入核算", "counted", default=True) is not False)
    decision.setdefault("retry_of", _first(record, "original_decision_id", "retry_of", "原决策", "补做自"))
    decision.setdefault("original_batch_id", _first(record, "原批次", "original_batch"))
    decision.setdefault("original_field_checks", _first(record, "原字段校验结论", "original_checks", default={}))
    decision.setdefault("original_source_input", _first(record, "原输入快照", "original_source_input"))
    decision.setdefault("retry_count", int(_first(record, "补做次数", "retry_count", default=0) or 0))
    decision.setdefault("duplicate_hits", int(_first(record, "duplicate_hits", default=0) or 0))
    decision.setdefault("alternate_blocks", list(_first(record, "备选阻断", "alternate_blocks", default=[]) or []))
    return decision


def list_decisions(
    *,
    batch_id: str | None = None,
    released: bool | None = None,
    blocked: bool | None = None,
    rule_id: Any = None,
    record_type: str | None = None,
    business_key: str | None = None,
    include_retry: bool = True,
) -> list[dict[str, Any]]:
    rows = [normalize_decision(row) for row in json_store.get_all(DECISION_KEY)]
    if batch_id is not None:
        rows = [row for row in rows if str(row.get("batch_id") or "") == str(batch_id)]
    if released is not None:
        rows = [row for row in rows if bool(row.get("released")) is bool(released)]
    if blocked is not None:
        rows = [row for row in rows if bool(row.get("blocked")) is bool(blocked)]
    if rule_id is not None:
        rows = [row for row in rows if str(row.get("rule_id")) == str(rule_id)]
    if record_type is not None:
        rows = [row for row in rows if str(row.get("record_type") or "") == str(record_type)]
    if business_key is not None:
        rows = [row for row in rows if str(row.get("business_key") or "") == str(business_key)]
    if not include_retry:
        rows = [row for row in rows if not row.get("retry_of")]
    return rows


def find_decision(decision_id: Any) -> dict[str, Any] | None:
    if decision_id is None:
        return None
    text = str(decision_id).strip()
    for row in json_store.get_all(DECISION_KEY):
        decision = normalize_decision(row)
        if str(row.get("id")) == text or str(decision.get("decision_no") or "") == text:
            return decision
    return None


def find_canonical(scope_key: str) -> dict[str, Any] | None:
    """同一输入范围里已确认的决策：已放行的优先级最高，其次是未被替代的阻断决策。"""
    if not scope_key:
        return None
    matches = []
    for row in json_store.get_all(DECISION_KEY):
        decision = normalize_decision(row)
        if str(decision.get("scope_key") or "") != scope_key:
            continue
        if decision.get("superseded_by"):
            continue
        matches.append(decision)
    if not matches:
        return None
    matches.sort(key=lambda row: (
        0 if row.get("released") else
        1 if not row.get("retry_of") else 2,
        int(row.get("id", 0)),
    ))
    return matches[0]


def supersede_blocked(scope_key: str, new_decision_id: Any) -> None:
    """新的合格结果确认后，同范围的旧阻断决策保留可查但不再参与批次核算。"""
    with _DECISION_LOCK:
        for row in json_store.get_all(DECISION_KEY):
            decision = normalize_decision(row)
            if str(decision.get("scope_key") or "") != scope_key:
                continue
            if decision.get("released") or str(decision.get("id")) == str(new_decision_id):
                continue
            json_store.update(DECISION_KEY, int(decision["id"]), {
                "superseded_by": new_decision_id,
                "superseded_at": utc_now(),
                "updated_at": utc_now(),
            })


def record_decision(decision: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    """原子地确认一条放行决策。

    补做决策（retry_of 非空）不复用原决策，但同一原决策的同类补做仍然只确认一份。

    Returns:
        (decision, duplicate)：duplicate=True 表示该输入已经确认过，本次不再新增。
    """
    with _DECISION_LOCK:
        retry_of = decision.get("retry_of")
        if retry_of in (None, ""):
            existing = find_canonical(str(decision.get("scope_key") or ""))
            # 之前只有阻断结论、这次算出合格结果时，要确认新的放行决策，
            # 不能把阻断决策当成“已确认的合格结果”。
            if existing is not None and existing.get("blocked") and decision.get("released"):
                existing = None
        else:
            existing = next((
                normalize_decision(row)
                for row in json_store.get_all(DECISION_KEY)
                if str(row.get("retry_of")) == str(retry_of)
                and str(row.get("input_fingerprint") or "") == str(decision.get("input_fingerprint") or "")
                and bool(row.get("released")) == bool(decision.get("released"))
            ), None)
        if existing is not None:
            hits = int(existing.get("duplicate_hits", 0) or 0) + 1
            json_store.update(DECISION_KEY, int(existing["id"]), {
                "duplicate_hits": hits,
                "last_duplicate_at": utc_now(),
            })
            existing["duplicate_hits"] = hits
            return existing, True

        payload = dict(decision)
        payload.setdefault("decision_no", f"REL-{uuid.uuid4().hex[:12].upper()}")
        payload.setdefault("created_at", utc_now())
        payload["updated_at"] = payload["created_at"]
        inserted = json_store.insert(DECISION_KEY, payload)
        return normalize_decision(inserted), False


def update_decision(decision_id: Any, updates: dict[str, Any]) -> dict[str, Any] | None:
    decision = find_decision(decision_id)
    if decision is None:
        return None
    with _DECISION_LOCK:
        updated = json_store.update(DECISION_KEY, int(decision["id"]), {**updates, "updated_at": utc_now()})
    return normalize_decision(updated) if updated else None


def add_alternate_block(decision_id: Any, block: dict[str, Any]) -> None:
    """同一输入已有合格结果时，把本次的阻断结论挂到已确认决策上以便追查。"""
    decision = find_decision(decision_id)
    if decision is None:
        return
    alternates = list(decision.get("alternate_blocks") or [])
    alternates.append({"at": utc_now(), **block})
    update_decision(decision["id"], {
        "alternate_blocks": alternates,
        "blocked_recheck": True,
        "blocked_recheck_at": utc_now(),
    })


def claim_formal_write(decision_id: Any) -> bool:
    """抢占正式数据写入权，保证并发时同一决策只落湖一次。"""
    decision = find_decision(decision_id)
    if decision is None or decision.get("formal_written"):
        return False
    with _DECISION_LOCK:
        current = find_decision(decision_id)
        if current is None or current.get("formal_written"):
            return False
        json_store.update(DECISION_KEY, int(current["id"]), {
            "formal_written": True,
            "formal_status": "写入中",
            "formal_claimed_at": utc_now(),
            "updated_at": utc_now(),
        })
        return True


def mark_formal_written(decision_id: Any, reference: dict[str, Any] | None = None) -> None:
    reference = reference or {}
    update_decision(decision_id, {
        "formal_written": True,
        "formal_status": "已落湖",
        "formal_ref": reference,
        "sunk": True,
        "已落湖": True,
    })


def release_formal_claim(decision_id: Any) -> None:
    update_decision(decision_id, {
        "formal_written": False,
        "formal_status": "写入失败",
        "formal_ref": {},
    })


def upsert_batch(batch_id: str, values: dict[str, Any]) -> dict[str, Any]:
    with _DECISION_LOCK:
        rows = json_store.get_all(BATCH_KEY)
        for row in rows:
            if str(row.get("batch_id")) == str(batch_id):
                updated = json_store.update(BATCH_KEY, int(row["id"]), {
                    **values,
                    "batch_id": batch_id,
                    "updated_at": utc_now(),
                })
                return dict(updated or row)
        payload = {
            "batch_id": batch_id,
            "created_at": utc_now(),
            "updated_at": utc_now(),
            **values,
        }
        return dict(json_store.insert(BATCH_KEY, payload))


def get_batch_record(batch_id: str) -> dict[str, Any] | None:
    for row in json_store.get_all(BATCH_KEY):
        if str(row.get("batch_id")) == str(batch_id):
            return dict(row)
    return None


def list_batches() -> list[dict[str, Any]]:
    return [dict(row) for row in json_store.get_all(BATCH_KEY)]


def batch_summary(batch_id: str) -> dict[str, Any]:
    decisions = list_decisions(batch_id=batch_id)
    # 批次核算按历史事实统计：补做替代只影响“哪份是当前有效结论”，
    # 不抹掉原批次里那条阻断记录，原字段校验结论和来源始终可追查。
    counted = [row for row in decisions if row.get("counted", True) is not False]
    released = [row for row in counted if row.get("released")]
    blocked = [row for row in counted if not row.get("released")]
    quality_blocked = [
        row for row in decisions
        if bool(_quality_of(row).get("blocked")) or bool(row.get("blocked"))
    ]
    quality_failed = [
        row for row in decisions
        if int(_quality_of(row).get("failed", 0) or 0) > 0
    ]
    recheck_blocked = [
        row for row in decisions
        if row.get("blocked_recheck") or row.get("alternate_blocks")
    ]
    warnings = [row for row in quality_failed if not row.get("blocked")]

    stored = get_batch_record(batch_id) or {}
    summary = {
        "batch_id": batch_id,
        "批次编号": batch_id,
        "rule_id": stored.get("rule_id"),
        "total": len(counted),
        "total_submitted": int(stored.get("total_submitted", len(decisions)) or 0),
        "success": len(released),
        "released": len(released),
        "released_count": len(released),
        "failed": len(blocked),
        "blocked": len(blocked),
        "blocked_count": len(blocked),
        "duplicates": int(stored.get("duplicate_submissions", 0) or 0),
        "quality_checks": len(decisions),
        "quality_failed": len(quality_blocked),
        "quality_blocked": len(quality_blocked),
        "quality_warning_failures": len(warnings),
        "recheck_blocked": len(recheck_blocked),
        "released_decision_ids": [row.get("id") for row in released],
        "blocked_decision_ids": [row.get("id") for row in blocked],
        "decision_ids": [row.get("id") for row in decisions],
        # 成功结果只取放行决策；阻断行永远不在这里。
        "release_decision_ids": [row.get("id") for row in decisions],
        "success_decision_ids": [row.get("id") for row in released],
        "formal_written": sum(1 for row in released if row.get("formal_written")),
        "records_fetched": int(stored.get("records_fetched", 0) or 0),
        "records_sunk": int(stored.get("records_sunk", 0) or 0),
        "created_at": stored.get("created_at") or (decisions[0].get("created_at") if decisions else None),
        "retry_of_batch_id": stored.get("retry_of_batch_id"),
        "record_count": len(decisions),
    }
    return summary


def link_retry(original_id: Any, retry: dict[str, Any], *, superseded: bool = False) -> None:
    original = find_decision(original_id)
    if original is None:
        return
    updates = {
        "retry_count": int(original.get("retry_count", 0) or 0) + 1,
        "last_retry_at": utc_now(),
        "last_retry_decision_id": retry.get("id"),
        "last_retry_status": "已放行" if retry.get("released") else "已阻断",
    }
    if superseded:
        updates["superseded_by"] = retry.get("id")
    update_decision(original["id"], updates)
