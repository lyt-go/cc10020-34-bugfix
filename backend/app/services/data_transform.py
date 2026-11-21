"""数据转换编排领域服务：JSON/XML数据转换、字段映射、类型转换与结构重组。

提供从源Schema到目标Schema的字段级映射和值转换能力，
支持嵌套结构抽取、类型强制转换和异常降级策略。
跨模块关联：data_quality_rule（质量校验）、data_lake_sink（转换后落湖）。

转换结果是否可用由“转换放行决策”决定：字段转换成功、但质量校验命中阻断
策略的数据，必须标记为阻断并留在决策台账里，不能算作成功、也不能进入正式数据。
"""
from __future__ import annotations

import json
import re
from typing import Any, Callable

from app.persistence import json_store
from app.services import transform_release
from app.services.base import BaseModuleService


class DataTransformService(BaseModuleService):
    MODULE = "data_transform"
    ENTITY = "转换规则"
    OPERATION_TERM = "转换"
    LIST_FIELDS = ["规则编号", "源Schema", "目标Schema", "字段映射", "转换函数", "默认值填充", "异常策略", "规则版本"]
    REQUIRED_FIELDS = ["规则编号", "源Schema", "目标Schema"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用规则": "已停用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    _BUILTIN_FUNCTIONS: dict[str, Callable[[Any], Any]] = {
        "to_string": lambda x: str(x) if x is not None else "",
        "to_integer": lambda x: int(x) if x is not None else 0,
        "to_float": lambda x: float(x) if x is not None else 0.0,
        "to_boolean": lambda x: bool(x) if x is not None else False,
        "to_upper": lambda x: str(x).upper() if x is not None else "",
        "to_lower": lambda x: str(x).lower() if x is not None else "",
        "extract_numbers": lambda x: re.sub(r"[^0-9]", "", str(x)) if x is not None else "",
        "parse_datetime": lambda x: x,
        "default_empty": lambda x: x if x is not None and str(x).strip() else "",
    }

    _RECORD_TYPE_KEYS = ("记录类型", "record_type", "数据类型", "记录角色", "type")
    _BUSINESS_KEY_FIELDS = (
        "business_key", "业务键", "关联键", "订单号", "order_id",
        "主记录编号", "master_id", "parent_id", "id",
    )

    # ──────────────────────────────────────────────────────────
    # 单条转换
    # ──────────────────────────────────────────────────────────
    def apply_transform(
        self,
        rule_id: int | None,
        source_data: dict[str, Any],
        *,
        record_type: str | None = None,
        business_key: str | None = None,
        batch_id: str | None = None,
        retry_of: Any = None,
        register: bool = True,
    ) -> dict[str, Any]:
        """应用转换规则，并产出“转换放行决策”。

        转换流程：
        1. 加载已生效的转换规则
        2. 解析字段映射定义（补做时优先沿用原决策里的历史映射）
        3. 逐个字段应用转换函数
        4. 处理异常策略（跳过/默认值/报错）
        5. 对转换结果执行质量校验，按阻断策略决定是否放行
        6. 把放行/阻断结论写入决策台账（同一输入只确认一份）

        Returns:
            { ok, data, errors, fields_transformed, total_fields, quality_check,
              released, blocked, release_status, decision, decision_id }
        """
        rule = json_store.get_by_id(self.MODULE, rule_id) if rule_id is not None else None
        identity_rule = rule_id is None
        if rule is None and not identity_rule:
            return self._rejected("转换规则不存在")

        if not identity_rule and rule.get("status") != "已生效":
            return self._rejected("转换规则未生效")

        if identity_rule:
            # 未配置转换规则时按原字段直通，但质量校验和放行决策照常执行。
            rule = {
                "规则编号": "IMPLICIT-IDENTITY",
                "目标Schema": "json",
                "转换函数": "passthrough",
                "异常策略": "跳过",
                "规则版本": "implicit",
            }

        original = transform_release.find_decision(retry_of) if retry_of is not None else None
        if retry_of is not None and original is None:
            return self._rejected(f"原转换决策 {retry_of} 不存在，无法补做")

        effective_type = str(
            record_type
            or (original or {}).get("record_type")
            or self._infer_record_type(source_data)
            or ""
        )
        effective_key = str(
            business_key
            or (original or {}).get("business_key")
            or self._infer_business_key(source_data)
            or ""
        )

        # 补做兼容历史字段映射：原决策保存的映射优先，其次才是当前规则。
        if original and isinstance(original.get("field_mapping"), dict) and original["field_mapping"]:
            field_mapping = dict(original["field_mapping"])
            mapping_source = "retry_original"
        elif identity_rule:
            field_mapping = {str(key): str(key) for key in source_data}
            mapping_source = "identity"
        else:
            field_mapping = self._parse_mapping(str(rule.get("字段映射", "{}")))
            mapping_source = "rule"

        transform_func = str(rule.get("转换函数", "passthrough"))
        error_policy = str(rule.get("异常策略", "跳过"))
        aliases = self._parse_aliases(rule.get("字段别名") or original_decision_aliases(original))

        result: dict[str, Any] = {}
        errors: list[str] = []
        field_errors: dict[str, str] = {}
        transformed = 0
        field_checks: dict[str, dict[str, Any]] = {}

        for target_field, source_path in field_mapping.items():
            try:
                source_value = self._extract_value(source_data, source_path, aliases)
                converted = self._apply_function(transform_func, source_value)
                result[target_field] = converted
                transformed += 1
                field_checks[target_field] = {
                    "source_path": source_path,
                    "source_value": source_value,
                    "converted": converted,
                    "passed": True,
                }
            except (KeyError, ValueError, TypeError) as e:
                field_errors[target_field] = str(e)
                field_checks[target_field] = {
                    "source_path": source_path,
                    "passed": False,
                    "error": str(e),
                }
                if error_policy == "使用默认值":
                    result[target_field] = self._get_default_value(target_field, rule)
                elif error_policy == "报错":
                    errors.append(f"字段 {target_field}: {e}")
                else:
                    errors.append(f"字段 {target_field} 跳过: {e}")

        # 质量校验（跨模块：data_quality_rule）
        quality_check = self._run_quality_checks(
            rule_id, result, source_module=self.MODULE,
            record_type=effective_type, source_input=source_data,
        )

        blocked = bool(quality_check.get("blocked")) or not bool(quality_check.get("passed", True))
        blocked_rules = quality_check.get("blocked_rules") or []
        block_reasons = quality_check.get("block_reasons") or []
        # 只有“阻断策略”命中才真正阻断；普通警告失败仍放行，但会保留在决策里。
        hard_blocked = bool(quality_check.get("blocked"))
        released = not hard_blocked
        if not released:
            errors = list(errors) + [f"质量阻断: {reason}" for reason in block_reasons]

        decision_payload: dict[str, Any] = {
            "rule_id": rule_id,
            "rule_no": rule.get("规则编号"),
            "rule_version": rule.get("规则版本"),
            "batch_id": batch_id,
            "record_type": effective_type,
            "business_key": effective_key,
            "source_input": source_data,
            "transformed_data": result,
            "field_mapping": field_mapping,
            "mapping_source": mapping_source,
            "transform_errors": errors,
            "field_checks": field_checks,
            "quality_check": quality_check,
            "released": released,
            "blocked": not released,
            "release_status": "已放行" if released else "已阻断",
            "block_reasons": block_reasons,
            "blocked_rules": blocked_rules,
            "counted": True,
            "input_fingerprint": transform_release.input_fingerprint(source_data),
            "scope_key": transform_release.decision_scope(rule_id, effective_type, source_data),
            "retry_of": original.get("id") if original else None,
            "original_batch_id": original.get("batch_id") if original else None,
            "original_field_checks": original.get("field_checks") if original else {},
            "original_quality_check": original.get("quality_check") if original else {},
            "original_source_input": original.get("source_input") if original else None,
            "retry_verification": self._verify_retry(original, source_data, quality_check),
        }

        if register:
            decision, duplicate = transform_release.record_decision(decision_payload)
            if duplicate and not decision.get("blocked"):
                # 同一输入已有合格结果时只认已确认的那一份，但本次的阻断结论不能被吞掉。
                released = True
                if hard_blocked:
                    transform_release.add_alternate_block(
                        decision.get("id"),
                        {
                            "decision_id": decision_payload.get("id"),
                            "blocked_rules": blocked_rules,
                            "block_reasons": block_reasons,
                            "quality_check": quality_check,
                            "source_input": source_data,
                        },
                    )
                else:
                    result = decision.get("transformed_data", result)
            elif released and not duplicate:
                # 本次确认了新的合格结果，旧范围内的阻断决策让位但仍可回查。
                transform_release.supersede_blocked(decision_payload["scope_key"], decision.get("id"))
            if original:
                transform_release.link_retry(
                    original.get("id"), decision,
                    superseded=bool(released and original.get("blocked")),
                )
        else:
            decision = decision_payload
            duplicate = False
            decision.setdefault("decision_no", None)
            decision.setdefault("id", None)

        return {
            "ok": bool(released),
            "released": bool(released),
            # 本次校验的清查结论也要如实返回：已有合格结果不等于本次数据没被阻断。
            "blocked": not bool(released) or hard_blocked,
            "release_status": "已放行" if released else "已阻断",
            "data": result,
            "errors": errors,
            "field_errors": field_errors,
            "fields_transformed": transformed,
            "total_fields": len(field_mapping),
            "quality_check": quality_check,
            "field_checks": field_checks,
            "mapping_source": mapping_source,
            "record_type": effective_type,
            "business_key": effective_key,
            "batch_id": batch_id,
            "decision": decision,
            "decision_id": decision.get("id"),
            "decision_no": decision.get("decision_no"),
            "duplicate": duplicate,
            "retry_of": original.get("id") if original else None,
            "retry_verification": decision_payload["retry_verification"],
            "warnings_present": bool(blocked and not hard_blocked),
        }

    def _rejected(self, message: str) -> dict[str, Any]:
        return {
            "ok": False,
            "released": False,
            "blocked": True,
            "release_status": "已阻断",
            "data": {},
            "errors": [message],
            "field_errors": {},
            "fields_transformed": 0,
            "total_fields": 0,
            "quality_check": {"passed": False, "checks": 0, "blocked": True,
                              "block_reasons": [message], "details": []},
            "field_checks": {},
            "mapping_source": "",
            "record_type": "",
            "business_key": "",
            "batch_id": None,
            "decision": None,
            "decision_id": None,
            "decision_no": None,
            "duplicate": False,
            "retry_of": None,
            "retry_verification": {},
            "warnings_present": False,
        }

    def _infer_record_type(self, source_data: dict[str, Any]) -> str:
        for key in self._RECORD_TYPE_KEYS:
            value = source_data.get(key)
            if value is not None and str(value).strip():
                return str(value).strip()
        return ""

    def _infer_business_key(self, source_data: dict[str, Any]) -> str:
        for key in self._BUSINESS_KEY_FIELDS:
            value = source_data.get(key)
            if value is not None and str(value).strip():
                return str(value).strip()
        return ""

    def _verify_retry(
        self,
        original: dict[str, Any] | None,
        source_data: dict[str, Any],
        quality_check: dict[str, Any],
    ) -> dict[str, Any]:
        """补做时同时核对原字段校验结论和原输入，便于追查历史批次。"""
        if original is None:
            return {}
        original_input = original.get("source_input")
        original_checks = original.get("field_checks") or {}
        current_checks = quality_check.get("field_checks") or {}
        changed_fields = []
        if isinstance(original_input, dict) and isinstance(source_data, dict):
            for key in sorted(set(original_input) | set(source_data)):
                if original_input.get(key) != source_data.get(key):
                    changed_fields.append(key)
        else:
            changed_fields = [] if original_input == source_data else ["*"]

        deltas = []
        for field, before in original_checks.items():
            after = current_checks.get(field, {})
            if bool(before.get("passed")) != bool(after.get("passed", True)):
                deltas.append({
                    "field": field,
                    "original_passed": before.get("passed"),
                    "current_passed": after.get("passed", True),
                })

        return {
            "original_decision_id": original.get("id"),
            "original_decision_no": original.get("decision_no"),
            "original_batch_id": original.get("batch_id"),
            "original_rule_version": original.get("rule_version"),
            "original_mapping": original.get("field_mapping") or {},
            "original_quality_check": original.get("quality_check") or {},
            "original_field_checks": original_checks,
            "original_source_input": original_input,
            "input_matched": not changed_fields,
            "changed_fields": changed_fields,
            "field_check_deltas": deltas,
            "original_block_reasons": original.get("block_reasons") or [],
        }

    # ──────────────────────────────────────────────────────────
    # 映射解析
    # ──────────────────────────────────────────────────────────
    def _parse_mapping(self, mapping_str: str) -> dict[str, str]:
        """解析字段映射定义，并兼容历史写法。

        支持格式：
        - JSON object: {"目标字段": "源字段路径"}
        - JSON list: [{"target": "目标", "source": "源"}, ["源", "目标"]]
        - JSON list of pairs: [["源", "目标"]]
        - 简单: "field1->target1,field2->target2"
        """
        try:
            parsed = json.loads(mapping_str)
        except (json.JSONDecodeError, TypeError):
            parsed = None

        if isinstance(parsed, dict):
            if "mapping" in parsed and isinstance(parsed["mapping"], (dict, list)):
                return self._normalize_mapping_items(parsed["mapping"])
            if "字段映射" in parsed and isinstance(parsed["字段映射"], (dict, list)):
                return self._normalize_mapping_items(parsed["字段映射"])
            return {str(k): str(v) for k, v in parsed.items()}
        if isinstance(parsed, list):
            return self._normalize_mapping_items(parsed)

        result = {}
        for part in mapping_str.split(","):
            if "->" in part:
                src, tgt = part.split("->", 1)
                result[tgt.strip()] = src.strip()
        return result if result else {"_raw": "payload"}

    def _normalize_mapping_items(self, items: Any) -> dict[str, str]:
        result: dict[str, str] = {}
        if isinstance(items, dict):
            return {str(k): str(v) for k, v in items.items()}
        for item in items:
            if isinstance(item, dict):
                target = item.get("target") or item.get("目标字段") or item.get("目标") or item.get("to")
                source = item.get("source") or item.get("源字段") or item.get("源") or item.get("from")
                if target is not None and source is not None:
                    result[str(target)] = str(source)
            elif isinstance(item, (list, tuple)) and len(item) >= 2:
                # 历史写法里既有 [源, 目标]，也有 [目标, 源]；带明确键的优先。
                first, second = str(item[0]), str(item[1])
                result[second] = first
        return result or {"_raw": "payload"}

    def _parse_aliases(self, raw: Any) -> dict[str, str]:
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

    def _extract_value(self, data: dict[str, Any], path: str, aliases: dict[str, str] | None = None) -> Any:
        """从嵌套数据中提取指定路径的值。

        支持点号和方括号路径：
        - "name" → data["name"]
        - "user.profile.email" → data["user"]["profile"]["email"]
        - "items[0].id" → data["items"][0]["id"]

        历史字段映射：当新字段名取不到值时，按别名回退到旧字段。
        """
        try:
            return self._extract_path(data, path)
        except KeyError:
            aliases = aliases or {}
            alias = aliases.get(path)
            if alias:
                return self._extract_path(data, alias)
            raise

    def _extract_path(self, data: dict[str, Any], path: str) -> Any:
        parts = re.split(r"\.|(?<=\w)(?=\[)", path)
        current: Any = data
        for part in parts:
            part = part.strip()
            if not part:
                continue
            if "[" in part and "]" in part:
                key, idx_str = part.split("[", 1)
                idx = int(idx_str.rstrip("]"))
                if key:
                    if not isinstance(current, dict) or key not in current:
                        raise KeyError(f"缺少字段 {key}")
                    current = current[key]
                current = current[idx]
            elif isinstance(current, dict):
                if part in current:
                    current = current[part]
                elif path in current:
                    current = current[path]
                else:
                    raise KeyError(f"缺少字段 {part}")
            else:
                raise KeyError(f"无法从 {type(current)} 中提取 {part}")

        return current

    def _apply_function(self, func_name: str, value: Any) -> Any:
        """应用命名转换函数。"""
        if func_name == "passthrough":
            return value
        func = self._BUILTIN_FUNCTIONS.get(func_name)
        if func:
            return func(value)
        return value

    def _get_default_value(self, field: str, rule: dict[str, Any]) -> Any:
        """根据规则配置获取字段默认值。"""
        default_mapping = {
            "string": "", "integer": 0, "float": 0.0,
            "boolean": False, "datetime": "",
        }
        return default_mapping.get(rule.get("目标Schema", ""), "")

    def _run_quality_checks(
        self,
        rule_id: int | None,
        transformed_data: dict[str, Any],
        source_module: str = "",
        record_type: str = "",
        source_input: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """跨模块运行质量校验。"""
        try:
            from app.services.data_quality_rule import DataQualityRuleService
            quality_svc = DataQualityRuleService()
            return quality_svc.validate_record(
                transformed_data,
                source_module=source_module or self.MODULE,
                record_type=record_type,
                source_input=source_input,
            )
        except Exception as e:  # 质量服务异常时不能静默放行
            return {
                "passed": False, "checks": 0, "failed": 1, "blocked": True,
                "block_reasons": [f"质量校验异常: {e}"], "blocked_rules": [],
                "details": [], "score": 0,
            }

    # ──────────────────────────────────────────────────────────
    # 批量转换
    # ──────────────────────────────────────────────────────────
    def batch_transform(
        self,
        rule_id: int | None,
        source_records: list[dict[str, Any]],
        *,
        batch_id: str | None = None,
        record_type: str | None = None,
        retry: bool = False,
    ) -> dict[str, Any]:
        """批量转换多条记录，并用“转换放行决策”核算成功数。

        同一批次里可以有多份关联记录（例如订单主记录 + 订单明细），
        每条记录按自己的记录类型匹配质量规则，互不牵连。
        """
        batch_id = batch_id or transform_release.new_batch_id("TXBATCH")
        retry_targets = self._retry_targets(rule_id) if retry else {}

        prior = transform_release.get_batch_record(batch_id) or {}
        results = []
        duplicate_submissions = 0
        for index, record in enumerate(source_records):
            retry_of = retry_targets.get(self._record_signature(record))
            r = self.apply_transform(
                rule_id,
                record,
                record_type=record_type,
                batch_id=batch_id,
                retry_of=retry_of,
            )
            r["index"] = index
            if r.get("duplicate"):
                duplicate_submissions += 1
            results.append(r)

        transform_release.upsert_batch(batch_id, {
            "rule_id": rule_id,
            "total_submitted": int(prior.get("total_submitted", 0) or 0) + len(source_records),
            "duplicate_submissions": int(prior.get("duplicate_submissions", 0) or 0) + duplicate_submissions,
            "status": "已完成",
            "retry": retry,
        })
        summary = transform_release.batch_summary(batch_id)

        decision_rows = [
            row for row in (
                transform_release.find_decision(did) for did in summary.get("decision_ids", [])
            ) if row
        ]
        decision_rows.sort(key=lambda row: (str(row.get("record_type") or ""), int(row.get("id", 0))))

        return {
            "ok": summary["blocked"] == 0,
            "message": (
                f"批量转换完成: 放行 {summary['released']}/{summary['total']}"
                f"（提交 {len(source_records)} 条，重复提交 {duplicate_submissions} 条，"
                f"阻断 {summary['blocked']} 条）"
            ),
            "batch_id": batch_id,
            "批次编号": batch_id,
            "success": summary["released"],
            "failed": summary["blocked"],
            "total": summary["total"],
            "total_submitted": len(source_records),
            "duplicates": duplicate_submissions,
            "blocked": summary["blocked"],
            "released": summary["released"],
            "summary": summary,
            "decisions": decision_rows,
            "results": results,
        }

    def _retry_targets(self, rule_id: int) -> dict[str, Any]:
        """查找可补做的原决策：优先取本规则最近一次阻断决策。"""
        rows = transform_release.list_decisions(blocked=True, rule_id=rule_id)
        targets: dict[str, Any] = {}
        for row in sorted(rows, key=lambda item: int(item.get("id", 0)), reverse=True):
            signature = transform_release.canonical_json(row.get("source_input"))
            targets.setdefault(signature, row.get("id"))
        return targets

    def _record_signature(self, record: dict[str, Any]) -> str:
        return transform_release.canonical_json(record)

    def retry_transform(
        self,
        rule_id: int | None,
        source_data: dict[str, Any],
        *,
        original_decision_id: Any = None,
        batch_id: str | None = None,
        record_type: str | None = None,
    ) -> dict[str, Any]:
        """补做（重试）转换：沿用原决策的历史字段映射，并核对原字段结论与原输入。"""
        if original_decision_id is None:
            fingerprint = transform_release.input_fingerprint(source_data)
            candidates = [
                row for row in transform_release.list_decisions(rule_id=rule_id)
                if row.get("input_fingerprint") == fingerprint
            ]
            if not candidates:
                # 补做时原输入可能已被修正：按业务键找回最近一条被阻断的决策。
                business_key = self._infer_business_key(source_data)
                if business_key:
                    candidates = [
                        row for row in transform_release.list_decisions(
                            rule_id=rule_id, blocked=True, business_key=business_key)
                    ]
            candidates.sort(key=lambda row: int(row.get("id", 0)), reverse=True)
            if candidates:
                original_decision_id = candidates[0].get("id")
        return self.apply_transform(
            rule_id,
            source_data,
            record_type=record_type,
            batch_id=batch_id or transform_release.new_batch_id("TXRETRY"),
            retry_of=original_decision_id,
        )

    # ──────────────────────────────────────────────────────────
    # 查询 / 对账
    # ──────────────────────────────────────────────────────────
    def get_batch_release(self, batch_id: str) -> dict[str, Any]:
        """取一个批次已放行的转换结果，并给出关联记录与阻断原因。"""
        summary = transform_release.batch_summary(batch_id)
        decisions = transform_release.list_decisions(batch_id=batch_id)
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in decisions:
            key = str(row.get("business_key") or f"decision:{row.get('id')}")
            grouped.setdefault(key, []).append(row)

        linked = []
        released_records = []
        for key, rows in grouped.items():
            released = [row for row in rows if row.get("released")]
            blocked = [row for row in rows if not row.get("released")]
            released_records.extend(released)
            linked.append({
                "business_key": key,
                "released": [
                    {
                        "decision_id": row.get("id"),
                        "decision_no": row.get("decision_no"),
                        "record_type": row.get("record_type"),
                        "data": row.get("transformed_data"),
                        "usable_alone": True,
                    }
                    for row in released
                ],
                "blocked": [
                    {
                        "decision_id": row.get("id"),
                        "decision_no": row.get("decision_no"),
                        "record_type": row.get("record_type"),
                        "block_reasons": row.get("block_reasons"),
                        "blocked_rules": row.get("blocked_rules"),
                        "quality_check": row.get("quality_check"),
                    }
                    for row in blocked
                ],
                "main_record_blocked": any(is_main_record(row) for row in blocked),
                "main_block_reasons": next(
                    (row.get("block_reasons") for row in blocked if is_main_record(row)),
                    [],
                ),
            })

        return {
            "ok": True,
            "batch_id": batch_id,
            "summary": summary,
            "released": [
                {
                    "decision_id": row.get("id"),
                    "decision_no": row.get("decision_no"),
                    "record_type": row.get("record_type"),
                    "business_key": row.get("business_key"),
                    "data": row.get("transformed_data"),
                    "formal_written": row.get("formal_written"),
                }
                for row in released_records
            ],
            "blocked": [
                {
                    "decision_id": row.get("id"),
                    "decision_no": row.get("decision_no"),
                    "record_type": row.get("record_type"),
                    "business_key": row.get("business_key"),
                    "block_reasons": row.get("block_reasons"),
                    "blocked_rules": row.get("blocked_rules"),
                    "quality_check": row.get("quality_check"),
                    "data": row.get("transformed_data"),
                    "superseded_by": row.get("superseded_by"),
                }
                for row in decisions if not row.get("released")
            ],
            "linked_records": linked,
            "formal_count": summary["formal_written"],
        }

    def get_decision(self, decision_id: Any) -> dict[str, Any] | None:
        decision = transform_release.find_decision(decision_id)
        if decision is None:
            return None
        retries = [
            row for row in transform_release.list_decisions()
            if str(row.get("retry_of")) == str(decision.get("id"))
        ]
        retries.sort(key=lambda row: int(row.get("id", 0)))
        decision["retries"] = retries
        return decision

    def reconcile_batch(self, batch_id: str | None = None) -> dict[str, Any]:
        """对照质量报告与批次成功明细，回查被错误算入成功的源行。"""
        batches = [batch_id] if batch_id else [
            str(row.get("batch_id")) for row in transform_release.list_batches()
        ]
        findings = []
        for bid in batches:
            if not bid or bid == "None":
                continue
            decisions = transform_release.list_decisions(batch_id=bid)
            summary = transform_release.batch_summary(bid)
            reported_failures = [
                row for row in decisions
                if int((row.get("quality_check") or {}).get("failed", 0) or 0) > 0
            ]
            # 质量报告里的失败若同时被算进批次成功，就是错误算入成功的源行。
            # 已补做放行的原阻断行属于“可解释差异”，普通警告失败单独列出。
            mis_counted = [
                row for row in reported_failures
                if row.get("released")
                and row.get("counted", True) is not False
                and bool((row.get("quality_check") or {}).get("blocked"))
            ]
            explainable = [
                row for row in reported_failures
                if (row.get("released") and not bool((row.get("quality_check") or {}).get("blocked")))
                or row.get("superseded_by")
            ]
            findings.append({
                "batch_id": bid,
                "quality_report_failed": len(reported_failures),
                "batch_success": summary["released"],
                "batch_failed": summary["blocked"],
                "matched": not mis_counted,
                "explainable_success_with_warnings": len(explainable),
                "mis_counted_source_rows": [
                    {
                        "decision_id": row.get("id"),
                        "decision_no": row.get("decision_no"),
                        "record_type": row.get("record_type"),
                        "business_key": row.get("business_key"),
                        "source_input": row.get("source_input"),
                        "quality_check": row.get("quality_check"),
                    }
                    for row in mis_counted
                ],
            })
        mismatched = [item for item in findings if not item["matched"]]
        return {
            "ok": not mismatched,
            "batch_id": batch_id,
            "total_batches": len(findings),
            "mismatched": len(mismatched),
            "batches": findings,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        """输出数据转换编排的数据质量快照。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        decisions = transform_release.list_decisions()
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "数据转换编排数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "active_rules": sum(1 for r in rows if r.get("status") == "已生效"),
            "release_decisions": len(decisions),
            "released_decisions": sum(1 for row in decisions if row.get("released")),
            "blocked_decisions": sum(1 for row in decisions if not row.get("released")),
            "operation": self.OPERATION_TERM,
        }


def original_decision_aliases(original: dict[str, Any] | None) -> Any:
    if not original:
        return None
    return original.get("field_aliases") or original.get("字段别名")


def is_main_record(decision: dict[str, Any]) -> bool:
    """判断一条决策是否为主记录（订单主记录等）。"""
    role = str(decision.get("record_type") or "").casefold()
    return any(word in role for word in ("主记录", "主表", "订单主", "header", "master", "main"))
