"""
Spark 配置过滤与脱敏工具。
"""

from typing import Dict, Iterable, Tuple, Any, Optional


DEFAULT_FILTER = {
    "allow_prefixes": ["spark."],
    "allow_keys": [],
    "deny_prefixes": [],
    "deny_keys": [],
    "sensitive_keywords": ["password", "secret", "token", "credential", "key"],
    "collect_system_properties": False,
    "collect_java_properties": False,
    "redact_sensitive_values": True,
    "max_configs_per_app": 200,
    "max_value_length": 1024,
    "audit_only": False,
}


def _as_list(value) -> list:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return list(value)
    return [value]


def normalize_filter(filter_cfg: Dict[str, Any] = None) -> Dict[str, Any]:
    """合并默认值并规范化过滤配置。"""
    cfg = {**DEFAULT_FILTER, **(filter_cfg or {})}
    cfg["allow_prefixes"] = [str(p) for p in _as_list(cfg.get("allow_prefixes"))]
    cfg["allow_keys"] = [str(k) for k in _as_list(cfg.get("allow_keys"))]
    cfg["deny_prefixes"] = [str(p) for p in _as_list(cfg.get("deny_prefixes"))]
    cfg["deny_keys"] = [str(k) for k in _as_list(cfg.get("deny_keys"))]
    cfg["sensitive_keywords"] = [str(k).lower() for k in _as_list(cfg.get("sensitive_keywords"))]
    # 基本类型兜底
    cfg["collect_system_properties"] = bool(cfg.get("collect_system_properties", False))
    cfg["collect_java_properties"] = bool(cfg.get("collect_java_properties", False))
    cfg["redact_sensitive_values"] = bool(cfg.get("redact_sensitive_values", True))
    cfg["audit_only"] = bool(cfg.get("audit_only", False))
    cfg["max_configs_per_app"] = int(cfg.get("max_configs_per_app", DEFAULT_FILTER["max_configs_per_app"]))
    cfg["max_value_length"] = int(cfg.get("max_value_length", DEFAULT_FILTER["max_value_length"]))
    return cfg


def _is_denied(key: str, cfg: Dict[str, Any]) -> bool:
    return key in cfg["deny_keys"] or any(key.startswith(p) for p in cfg["deny_prefixes"])


def _is_allowed(key: str, cfg: Dict[str, Any]) -> bool:
    # 若配置了允许列表，则必须命中；否则默认允许
    if cfg["allow_keys"] or cfg["allow_prefixes"]:
        return key in cfg["allow_keys"] or any(key.startswith(p) for p in cfg["allow_prefixes"])
    return True


def _needs_redact(key: str, cfg: Dict[str, Any]) -> bool:
    k = key.lower()
    return any(token in k for token in cfg["sensitive_keywords"])


def sanitize_kv(key: str, value: Any, category: str, cfg: Dict[str, Any]) -> Optional[Tuple[str, str]]:
    """
    按配置过滤/脱敏单个键值。
    返回 (key, sanitized_value)；如需丢弃返回 None。
    """
    if category == "system" and not cfg["collect_system_properties"]:
        return None
    if category == "java" and not cfg["collect_java_properties"]:
        return None

    if _is_denied(key, cfg):
        return None
    if not _is_allowed(key, cfg):
        return None

    sanitized_value = "" if cfg["audit_only"] else str(value)

    if cfg["redact_sensitive_values"] and _needs_redact(key, cfg):
        sanitized_value = "***redacted***"

    if not cfg["audit_only"] and sanitized_value and len(sanitized_value) > cfg["max_value_length"]:
        sanitized_value = sanitized_value[: cfg["max_value_length"]] + "...(truncated)"

    return key, sanitized_value


def sanitize_items(
    items: Iterable[Tuple[str, Any]],
    category: str,
    cfg: Dict[str, Any],
    current_count: int,
) -> Tuple[Dict[str, str], int]:
    """批量过滤键值对，返回过滤后的字典及最新累计数量。"""
    cleaned = {}
    for key, value in items:
        if current_count >= cfg["max_configs_per_app"]:
            break
        result = sanitize_kv(key, value, category, cfg)
        if result is None:
            continue
        cleaned_key, cleaned_value = result
        cleaned[cleaned_key] = cleaned_value
        current_count += 1
    return cleaned, current_count


