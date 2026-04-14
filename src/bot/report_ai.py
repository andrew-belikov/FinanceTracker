from __future__ import annotations

import json
import os
import re
from typing import Any
from urllib import error, request

from common.logging_setup import get_logger
from report_payload import build_monthly_ai_input
from report_render import build_deterministic_monthly_narrative


MONTHLY_AI_OUTPUT_SCHEMA_VERSION = "monthly_ai_output.v1"
OLLAMA_ENABLED = os.getenv("OLLAMA_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434").strip() or "http://ollama:11434"
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:1.5b").strip() or "qwen2.5:1.5b"
OLLAMA_TIMEOUT_SECONDS = float(os.getenv("OLLAMA_TIMEOUT_SECONDS", "60").strip() or "60")
OLLAMA_KEEP_ALIVE = os.getenv("OLLAMA_KEEP_ALIVE", "10m").strip() or "10m"
OLLAMA_NUM_CTX = int(os.getenv("OLLAMA_NUM_CTX", "8192").strip() or "8192")
OLLAMA_MAX_INPUT_CHARS = int(os.getenv("OLLAMA_MAX_INPUT_CHARS", "12000").strip() or "12000")
OLLAMA_TEMPERATURE = 0

_MAX_ITEM_LENGTH = {
    "executive_summary": 220,
    "performance_commentary": 180,
    "instrument_takeaways": 220,
    "cashflow_notes": 180,
    "quality_notes": 180,
    "risk_notes": 180,
    "warnings": 160,
}
_MAX_ITEM_COUNT = {
    "executive_summary": 4,
    "performance_commentary": 5,
    "instrument_takeaways": 6,
    "cashflow_notes": 3,
    "quality_notes": 3,
    "risk_notes": 4,
    "warnings": 4,
}
_MIN_ITEM_COUNT = {
    "executive_summary": 1,
    "performance_commentary": 1,
    "instrument_takeaways": 1,
    "cashflow_notes": 0,
    "quality_notes": 0,
    "risk_notes": 0,
    "warnings": 0,
}
_NARRATIVE_LIST_FIELDS = tuple(_MAX_ITEM_COUNT.keys())
_DIGIT_TOKEN_RE = re.compile(
    r"(?<![\w])(?:\d{2}\.\d{2}(?:\.\d{4})?|[+-]?\d[\d ]*(?:[.,]\d+)?(?: ?[%₽])?)(?![\w])"
)

logger = get_logger(__name__)

MONTHLY_AI_OUTPUT_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "schema_version": {"type": "string", "const": MONTHLY_AI_OUTPUT_SCHEMA_VERSION},
        "report_title": {"type": "string", "minLength": 1, "maxLength": 90},
        "executive_summary": {
            "type": "array",
            "minItems": 1,
            "maxItems": 4,
            "items": {"type": "string", "minLength": 1, "maxLength": 220},
        },
        "performance_commentary": {
            "type": "array",
            "minItems": 1,
            "maxItems": 5,
            "items": {"type": "string", "minLength": 1, "maxLength": 180},
        },
        "instrument_takeaways": {
            "type": "array",
            "minItems": 1,
            "maxItems": 6,
            "items": {"type": "string", "minLength": 1, "maxLength": 220},
        },
        "cashflow_notes": {
            "type": "array",
            "minItems": 0,
            "maxItems": 3,
            "items": {"type": "string", "minLength": 1, "maxLength": 180},
        },
        "quality_notes": {
            "type": "array",
            "minItems": 0,
            "maxItems": 3,
            "items": {"type": "string", "minLength": 1, "maxLength": 180},
        },
        "risk_notes": {
            "type": "array",
            "minItems": 0,
            "maxItems": 4,
            "items": {"type": "string", "minLength": 1, "maxLength": 180},
        },
        "warnings": {
            "type": "array",
            "minItems": 0,
            "maxItems": 4,
            "items": {"type": "string", "minLength": 1, "maxLength": 160},
        },
    },
    "required": [
        "schema_version",
        "report_title",
        "executive_summary",
        "performance_commentary",
        "instrument_takeaways",
        "cashflow_notes",
        "quality_notes",
        "risk_notes",
        "warnings",
    ],
}


class ReportAIError(RuntimeError):
    pass


class ReportAIValidationError(ReportAIError):
    def __init__(self, errors: list[str]):
        super().__init__("; ".join(errors))
        self.errors = errors


def _clean_text(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    text = " ".join(value.replace("\u00a0", " ").split()).strip()
    for prefix in ("- ", "* ", "• "):
        if text.startswith(prefix):
            text = text[len(prefix) :].strip()
    return text


def _collect_text_values(value: Any, *, skip_keys: set[str] | None = None) -> list[str]:
    skip_keys = skip_keys or set()
    values: list[str] = []
    if isinstance(value, str):
        values.append(value)
    elif isinstance(value, list):
        for item in value:
            values.extend(_collect_text_values(item, skip_keys=skip_keys))
    elif isinstance(value, dict):
        for key, item in value.items():
            if key in skip_keys:
                continue
            values.extend(_collect_text_values(item, skip_keys=skip_keys))
    return values


def _normalize_fact_token(token: str) -> str:
    return token.replace("\u00a0", "").replace(" ", "").replace(",", ".").strip()


def _extract_fact_tokens(text: str) -> set[str]:
    return {
        _normalize_fact_token(match.group(0))
        for match in _DIGIT_TOKEN_RE.finditer(text)
        if match.group(0).strip()
    }


def _build_allowed_fact_tokens(ai_input: dict[str, Any]) -> set[str]:
    tokens: set[str] = set()
    for text in _collect_text_values(ai_input, skip_keys={"schema_version"}):
        tokens.update(_extract_fact_tokens(text))
    return tokens


def _extract_json_object(text: str) -> dict[str, Any]:
    candidate = text.strip()
    if not candidate:
        raise ReportAIValidationError(["Модель вернула пустой ответ."])

    try:
        parsed = json.loads(candidate)
    except json.JSONDecodeError:
        parsed = None

    if isinstance(parsed, dict):
        return parsed

    start = candidate.find("{")
    if start < 0:
        raise ReportAIValidationError(["Модель вернула текст без JSON-объекта."])

    depth = 0
    in_string = False
    is_escaped = False
    for index in range(start, len(candidate)):
        char = candidate[index]
        if in_string:
            if is_escaped:
                is_escaped = False
            elif char == "\\":
                is_escaped = True
            elif char == '"':
                in_string = False
            continue

        if char == '"':
            in_string = True
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                snippet = candidate[start : index + 1]
                try:
                    parsed = json.loads(snippet)
                except json.JSONDecodeError as exc:
                    raise ReportAIValidationError(["Не удалось распарсить JSON из ответа модели."]) from exc
                if not isinstance(parsed, dict):
                    raise ReportAIValidationError(["Модель вернула JSON, но не объект верхнего уровня."])
                return parsed

    raise ReportAIValidationError(["Не удалось найти завершённый JSON-объект в ответе модели."])


def _normalize_list_field(
    payload: dict[str, Any],
    field_name: str,
    *,
    required: bool = True,
) -> list[str]:
    raw_value = payload.get(field_name, [])
    if raw_value is None and not required:
        return []
    if not isinstance(raw_value, list):
        raise ReportAIValidationError([f"Поле {field_name} должно быть массивом строк."])

    items = [_clean_text(item) for item in raw_value]
    items = [item for item in items if item]

    min_items = _MIN_ITEM_COUNT[field_name]
    max_items = _MAX_ITEM_COUNT[field_name]
    max_length = _MAX_ITEM_LENGTH[field_name]

    if len(items) < min_items:
        raise ReportAIValidationError([f"Поле {field_name} содержит слишком мало элементов."])
    if len(items) > max_items:
        raise ReportAIValidationError([f"Поле {field_name} содержит слишком много элементов."])
    if any(len(item) > max_length for item in items):
        raise ReportAIValidationError([f"Поле {field_name} содержит слишком длинный bullet."])
    return items


def normalize_monthly_ai_output(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ReportAIValidationError(["Ответ модели должен быть JSON-объектом."])

    errors: list[str] = []
    schema_version = _clean_text(payload.get("schema_version"))
    if schema_version != MONTHLY_AI_OUTPUT_SCHEMA_VERSION:
        errors.append(f"Поле schema_version должно быть {MONTHLY_AI_OUTPUT_SCHEMA_VERSION}.")

    report_title = _clean_text(payload.get("report_title"))
    if not report_title:
        errors.append("Поле report_title обязательно.")
    elif len(report_title) > 90:
        errors.append("Поле report_title слишком длинное.")

    if errors:
        raise ReportAIValidationError(errors)

    normalized = {
        "schema_version": MONTHLY_AI_OUTPUT_SCHEMA_VERSION,
        "report_title": report_title,
    }
    for field_name in _NARRATIVE_LIST_FIELDS:
        normalized[field_name] = _normalize_list_field(payload, field_name, required=field_name != "warnings")
    return normalized


def validate_monthly_ai_output_semantics(
    narrative: dict[str, Any],
    *,
    ai_input: dict[str, Any],
) -> None:
    allowed_tokens = _build_allowed_fact_tokens(ai_input)
    unexpected: set[str] = set()
    output_texts = _collect_text_values(narrative, skip_keys={"schema_version"})
    for text in output_texts:
        for token in _extract_fact_tokens(text):
            if token not in allowed_tokens:
                unexpected.add(token)

    if unexpected:
        joined = ", ".join(sorted(unexpected)[:12])
        raise ReportAIValidationError(
            [f"Модель использовала числа или даты, которых нет во входных фактах: {joined}."]
        )


def build_monthly_ai_system_prompt() -> str:
    return (
        "Ты пишешь narrative-блоки для monthly PDF-отчёта по инвестиционному портфелю.\n"
        "Пиши только на русском языке.\n"
        "Ты не считаешь финансовые метрики и не придумываешь новые числа.\n"
        "Используй только факты из входного JSON.\n"
        "Если факта не хватает, не додумывай его и помести короткую пометку в warnings.\n"
        "Стиль: спокойный, точный, не рекламный, без пафоса, без инвестиционных советов.\n"
        "Верни только JSON, который соответствует заданной схеме."
    )


def build_monthly_ai_user_prompt(ai_input: dict[str, Any]) -> str:
    return (
        "Собери narrative-блоки для monthly PDF-отчёта.\n\n"
        "Правила:\n"
        "1. Не придумывай новые числа, проценты, даты или причины движения.\n"
        "2. Не делай intraday-утверждений: данные только end-of-day.\n"
        "3. Не давай советов и прогнозов.\n"
        "4. Если данных недостаточно, запиши короткое предупреждение в warnings.\n"
        "5. Поля executive_summary, performance_commentary, instrument_takeaways, cashflow_notes, "
        "quality_notes, risk_notes возвращай как массивы коротких bullets.\n"
        "6. Не добавляй markdown.\n\n"
        "Схема ответа:\n"
        f"{json.dumps(MONTHLY_AI_OUTPUT_SCHEMA, ensure_ascii=False, indent=2)}\n\n"
        "Входные факты:\n"
        f"{json.dumps(ai_input, ensure_ascii=False, indent=2)}"
    )


def build_monthly_ai_repair_prompt(ai_input: dict[str, Any], errors: list[str]) -> str:
    issues = "\n".join(f"- {item}" for item in errors)
    return (
        "Предыдущий ответ не подходит.\n\n"
        "Проблемы:\n"
        f"{issues}\n\n"
        "Верни JSON заново.\n"
        "Не меняй факты и не добавляй новые числа.\n"
        "Исправь только структуру и спорные формулировки.\n\n"
        "Схема ответа:\n"
        f"{json.dumps(MONTHLY_AI_OUTPUT_SCHEMA, ensure_ascii=False, indent=2)}\n\n"
        "Входные факты:\n"
        f"{json.dumps(ai_input, ensure_ascii=False, indent=2)}"
    )


def _call_ollama_chat(messages: list[dict[str, str]]) -> dict[str, Any]:
    url = f"{OLLAMA_BASE_URL.rstrip('/')}/api/chat"
    body = json.dumps(
        {
            "model": OLLAMA_MODEL,
            "stream": False,
            "keep_alive": OLLAMA_KEEP_ALIVE,
            "format": MONTHLY_AI_OUTPUT_SCHEMA,
            "messages": messages,
            "options": {
                "temperature": OLLAMA_TEMPERATURE,
                "num_ctx": OLLAMA_NUM_CTX,
            },
        },
        ensure_ascii=False,
    ).encode("utf-8")
    http_request = request.Request(
        url,
        data=body,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        method="POST",
    )

    try:
        with request.urlopen(http_request, timeout=OLLAMA_TIMEOUT_SECONDS) as response:
            response_payload = json.loads(response.read().decode("utf-8"))
    except error.HTTPError as exc:
        message = f"Ollama вернула HTTP {exc.code}."
        try:
            response_payload = json.loads(exc.read().decode("utf-8"))
            message = response_payload.get("error") or response_payload.get("message") or message
        except Exception:
            pass
        raise ReportAIError(message) from exc
    except Exception as exc:
        raise ReportAIError("Не удалось выполнить запрос к Ollama.") from exc

    if not isinstance(response_payload, dict):
        raise ReportAIError("Ollama вернула неожиданный ответ.")
    return response_payload


def _parse_ollama_narrative(response_payload: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    message = response_payload.get("message")
    if not isinstance(message, dict):
        raise ReportAIValidationError(["В ответе Ollama отсутствует message."])

    content = message.get("content")
    if isinstance(content, dict):
        raw_output = content
    elif isinstance(content, str):
        raw_output = _extract_json_object(content)
    else:
        raise ReportAIValidationError(["Ollama вернула message.content в неожиданном формате."])

    normalized = normalize_monthly_ai_output(raw_output)
    telemetry = {
        "done": response_payload.get("done"),
        "total_duration": response_payload.get("total_duration"),
        "load_duration": response_payload.get("load_duration"),
        "prompt_eval_count": response_payload.get("prompt_eval_count"),
        "eval_count": response_payload.get("eval_count"),
    }
    return normalized, telemetry


def _request_validated_monthly_ai_output(
    ai_input: dict[str, Any],
    *,
    attempt: int,
    repair_errors: list[str] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    user_prompt = (
        build_monthly_ai_repair_prompt(ai_input, repair_errors or [])
        if repair_errors
        else build_monthly_ai_user_prompt(ai_input)
    )
    messages = [
        {"role": "system", "content": build_monthly_ai_system_prompt()},
        {"role": "user", "content": user_prompt},
    ]

    logger.info(
        "monthly_report_ai_requested",
        "Requested monthly report narrative from Ollama.",
        {
            "attempt": attempt,
            "base_url": OLLAMA_BASE_URL,
            "model": OLLAMA_MODEL,
            "num_ctx": OLLAMA_NUM_CTX,
            "input_chars": len(json.dumps(ai_input, ensure_ascii=False)),
            "is_repair": bool(repair_errors),
        },
    )

    response_payload = _call_ollama_chat(messages)
    narrative, telemetry = _parse_ollama_narrative(response_payload)
    validate_monthly_ai_output_semantics(narrative, ai_input=ai_input)
    return narrative, telemetry


def build_monthly_report_narrative(payload: dict[str, Any]) -> dict[str, Any]:
    ai_input = build_monthly_ai_input(payload, max_input_chars=OLLAMA_MAX_INPUT_CHARS)
    fallback_narrative = build_deterministic_monthly_narrative(payload)
    fallback_result = {
        "source": "fallback",
        "narrative": fallback_narrative,
        "ai_input": ai_input,
        "attempts": 0,
        "telemetry": {},
        "errors": [],
    }

    if not OLLAMA_ENABLED:
        logger.info(
            "monthly_report_ai_fallback_used",
            "Using deterministic monthly report narrative fallback.",
            {
                "reason": "disabled",
                "model": OLLAMA_MODEL,
            },
        )
        return fallback_result

    validation_errors: list[str] = []
    last_error: str | None = None
    for attempt in (1, 2):
        repair_errors = validation_errors if attempt == 2 else None
        try:
            narrative, telemetry = _request_validated_monthly_ai_output(
                ai_input,
                attempt=attempt,
                repair_errors=repair_errors,
            )
            logger.info(
                "monthly_report_ai_completed",
                "Completed monthly report narrative generation via Ollama.",
                {
                    "attempt": attempt,
                    "model": OLLAMA_MODEL,
                    "total_duration": telemetry.get("total_duration"),
                    "load_duration": telemetry.get("load_duration"),
                    "prompt_eval_count": telemetry.get("prompt_eval_count"),
                    "eval_count": telemetry.get("eval_count"),
                    "warnings_count": len(narrative.get("warnings", [])),
                },
            )
            return {
                "source": "ollama",
                "narrative": narrative,
                "ai_input": ai_input,
                "attempts": attempt,
                "telemetry": telemetry,
                "errors": [],
            }
        except ReportAIValidationError as exc:
            validation_errors = exc.errors
            last_error = str(exc)
            logger.warning(
                "monthly_report_ai_validation_failed",
                "Monthly report narrative from Ollama failed validation.",
                {
                    "attempt": attempt,
                    "model": OLLAMA_MODEL,
                    "errors": validation_errors,
                },
            )
            continue
        except ReportAIError as exc:
            last_error = str(exc)
            break

    logger.warning(
        "monthly_report_ai_fallback_used",
        "Using deterministic monthly report narrative fallback.",
        {
            "reason": "ollama_failed",
            "model": OLLAMA_MODEL,
            "last_error": last_error,
            "validation_errors": validation_errors,
        },
    )
    fallback_result["errors"] = validation_errors or ([last_error] if last_error else [])
    fallback_result["attempts"] = 2 if validation_errors else 1
    return fallback_result
