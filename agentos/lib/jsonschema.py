"""Минимальный валидатор JSON Schema (подмножество draft-07).

Поддерживает: type, properties, required, items, enum, minimum, maximum,
minLength, maxLength, additionalProperties. Полный валидатор подключается
адаптером-плагином; этого подмножества достаточно для манифестов и
схем инструментов.
"""

from __future__ import annotations

from typing import Any

_TYPES = {
    "object": dict,
    "array": list,
    "string": str,
    "integer": int,
    "number": (int, float),
    "boolean": bool,
    "null": type(None),
}


def validate(schema: dict[str, Any], data: Any, path: str = "$") -> list[str]:
    """Возвращает список ошибок; пустой список — данные валидны."""
    errors: list[str] = []
    expected = schema.get("type")
    if expected:
        py_type = _TYPES.get(expected)
        if py_type is None:
            errors.append(f"{path}: неизвестный type '{expected}'")
            return errors
        if expected == "integer" and isinstance(data, bool):
            errors.append(f"{path}: ожидался integer, получен boolean")
            return errors
        if not isinstance(data, py_type):
            errors.append(f"{path}: ожидался {expected}, получен {type(data).__name__}")
            return errors

    if "enum" in schema and data not in schema["enum"]:
        errors.append(f"{path}: значение вне enum {schema['enum']}")

    if isinstance(data, (int, float)) and not isinstance(data, bool):
        if "minimum" in schema and data < schema["minimum"]:
            errors.append(f"{path}: {data} < minimum {schema['minimum']}")
        if "maximum" in schema and data > schema["maximum"]:
            errors.append(f"{path}: {data} > maximum {schema['maximum']}")

    if isinstance(data, str):
        if "minLength" in schema and len(data) < schema["minLength"]:
            errors.append(f"{path}: короче minLength {schema['minLength']}")
        if "maxLength" in schema and len(data) > schema["maxLength"]:
            errors.append(f"{path}: длиннее maxLength {schema['maxLength']}")

    if isinstance(data, dict):
        for req in schema.get("required", []):
            if req not in data:
                errors.append(f"{path}: отсутствует обязательное поле '{req}'")
        props = schema.get("properties", {})
        for key, value in data.items():
            if key in props:
                errors.extend(validate(props[key], value, f"{path}.{key}"))
            elif schema.get("additionalProperties") is False:
                errors.append(f"{path}: недопустимое поле '{key}'")

    if isinstance(data, list) and "items" in schema:
        for i, item in enumerate(data):
            errors.extend(validate(schema["items"], item, f"{path}[{i}]"))

    return errors
