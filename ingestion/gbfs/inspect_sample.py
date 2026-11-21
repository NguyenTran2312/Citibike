import json
from collections import Counter, defaultdict
from pathlib import Path


def json_type(value):
    """Tên kiểu dữ liệu theo JSON."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


def inspect_nested(value, path, result):
    """Ghi nhận kiểu dữ liệu ở cả các trường lồng nhau."""
    result[path].add(json_type(value))

    if isinstance(value, dict):
        for key, child in value.items():
            inspect_nested(child, f"{path}.{key}", result)

    elif isinstance(value, list):
        for child in value:
            inspect_nested(child, f"{path}[]", result)


def inspect_feed(feed_name):
    path = Path("data/samples") / f"{feed_name}.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload["data"]["stations"]

    present = Counter()
    nulls = Counter()
    types = defaultdict(set)
    nested_types = defaultdict(set)

    for row in rows:
        for field, value in row.items():
            present[field] += 1
            nulls[field] += int(value is None)
            types[field].add(json_type(value))
            inspect_nested(value, field, nested_types)

    print(f"\n=== {feed_name}: {len(rows)} trạm ===")
    print("Metadata cấp feed:", {
        key: value
        for key, value in payload.items()
        if key != "data"
    })

    print("\nKIỂM KÊ TRƯỜNG CẤP TRẠM")

    for field in sorted(present):
        print(
            f"{field}: "
            f"types={','.join(sorted(types[field]))}; "
            f"missing={len(rows) - present[field]}; "
            f"null={nulls[field]}"
        )

    print("\nKIỂU CỦA CÁC TRƯỜNG LỒNG")

    for field, observed_types in sorted(nested_types.items()):
        if "." in field or "[]" in field:
            print(f"{field}: {','.join(sorted(observed_types))}")


def main():
    for feed_name in ("station_status", "station_information"):
        inspect_feed(feed_name)


if __name__ == "__main__":
    main()
