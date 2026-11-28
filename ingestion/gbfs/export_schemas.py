import json
from pathlib import Path

from ingestion.gbfs.schemas import (
    InvalidMessage,
    StationInformationMessage,
    StationStatusMessage,
)


def main():
    output_dir = Path("ingestion/gbfs/json_schemas")
    output_dir.mkdir(parents=True, exist_ok=True)

    models = {
        "station_status": StationStatusMessage,
        "station_information": StationInformationMessage,
        "invalid": InvalidMessage,
    }

    for name, model in models.items():
        schema = model.model_json_schema(mode="serialization")
        schema["$schema"] = (
            "https://json-schema.org/draft/2020-12/schema"
        )

        path = output_dir / f"{name}.schema.json"
        path.write_text(
            json.dumps(schema, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        print("Đã xuất:", path)


if __name__ == "__main__":
    main()
