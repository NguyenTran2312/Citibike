from __future__ import annotations

import hashlib
import json
import zipfile
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass
class CsvMemberInfo:
    member_name: str
    headers: list[str]
    row_count: int


@dataclass
class TripManifest:
    month: str
    source_url: str
    archive_name: str
    size_bytes: int
    sha256: str
    zip_members: list[str]
    csv_members: list[CsvMemberInfo] = field(default_factory=list)
    status: str = "staged"
    downloaded_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    error_message: str | None = None

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2, ensure_ascii=False)

    @classmethod
    def from_json(cls, raw_text: str) -> "TripManifest":
        data: dict[str, Any] = json.loads(raw_text)
        csv_members = [
            CsvMemberInfo(**item) for item in data.get("csv_members", [])
        ]
        return cls(
            month=data["month"],
            source_url=data["source_url"],
            archive_name=data["archive_name"],
            size_bytes=int(data["size_bytes"]),
            sha256=data["sha256"],
            zip_members=list(data.get("zip_members", [])),
            csv_members=csv_members,
            status=data.get("status", "unknown"),
            downloaded_at=data.get("downloaded_at", ""),
            error_message=data.get("error_message"),
        )


def compute_sha256(file_path: Path) -> str:
    h = hashlib.sha256()
    with file_path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def inspect_and_extract_zip(
    zip_path: Path, extract_dir: Path, source_url: str, month: str
) -> tuple[TripManifest, list[Path]]:
    extract_dir.mkdir(parents=True, exist_ok=True)
    sha256 = compute_sha256(zip_path)
    size_bytes = zip_path.stat().st_size

    extracted_csvs: list[Path] = []
    csv_members: list[CsvMemberInfo] = []

    with zipfile.ZipFile(zip_path, "r") as zf:
        members = [
            name for name in zf.namelist()
            if not name.endswith("/") and not Path(name).name.startswith("__MACOSX")
        ]
        for member in members:
            target = extract_dir / Path(member).name
            with zf.open(member) as src, target.open("wb") as dst:
                while True:
                    buf = src.read(1024 * 1024)
                    if not buf:
                        break
                    dst.write(buf)

            if target.suffix.lower() == ".csv":
                extracted_csvs.append(target)
                headers: list[str] = []
                total_lines = 0
                with target.open("r", encoding="utf-8", errors="replace") as csv_f:
                    for idx, line in enumerate(csv_f):
                        total_lines += 1
                        if idx == 0:
                            headers = [col.strip().strip('"') for col in line.rstrip("\r\n").split(",")]
                data_rows = max(0, total_lines - 1)
                csv_members.append(
                    CsvMemberInfo(
                        member_name=target.name,
                        headers=headers,
                        row_count=data_rows,
                    )
                )

    manifest = TripManifest(
        month=month,
        source_url=source_url,
        archive_name=zip_path.name,
        size_bytes=size_bytes,
        sha256=sha256,
        zip_members=members,
        csv_members=csv_members,
        status="staged",
    )
    return manifest, extracted_csvs