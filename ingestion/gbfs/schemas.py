from typing import Annotated, Any, Literal

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    field_validator,
)


NonNegativeInt = Annotated[int, Field(strict=True, ge=0)]
NonEmptyString = Annotated[str, Field(min_length=1)]


class SourceModel(BaseModel):
    # Giữ trường nguồn mới xuất hiện, không âm thầm bỏ mất.
    model_config = ConfigDict(extra="allow")


class VehicleTypeAvailable(SourceModel):
    vehicle_type_id: NonEmptyString
    count: NonNegativeInt


class RentalURIs(SourceModel):
    android: str | None = None
    ios: str | None = None
    web: str | None = None


class StationStatus(SourceModel):
    station_id: NonEmptyString

    # Chính sách bản nháp: bắt buộc có và không nhận null.
    num_bikes_available: NonNegativeInt
    num_docks_available: NonNegativeInt

    is_installed: StrictBool
    is_renting: StrictBool
    is_returning: StrictBool

    last_reported: NonNegativeInt

    # Các trường bổ sung: không tự giả định thiếu nghĩa là 0.
    num_bikes_disabled: NonNegativeInt | None = None
    num_docks_disabled: NonNegativeInt | None = None
    num_ebikes_available: NonNegativeInt | None = None
    num_scooters_available: NonNegativeInt | None = None
    num_scooters_unavailable: NonNegativeInt | None = None

    vehicle_types_available: list[VehicleTypeAvailable] | None = None

    @field_validator(
        "is_installed",
        "is_renting",
        "is_returning",
        mode="before",
    )
    @classmethod
    def normalize_flag(cls, value):
        if type(value) is bool:
            return value

        if type(value) is int and value in (0, 1):
            return bool(value)

        raise ValueError("Cờ hoạt động phải là 0/1 hoặc boolean")


class StationInformation(SourceModel):
    station_id: NonEmptyString
    name: NonEmptyString

    lat: float = Field(ge=-90, le=90, allow_inf_nan=False)
    lon: float = Field(ge=-180, le=180, allow_inf_nan=False)

    short_name: str | None = None
    capacity: NonNegativeInt | None = None
    is_virtual_station: StrictBool | None = None

    region_id: str | None = None
    is_charging: StrictBool | None = None
    rental_uris: RentalURIs | None = None


class ObservationMetadata(BaseModel):
    schema_version: Literal[1] = 1
    poll_id: NonEmptyString
    last_updated: NonNegativeInt
    collected_at: AwareDatetime


class StationStatusMessage(StationStatus, ObservationMetadata):
    """Dữ liệu trạm và metadata nằm cùng cấp trong message."""


class StationInformationMessage(
    StationInformation,
    ObservationMetadata,
):
    """Message thông tin trạm chuẩn hóa."""


class InvalidMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: NonEmptyString
    error_type: NonEmptyString
    error_message: NonEmptyString
    raw_payload: Any
    detected_at: AwareDatetime

