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
# Epoch giây, từ 1970 tới 2100-01-01T00:00:00Z. Giá trị lớn hơn là mili/micro giây
# (sai đơn vị) và làm tràn số khi Spark đổi sang timestamp.
EpochSeconds = Annotated[int, Field(strict=True, ge=0, le=4102444800)]
NonEmptyString = Annotated[str, Field(min_length=1)]


class SourceModel(BaseModel):
    # Chỉ lấy các trường thuộc schema chuẩn hóa.
    # Dữ liệu nguồn đầy đủ được poller lưu riêng trong raw.json.
    model_config = ConfigDict(extra="ignore")


class VehicleTypeAvailable(SourceModel):
    vehicle_type_id: NonEmptyString
    count: NonNegativeInt


class StationStatus(SourceModel):
    station_id: NonEmptyString
    num_bikes_available: NonNegativeInt
    num_docks_available: NonNegativeInt

    is_installed: StrictBool
    is_renting: StrictBool
    is_returning: StrictBool

    last_reported: EpochSeconds
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


class RentalUris(SourceModel):
    # GBFS 2.3: deep links để thuê xe; nguồn hiện chỉ có android/ios.
    android: str | None = None
    ios: str | None = None
    web: str | None = None


class StationInformation(SourceModel):
    station_id: NonEmptyString
    name: NonEmptyString

    lat: float = Field(ge=-90, le=90, allow_inf_nan=False)
    lon: float = Field(ge=-180, le=180, allow_inf_nan=False)

    short_name: str | None = None
    capacity: NonNegativeInt | None = None
    is_virtual_station: StrictBool | None = None

    # Các trường còn lại của feed nguồn (STR-10 giữ đầy đủ các trường).
    region_id: str | None = None
    rental_uris: RentalUris | None = None
    is_charging: StrictBool | None = None


class FeedMetadata(BaseModel):
    last_updated: EpochSeconds
    collected_at: AwareDatetime


class PollMetadata(FeedMetadata):
    # Mã lần polling; mỗi feed có mã riêng, không dùng chung.
    poll_id: NonEmptyString


class ObservationMetadata(PollMetadata):
    # Metadata dành riêng cho station_status.
    schema_version: Literal[1] = 1

    @field_validator("schema_version", mode="before")
    @classmethod
    def validate_schema_version(cls, value):
        if type(value) is not int or value != 1:
            raise ValueError("schema_version phải là số nguyên 1")

        return value


class StationStatusMessage(StationStatus, ObservationMetadata):
    """Message station_status chuẩn hóa."""


class StationInformationMessage(StationInformation, PollMetadata):
    """Message station_information: có poll_id, không có schema_version."""


class InvalidMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: NonEmptyString
    error_type: NonEmptyString
    error_message: NonEmptyString
    raw_payload: Any
    detected_at: AwareDatetime
