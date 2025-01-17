from __future__ import annotations
from typing import NamedTuple, ClassVar, Union
from functools import cached_property
import logging
import dataclasses as dc
import json
import itertools

logger = logging.getLogger(__name__)


class ECG(NamedTuple):
    device: str
    ts: int
    values: list[float]


@dc.dataclass
class ECGBulk:
    """ECG Bulk for Calcuration."""

    ecgs: list[ECG]

    @property
    def device(self) -> str:
        return self.ecgs[0].device

    @property
    def ts_range(self) -> tuple[int, int]:
        return (self.ecgs[0].ts, self.ecgs[-1].ts)

    @cached_property
    def values(self) -> list[float]:
        values = [ecg.values for ecg in self.ecgs]
        return list(itertools.chain(*values))

    def is_valid(self) -> bool:
        devices = {ecg.device for ecg in self.ecgs}
        return True if len(devices) == 1 else False


class HeartRate(NamedTuple):
    device: str
    ts: int
    value: int


@dc.dataclass
class HeartRateTelemetry:
    THINGSBOARD_TOPIC: ClassVar[str] = "v1/gateway/telemetry"
    MOSQUITTO_TOPIC: ClassVar[str] = "devices/hr"
    heart_rates: list[HeartRate]

    def export_thingsboard_message(self) -> Union[str, None]:
        msgs = {}
        for hr in self.heart_rates:
            msg = {f"{hr.device}": [{"ts": hr.ts, "values": {"hr": hr.value}}]}
            msgs.update(msg)
        return json.dumps(msgs) if msgs else None

    def export_mosquitto_messages(self) -> Union[list[str], None]:
        msgs = []
        for hr in self.heart_rates:
            msg = f"noti=hr:params={hr.device},{hr.value}"
            msgs.append(msg)
        return msgs if msgs else None


@dc.dataclass
class EcgData:
    device: str
    ts: int
    idx: int
    values: list[float]

    @property
    def redis_key(self) -> str:
        """Key for Redis"""
        return f"ecg:{self.device}:{self.idx}"

    @property
    def redis_values(self) -> str:
        """Values for Redis"""
        return json.dumps({"ts": self.ts, "ecg": self.values}).encode("utf-8")

    @staticmethod
    def from_telemetry(telemetry_data: dict) -> EcgData:
        device_name = str(telemetry_data["deviceName"])
        field_ts = int(telemetry_data["telemetry"][0]["ts"])
        field_ecg = json.dumps(telemetry_data["telemetry"][0]["values"]["ecgData"])
        field_ecg_index = int(telemetry_data["telemetry"][0]["values"]["ecgDataIndex"])
        return EcgData(
            device=device_name,
            ts=field_ts,
            idx=field_ecg_index,
            values=json.loads(field_ecg),
        )
