"""请求/响应模型（Pydantic v2）。"""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, field_validator

Op = Literal["register", "correct", "revoke"]
STATUS_VALUES = {"in_service", "maintenance", "overhaul"}


class BatchItem(BaseModel):
    device_id: int = Field(ge=1, description="设备号")
    op: Op
    # register/correct 必填；revoke 忽略该字段
    new_status: str | None = Field(default=None, description="register/correct 时的目标状态")

    @field_validator("new_status")
    @classmethod
    def _check_status(cls, v: str | None) -> str | None:
        if v is not None and v not in STATUS_VALUES:
            raise ValueError(f"new_status 必须是 {sorted(STATUS_VALUES)} 之一")
        return v


class BatchRequest(BaseModel):
    items: list[BatchItem] = Field(min_length=1, description="整批变更，单事务提交")

    @field_validator("items")
    @classmethod
    def _no_duplicate_device(cls, v: list[BatchItem]) -> list[BatchItem]:
        ids = [i.device_id for i in v]
        if len(ids) != len(set(ids)):
            raise ValueError("同一批内设备号不能重复")
        return v


class BatchResponse(BaseModel):
    seq: int
    item_count: int


class QueryRequest(BaseModel):
    device_ids: list[int] = Field(min_length=1, max_length=32, description="1～32 个设备号")
    min_commit: int = Field(ge=1, description="调用方已签发（已见到）的最低提交号")
    main_read_cap: int = Field(ge=0, description="主表探查读取上限（未命中也计次）")
    log_read_cap: int = Field(ge=0, description="变更日志读取上限（按实读条数，撤销也计）")

    @field_validator("device_ids")
    @classmethod
    def _no_duplicate_device(cls, v: list[int]) -> list[int]:
        if len(v) != len(set(v)):
            raise ValueError("设备号不能重复")
        return v


class DeviceResult(BaseModel):
    device_id: int
    registered: bool
    status: str | None = None
    revoked: bool = False


class QueryResponse(BaseModel):
    used_seq: int
    replica_current_seq: int
    served_from: Literal["replica_snapshot", "primary_reconstructed"]
    results: list[DeviceResult]
    main_reads: int
    log_reads: int


class AdvanceRequest(BaseModel):
    max_commits: int = Field(default=1, ge=1, description="本次最多推进的提交数")


class AdvanceResponse(BaseModel):
    applied_commits: list[int]
    replica_seq_before: int
    replica_seq_after: int


class FaultRequest(BaseModel):
    seq: int = Field(ge=1, description="回放该提交号前注入故障（HTTP 409）")


class FaultResponse(BaseModel):
    fault_seq: int
    cleared: bool


class ReplicaStatus(BaseModel):
    replica_seq: int
    primary_seq: int
    pending_commits: list[int]
    fault_seq: int | None
