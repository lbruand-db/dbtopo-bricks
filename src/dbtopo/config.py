from __future__ import annotations

from pydantic import BaseModel

# All 96 metropolitan department codes (020 doesn't exist; Corsica is 02A/02B)
ALL_DEPARTMENTS = [f"{i:03d}" for i in range(1, 96) if i != 20] + ["02A", "02B"]


class IGNConfig(BaseModel):
    version: str = "3-5"
    version_date: str = "2025-09-15"
    projection: str = "LAMB93"
    departments: list[str] = ["001"]
    layers: list[str] = []


class DatabricksConfig(BaseModel):
    catalog: str = "dev_catalog"
    schema_name: str = "ign_bdtopo"
    volume: str = "bronze_volume"
    table_prefix: str = "ign_bdtopo_"
    batch_size: int = 10000


class TransformConfig(BaseModel):
    target_crs: str = "EPSG:4326"


class AppConfig(BaseModel):
    ign: IGNConfig = IGNConfig()
    databricks: DatabricksConfig = DatabricksConfig()
    transform: TransformConfig = TransformConfig()

    def volume_path(self) -> str:
        db = self.databricks
        return f"/Volumes/{db.catalog}/{db.schema_name}/{db.volume}"

    def resolve_departments(self) -> list[str]:
        if self.ign.departments == ["all"]:
            return ALL_DEPARTMENTS
        return self.ign.departments
