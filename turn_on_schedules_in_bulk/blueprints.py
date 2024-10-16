from pathlib import Path
from typing import Literal, Optional, Sequence

from dagster import define_asset_job, Definitions, ScheduleDefinition, AssetSelection
from dagster_blueprints import YamlBlueprintsLoader
from dagster_blueprints.blueprint import Blueprint

# This class is used to construct dbt jobs via yaml files
class ScheduledJobBlueprint(Blueprint):
    type: Literal["scheduled_job"]
    name: str
    assets: Sequence
    cron: str
    schedule_type: Optional[str]
    schedule_expiration: Optional[str]

    def build_defs(self) -> Definitions:
        schedule_def = ScheduleDefinition(
        name=self.name,
        cron_schedule=self.cron,
        tags={"schedule-type": self.schedule_type, "schedule-expiration": self.schedule_expiration},
        job=define_asset_job(
            f"{self.name}_job",
            selection=AssetSelection.assets(self.assets))
        )
        return Definitions(schedules=[schedule_def])
        
        

# The loader will pick up any yaml files in the blueprints_jobs directory
loader = YamlBlueprintsLoader(
    per_file_blueprint_type=Sequence[ScheduledJobBlueprint],
    path=Path(__file__).parent / "blueprints_jobs",
)