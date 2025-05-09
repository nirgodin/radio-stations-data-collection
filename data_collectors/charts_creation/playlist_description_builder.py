from datetime import datetime
from typing import Dict, List

from genie_datastores.postgres.models import Chart, ChartEntry

DESCRIPTION_FORMAT = b"\xd7\x94\xd7\x9e\xd7\xa6\xd7\xa2\xd7\x93 \xd7\x94{chart_type} \xd7\x94\xd7\xa9\xd7\x91\xd7\x95\xd7\xa2\xd7\x99 \xd7\xa9\xd7\x9c \xd7\x92\xd7\x9c\xd7\x92\xd7\x9c\xd7\xa6 \xd7\x9c\xd7\xaa\xd7\x90\xd7\xa8\xd7\x99\xd7\x9a {date}".decode(
    "utf-8"
)
MISSING = b". \xd7\x97\xd7\xa1\xd7\xa8\xd7\x99\xd7\x9d: ".decode("utf-8")
MISSING_ENTRY_FORMAT = b"{entry_key} (\xd7\x9e\xd7\xa7\xd7\x95\xd7\x9d {entry_position})".decode("utf-8")
ISRAELI = b"\xd7\x99\xd7\xa9\xd7\xa8\xd7\x90\xd7\x9c\xd7\x99".decode("utf-8")
INTERNATIONAL = b"\xd7\x91\xd7\x99\xd7\xa0\xd7\x9c\xd7\x90\xd7\x95\xd7\x9e\xd7\x99".decode("utf-8")


class PlaylistDescriptionBuilder:
    def build(self, chart: Chart, formatted_date: str, entries: List[ChartEntry]) -> str:
        description = self._format_base_description(chart, formatted_date)
        missing_entries = [entry for entry in entries if entry.track_id is None]

        return self._add_missing_entries(description, missing_entries) if missing_entries else description

    def _format_base_description(self, chart: Chart, formatted_date: str) -> str:
        chart_name = self._chart_name_map[chart]
        return DESCRIPTION_FORMAT.format(date=formatted_date, chart_type=chart_name)

    def _add_missing_entries(self, description: str, missing_entries: List[ChartEntry]) -> str:
        description += MISSING
        missing_entries_descriptions = [self._to_missing_entry_description(entry) for entry in missing_entries]
        missing_entries_string = "; ".join(missing_entries_descriptions)

        return description + missing_entries_string

    @staticmethod
    def _to_missing_entry_description(entry: ChartEntry) -> str:
        return MISSING_ENTRY_FORMAT.format(entry_key=entry.key, entry_position=entry.position)

    @property
    def _chart_name_map(self) -> Dict[Chart, str]:
        return {
            Chart.GLGLZ_WEEKLY_ISRAELI: ISRAELI,
            Chart.GLGLZ_WEEKLY_INTERNATIONAL: INTERNATIONAL,
        }
