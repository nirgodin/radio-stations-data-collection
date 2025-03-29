from typing import Optional, Dict, List

from genie_common.utils import safe_nested_get
from genie_datastores.postgres.models import Artist

from data_collectors.consts.google_consts import (
    RESULTS,
    ADDRESS_COMPONENTS,
    TYPES,
    GEOMETRY,
    LOCATION,
    LATITUDE,
    LONGITUDE,
)
from data_collectors.contract import ISerializer
from data_collectors.logic.models import DBUpdateRequest, AddressComponentSetting


class GoogleGeocodingResponseSerializer(ISerializer):
    def serialize(self, artist_id: str, geocoding: dict) -> Optional[DBUpdateRequest]:
        first_result = self._extract_geocoding_first_result(geocoding)

        if first_result is not None:
            update_values = self._serialize_address_components(first_result)
            self._add_latitude_and_longitude_to_update_values(update_values, first_result)

            return DBUpdateRequest(id=artist_id, values=update_values)

    @staticmethod
    def _extract_geocoding_first_result(geocoding: dict) -> Optional[dict]:
        results = geocoding.get(RESULTS, [])

        if results and isinstance(results, list):
            return results[0]

    def _serialize_address_components(self, geocoding_result: dict) -> Dict[Artist, str]:
        components = {}
        raw_address_components = geocoding_result.get(ADDRESS_COMPONENTS, [])

        for setting in self._address_components_settings:
            serialized_component = self._serialize_single_address_component(setting, raw_address_components)

            if serialized_component is not None:
                components.update(serialized_component)

        return components

    @staticmethod
    def _serialize_single_address_component(
        setting: AddressComponentSetting, raw_address_components: List[dict]
    ) -> Optional[Dict[Artist, str]]:
        for component in raw_address_components:
            raw_component_types = component.get(TYPES, [])

            if setting.type in raw_component_types:
                component_value = component.get(setting.extract_field)

                return {setting.column: component_value}

    def _add_latitude_and_longitude_to_update_values(
        self, update_values: Dict[Artist, str], geocoding_result: dict
    ) -> None:
        for column, field_name in self._lat_long_columns_mapping.items():
            field_value = safe_nested_get(geocoding_result, [GEOMETRY, LOCATION, field_name], default=None)
            update_values[column] = field_value

    @property
    def _address_components_settings(self) -> List[AddressComponentSetting]:
        return [
            AddressComponentSetting(column=Artist.country, type="country", extract_field="short_name"),
            AddressComponentSetting(
                column=Artist.state,
                type="administrative_area_level_1",
                extract_field="short_name",
            ),
            AddressComponentSetting(
                column=Artist.county,
                type="administrative_area_level_2",
                extract_field="long_name",
            ),
            AddressComponentSetting(column=Artist.city, type="locality", extract_field="long_name"),
        ]

    @property
    def _lat_long_columns_mapping(self) -> Dict[Artist, str]:
        return {Artist.latitude: LATITUDE, Artist.longitude: LONGITUDE}
