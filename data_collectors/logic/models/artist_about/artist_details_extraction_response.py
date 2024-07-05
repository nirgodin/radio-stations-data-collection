from genie_datastores.models import DataSource
from pydantic import BaseModel

from data_collectors.logic.models.artist_about.artist_extracted_details import ArtistExtractedDetails
from data_collectors.logic.models.artist_about.artist_existing_details import ArtistExistingDetails


class ArtistDetailsExtractionResponse(BaseModel):
    existing_details: ArtistExistingDetails
    extracted_details: ArtistExtractedDetails
    data_source: DataSource
