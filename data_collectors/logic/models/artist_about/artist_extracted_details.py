from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Any

from genie_datastores.postgres.models import Gender
from pydantic import BaseModel


class BaseDecision(BaseModel):
    value: Optional[Any]
    evidence: Optional[str]
    confidence: Optional[float]


class DateDecision(BaseDecision):
    value: Optional[datetime]


class StringDecision(BaseDecision):
    value: Optional[str]


class GenderDecision(BaseDecision):
    value: Optional[Gender]


class ArtistExtractedDetails(BaseModel):
    birth_date: Optional[DateDecision]
    death_date: Optional[DateDecision]
    origin: Optional[StringDecision]
    gender: Optional[GenderDecision]
