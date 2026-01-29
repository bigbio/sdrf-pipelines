"""SDRF converter utilities and base classes.

This package contains all SDRF format converters:
- maxquant: MaxQuant parameter file converter
- msstats: MSstats annotation file converter
- normalyzerde: NormalyzerDE input file converter
- openms: OpenMS experimental design converter
"""

from sdrf_pipelines.converters.base import (
    SAMPLE_IDENTIFIER_PATTERN,
    BaseConverter,
    ConditionBuilder,
    SampleTracker,
)
from sdrf_pipelines.converters.maxquant.maxquant import Maxquant
from sdrf_pipelines.converters.msstats.msstats import Msstats
from sdrf_pipelines.converters.normalyzerde.normalyzerde import NormalyzerDE
from sdrf_pipelines.converters.openms.openms import OpenMS

__all__ = [
    # Base classes
    "BaseConverter",
    "ConditionBuilder",
    "SampleTracker",
    "SAMPLE_IDENTIFIER_PATTERN",
    # Converters
    "Maxquant",
    "Msstats",
    "NormalyzerDE",
    "OpenMS",
]
