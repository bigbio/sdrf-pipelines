"""Constants for OpenMS conversion including label plexes and enzyme mappings."""

from importlib.resources import files
from typing import Any

import yaml
from yaml.nodes import MappingNode

_CHANNEL_MAP_RESOURCE = "channel_map.yaml"
_CHANNEL_MAP_SCHEMA_VERSION = 1


class _UniqueKeyLoader(yaml.SafeLoader):
    """Safe YAML loader that rejects silently overwritten mapping entries."""

    def construct_mapping(self, node: MappingNode, deep: bool = False) -> dict[Any, Any]:
        self.flatten_mapping(node)
        mapping: dict[Any, Any] = {}
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            if key in mapping:
                raise ValueError(f"Duplicate key {key!r} in {_CHANNEL_MAP_RESOURCE}")
            mapping[key] = self.construct_object(value_node, deep=deep)
        return mapping


def _validate_channel_map(document: object) -> dict[str, dict[str, int]]:
    """Validate and type the channel map document loaded from YAML."""
    if not isinstance(document, dict):
        raise ValueError(f"{_CHANNEL_MAP_RESOURCE} must contain a YAML mapping")
    if document.get("schema_version") != _CHANNEL_MAP_SCHEMA_VERSION:
        raise ValueError(f"{_CHANNEL_MAP_RESOURCE} must use schema_version {_CHANNEL_MAP_SCHEMA_VERSION}")

    raw_channel_map = document.get("channel_map")
    if not isinstance(raw_channel_map, dict):
        raise ValueError(f"{_CHANNEL_MAP_RESOURCE} must define a channel_map mapping")

    channel_map: dict[str, dict[str, int]] = {}
    for plex, raw_channels in raw_channel_map.items():
        if not isinstance(plex, str) or not plex:
            raise ValueError(f"{_CHANNEL_MAP_RESOURCE} plex names must be non-empty strings")
        if not isinstance(raw_channels, dict) or not raw_channels:
            raise ValueError(f"{_CHANNEL_MAP_RESOURCE} entry {plex!r} must be a non-empty mapping")

        channels: dict[str, int] = {}
        for label, channel_id in raw_channels.items():
            if not isinstance(label, str) or not label:
                raise ValueError(f"{_CHANNEL_MAP_RESOURCE} labels in {plex!r} must be non-empty strings")
            if not isinstance(channel_id, int) or isinstance(channel_id, bool):
                raise ValueError(f"Channel ID for {plex!r}/{label!r} must be an integer")
            channels[label] = channel_id

        expected_ids = list(range(1, len(channels) + 1))
        if sorted(channels.values()) != expected_ids:
            raise ValueError(f"Channel IDs for {plex!r} must be unique and contiguous from 1")
        channel_map[plex] = channels

    return channel_map


def _load_channel_map() -> dict[str, dict[str, int]]:
    """Load the canonical label-to-channel-ID mapping from the package data."""
    resource = files("sdrf_pipelines.converters.openms").joinpath(_CHANNEL_MAP_RESOURCE)
    document: object = yaml.load(resource.read_text(encoding="utf-8"), Loader=_UniqueKeyLoader)
    return _validate_channel_map(document)


# Labels are join keys, so their spelling and capitalization are public API.
CHANNEL_MAP: dict[str, dict[str, int]] = _load_channel_map()

# Backwards-compatible lookup names used by the OpenMS converter.
TMT_PLEXES: dict[str, dict[str, int]] = {
    plex: channels for plex, channels in CHANNEL_MAP.items() if plex.startswith("tmt")
}
ITRAQ_4PLEX: dict[str, int] = {label.lower(): channel_id for label, channel_id in CHANNEL_MAP["itraq4plex"].items()}
ITRAQ_8PLEX: dict[str, int] = {label.lower(): channel_id for label, channel_id in CHANNEL_MAP["itraq8plex"].items()}
SILAC_2PLEX: dict[str, int] = {label.lower(): channel_id for label, channel_id in CHANNEL_MAP["silac2plex"].items()}
SILAC_3PLEX: dict[str, int] = {label.lower(): channel_id for label, channel_id in CHANNEL_MAP["silac3plex"].items()}

# OpenMS enzyme name mappings (SDRF names to OpenMS names)
ENZYME_MAPPINGS = {
    "Glutamyl endopeptidase": "glutamyl endopeptidase",
    "Trypsin/p": "Trypsin/P",
    "Trypchymo": "TrypChymo",
    "Lys-c": "Lys-C",
    "Lys-c/p": "Lys-C/P",
    "Lys-n": "Lys-N",
    "Arg-c": "Arg-C",
    "Arg-c/p": "Arg-C/P",
    "Asp-n": "Asp-N",
    "Asp-n/b": "Asp-N/B",
    "Asp-n_ambic": "Asp-N_ambic",
    "Chymotrypsin/p": "Chymotrypsin/P",
    "Cnbr": "CNBr",
    "V8-de": "V8-DE",
    "V8-e": "V8-E",
    "Elastase-trypsin-chymotrypsin": "elastase-trypsin-chymotrypsin",
    "Pepsina": "PepsinA",
    "Unspecific cleavage": "unspecific cleavage",
    "No cleavage": "no cleavage",
}

# Default TMT modifications when not specified in SDRF
TMT_DEFAULT_MODS = {
    "tmt6plex": ["TMT6plex (K)", "TMT6plex (N-term)"],
    "tmt10plex": ["TMT6plex (K)", "TMT6plex (N-term)"],
    "tmt11plex": ["TMT6plex (K)", "TMT6plex (N-term)"],
    "tmt16plex": ["TMTpro (K)", "TMTpro (N-term)"],
    "tmt18plex": ["TMTpro (K)", "TMTpro (N-term)"],
    "tmt32plex": ["TMTpro (K)", "TMTpro (N-term)"],
    "tmt35plex": ["TMTpro (K)", "TMTpro (N-term)"],
}

# Default iTRAQ modifications when not specified in SDRF
ITRAQ_DEFAULT_MODS = {
    "itraq4plex": ["iTRAQ4plex (K)", "iTRAQ4plex (N-term)"],
    "itraq8plex": ["iTRAQ8plex (K)", "iTRAQ8plex (N-term)"],
}
