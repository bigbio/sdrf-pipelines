"""Shared quantification-channel vocabulary for the SDRF converters.

A single source of truth (``channel_map.yaml``) for the label<->channel-index
mapping used by every converter that has to reason about labeled channels:

* OpenMS  — isobaric reagents (TMT, iTRAQ) plus SILAC / LFQ.
* DIA-NN  — plexDIA reagents (mTRAQ, Dimethyl, SILAC) plus LFQ.

Only the *vocabulary* is shared here — plex membership and the canonical
``label -> 1-based index`` mapping, plus ``synonyms`` so a converter can accept
whatever spelling the SDRF ``comment[label]`` used (e.g. the ontology name
``"label free sample"`` for ``LFQ``). Reagent-specific quantitative data
(channel masses, fixed modifications used to build the DIA-NN ``--channels``
flag) stays in the individual converters, keyed by these same labels.
"""

from importlib.resources import files
from typing import Any

import yaml
from yaml.nodes import MappingNode

_CHANNEL_MAP_RESOURCE = "channel_map.yaml"
_CHANNEL_MAP_SCHEMA_VERSION = 2


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


def _validate_channel_map(raw_channel_map: object) -> dict[str, dict[str, int]]:
    """Validate the ``channel_map`` section: {plex: {label: contiguous 1-based id}}."""
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


def _validate_synonyms(raw_synonyms: object, known_labels: set[str]) -> dict[str, list[str]]:
    """Validate the optional ``synonyms`` section: {canonical_label: [alternates]}."""
    if raw_synonyms is None:
        return {}
    if not isinstance(raw_synonyms, dict):
        raise ValueError(f"{_CHANNEL_MAP_RESOURCE} synonyms must be a mapping")

    synonyms: dict[str, list[str]] = {}
    for canonical, alternates in raw_synonyms.items():
        if not isinstance(canonical, str) or canonical not in known_labels:
            raise ValueError(f"{_CHANNEL_MAP_RESOURCE} synonym key {canonical!r} is not a known channel label")
        if not isinstance(alternates, list) or not all(isinstance(a, str) and a for a in alternates):
            raise ValueError(f"{_CHANNEL_MAP_RESOURCE} synonyms for {canonical!r} must be a list of non-empty strings")
        synonyms[canonical] = list(alternates)
    return synonyms


def _load() -> tuple[dict[str, dict[str, int]], dict[str, list[str]]]:
    resource = files("sdrf_pipelines.converters").joinpath(_CHANNEL_MAP_RESOURCE)
    document: object = yaml.load(resource.read_text(encoding="utf-8"), Loader=_UniqueKeyLoader)
    if not isinstance(document, dict):
        raise ValueError(f"{_CHANNEL_MAP_RESOURCE} must contain a YAML mapping")
    if document.get("schema_version") != _CHANNEL_MAP_SCHEMA_VERSION:
        raise ValueError(f"{_CHANNEL_MAP_RESOURCE} must use schema_version {_CHANNEL_MAP_SCHEMA_VERSION}")

    channel_map = _validate_channel_map(document.get("channel_map"))
    known_labels = {label for channels in channel_map.values() for label in channels}
    synonyms = _validate_synonyms(document.get("synonyms"), known_labels)
    return channel_map, synonyms


# Labels are join keys, so their spelling and capitalization are public API.
CHANNEL_MAP: dict[str, dict[str, int]]
SYNONYMS: dict[str, list[str]]
CHANNEL_MAP, SYNONYMS = _load()

# Inverse view: {plex: {1-based index: label}} — the direction the write side
# (OpenMS ConsensusMap export / qpx intensity labels) needs, so consumers don't
# each re-invert CHANNEL_MAP and risk drifting.
CHANNEL_LABELS: dict[str, dict[int, str]] = {
    plex: {index: label for label, index in channels.items()} for plex, channels in CHANNEL_MAP.items()
}

# Case-insensitive alternate-spelling -> canonical label (includes each
# canonical label mapping to itself).
_SYNONYM_TO_CANONICAL: dict[str, str] = {}
for _canonical, _alternates in SYNONYMS.items():
    _SYNONYM_TO_CANONICAL[_canonical.lower()] = _canonical
    for _alt in _alternates:
        _SYNONYM_TO_CANONICAL[_alt.lower()] = _canonical


def normalize_label(label: str) -> str:
    """Return the canonical channel label for ``label``.

    Maps a known synonym (e.g. the SDRF ontology spelling ``"label free
    sample"``) to its canonical form (``"LFQ"``), case-insensitively. Labels
    that are not synonyms are returned stripped but otherwise unchanged, so
    canonical labels (``"TMT126"``, ``"SILAC light"``) pass through as-is.
    """
    stripped = label.strip()
    return _SYNONYM_TO_CANONICAL.get(stripped.lower(), stripped)


def labels_for_plex(plex: str) -> list[str]:
    """Return a plex's labels ordered by their 1-based channel index."""
    return [CHANNEL_LABELS[plex][i] for i in sorted(CHANNEL_LABELS[plex])]
