"""Tests for the shared channel vocabulary (sdrf_pipelines.converters.channel_map)."""

import pytest

from sdrf_pipelines.converters import channel_map as cm
from sdrf_pipelines.converters.diann import constants as diann_constants
from sdrf_pipelines.converters.diann import plexdia
from sdrf_pipelines.converters.openms import constants as openms_constants


def test_shared_map_covers_openms_and_diann_reagents() -> None:
    plexes = set(cm.CHANNEL_MAP)
    # OpenMS isobaric + DIA-NN plexDIA + shared SILAC/LFQ, all in one file.
    assert {"tmt10plex", "itraq4plex"} <= plexes  # OpenMS
    assert {"mtraq3plex", "dimethyl2plex", "dimethyl3plex", "dimethyl5plex"} <= plexes  # DIA-NN
    assert {"silac2plex", "silac3plex", "lfq"} <= plexes  # shared


@pytest.mark.parametrize("plex", ["mtraq3plex", "dimethyl5plex", "lfq"])
def test_new_plexes_have_contiguous_one_based_ids(plex: str) -> None:
    assert sorted(cm.CHANNEL_MAP[plex].values()) == list(range(1, len(cm.CHANNEL_MAP[plex]) + 1))


@pytest.mark.parametrize(
    ("raw", "canonical"),
    [
        ("label free sample", "LFQ"),  # the canonical SDRF term -> LFQ
        ("Label Free Sample", "LFQ"),  # case-insensitive
        ("  LFQ  ", "LFQ"),  # stripped + canonical passes through
        ("label free", "label free"),  # not a declared synonym -> unchanged
        ("TMT126", "TMT126"),  # unknown-to-synonyms passes through unchanged
        ("SILAC light", "SILAC light"),
    ],
)
def test_normalize_label(raw: str, canonical: str) -> None:
    assert cm.normalize_label(raw) == canonical


def test_channel_labels_is_inverse_of_channel_map() -> None:
    for plex, channels in cm.CHANNEL_MAP.items():
        for label, index in channels.items():
            assert cm.CHANNEL_LABELS[plex][index] == label
    # ordinal -> label is the direction the write side (OpenMS/qpx) needs.
    assert cm.CHANNEL_LABELS["tmt10plex"][10] == "TMT131"


def test_labels_for_plex_is_index_ordered() -> None:
    assert cm.labels_for_plex("mtraq3plex") == ["MTRAQ0", "MTRAQ4", "MTRAQ8"]
    assert cm.labels_for_plex("silac3plex") == ["SILAC light", "SILAC medium", "SILAC heavy"]


def test_openms_reexports_shared_map() -> None:
    assert openms_constants.CHANNEL_MAP is cm.CHANNEL_MAP


def test_diann_plex_membership_is_sourced_from_shared_map() -> None:
    assert diann_constants.MTRAQ_PLEXES == {"mtraq3plex": cm.labels_for_plex("mtraq3plex")}
    assert diann_constants.SILAC_PLEXES == {p: cm.labels_for_plex(p) for p in ("silac2plex", "silac3plex")}
    # DIA-NN's local mass tables must be keyed by exactly the shared labels.
    for reagent, channels in [
        ("mtraq", diann_constants.MTRAQ_CHANNELS),
        ("dimethyl", diann_constants.DIMETHYL_CHANNELS),
        ("silac", diann_constants.SILAC_CHANNELS),
    ]:
        shared_labels = {label for plex in cm.CHANNEL_MAP if plex.startswith(reagent) for label in cm.CHANNEL_MAP[plex]}
        assert set(channels) == shared_labels


def test_plexdia_detects_label_free_via_synonym() -> None:
    # Both the SDRF ontology spelling and the canonical LFQ resolve to label-free.
    assert plexdia.detect_plexdia_type({"label free sample"}) is None
    assert plexdia.detect_plexdia_type({"LFQ"}) is None
    assert plexdia.detect_plexdia_type({"MTRAQ0", "MTRAQ4", "MTRAQ8"})["type"] == "mtraq"
