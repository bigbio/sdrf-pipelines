"""Tests for plexDIA label detection and channel generation."""

import pytest

from sdrf_pipelines.converters.diann.plexdia import (
    build_channels_flag,
    build_fixed_mod_flag,
    detect_plexdia_type,
)


class TestDetectPlexdiaType:
    def test_label_free(self):
        labels = {"label free sample"}
        result = detect_plexdia_type(labels)
        assert result is None

    def test_mtraq_3plex(self):
        labels = {"MTRAQ0", "MTRAQ4", "MTRAQ8"}
        result = detect_plexdia_type(labels)
        assert result["type"] == "mtraq"
        assert result["plex"] == "mtraq3plex"

    def test_silac_2plex(self):
        labels = {"SILAC light", "SILAC heavy"}
        result = detect_plexdia_type(labels)
        assert result["type"] == "silac"
        assert result["plex"] == "silac2plex"

    def test_silac_3plex(self):
        labels = {"SILAC light", "SILAC medium", "SILAC heavy"}
        result = detect_plexdia_type(labels)
        assert result["type"] == "silac"
        assert result["plex"] == "silac3plex"

    def test_dimethyl_2plex(self):
        labels = {"DIMETHYL0", "DIMETHYL2"}
        result = detect_plexdia_type(labels)
        assert result["type"] == "dimethyl"
        assert result["plex"] == "dimethyl2plex"

    def test_dimethyl_3plex(self):
        labels = {"DIMETHYL0", "DIMETHYL2", "DIMETHYL4"}
        result = detect_plexdia_type(labels)
        assert result["type"] == "dimethyl"
        assert result["plex"] == "dimethyl3plex"

    def test_dimethyl_5plex(self):
        labels = {"DIMETHYL0", "DIMETHYL2", "DIMETHYL4", "DIMETHYL6", "DIMETHYL8"}
        result = detect_plexdia_type(labels)
        assert result["type"] == "dimethyl"
        assert result["plex"] == "dimethyl5plex"

    def test_unknown_label_raises(self):
        labels = {"UNKNOWN_LABEL"}
        with pytest.raises(ValueError, match="Unsupported label"):
            detect_plexdia_type(labels)


class TestBuildChannelsFlag:
    def test_mtraq_channels(self):
        plex_info = detect_plexdia_type({"MTRAQ0", "MTRAQ4", "MTRAQ8"})
        result = build_channels_flag(plex_info)
        assert "mTRAQ,0,nK,0:0" in result
        assert "mTRAQ,4,nK,4.0070994:4.0070994" in result
        assert "mTRAQ,8,nK,8.0141988132:8.0141988132" in result

    def test_silac_channels(self):
        plex_info = detect_plexdia_type({"SILAC light", "SILAC heavy"})
        result = build_channels_flag(plex_info)
        assert "SILAC,L,KR,0:0" in result
        assert "SILAC,H,KR,8.014199:10.008269" in result

    def test_dimethyl_channels(self):
        plex_info = detect_plexdia_type({"DIMETHYL0", "DIMETHYL2"})
        result = build_channels_flag(plex_info)
        assert "Dimethyl,0,nK,0:0" in result
        assert "Dimethyl,2,nK,2.0126:2.0126" in result


class TestBuildFixedModFlag:
    def test_mtraq_no_label_suffix(self):
        plex_info = detect_plexdia_type({"MTRAQ0", "MTRAQ4", "MTRAQ8"})
        result = build_fixed_mod_flag(plex_info)
        assert result == "mTRAQ,140.0949630177,nK"
        assert "label" not in result

    def test_silac_has_label_suffix(self):
        plex_info = detect_plexdia_type({"SILAC light", "SILAC heavy"})
        result = build_fixed_mod_flag(plex_info)
        assert result == "SILAC,0.0,KR,label"

    def test_dimethyl_no_label_suffix(self):
        plex_info = detect_plexdia_type({"DIMETHYL0", "DIMETHYL2"})
        result = build_fixed_mod_flag(plex_info)
        assert result == "Dimethyl,28.0313,nK"
        assert "label" not in result
