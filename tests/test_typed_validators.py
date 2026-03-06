import pandas as pd

from sdrf_pipelines.sdrf.validators import get_validator


class TestNumberWithUnitValidator:
    """Tests for NumberWithUnitValidator."""

    def setup_method(self):
        cls = get_validator("number_with_unit")
        assert cls is not None, "number_with_unit validator not registered"
        self.cls = cls

    def test_valid_integer_with_unit(self):
        v = self.cls(params={"units": ["ppm", "Da"]})
        series = pd.Series(["10 ppm", "500 Da", "0 ppm"])
        errors = v.validate(series, column_name="comment[precursor mass tolerance]")
        assert len(errors) == 0

    def test_valid_decimal_with_unit(self):
        v = self.cls(params={"units": ["ppm", "Da"]})
        series = pd.Series(["0.5 Da", "10.25 ppm"])
        errors = v.validate(series, column_name="comment[precursor mass tolerance]")
        assert len(errors) == 0

    def test_invalid_unit(self):
        v = self.cls(params={"units": ["ppm", "Da"]})
        series = pd.Series(["10 eV"])
        errors = v.validate(series, column_name="comment[precursor mass tolerance]")
        assert len(errors) == 1
        assert "ppm" in errors[0].suggestion or "Da" in errors[0].suggestion

    def test_missing_unit(self):
        v = self.cls(params={"units": ["ppm", "Da"]})
        series = pd.Series(["10"])
        errors = v.validate(series, column_name="comment[precursor mass tolerance]")
        assert len(errors) == 1

    def test_negative_not_allowed_by_default(self):
        v = self.cls(params={"units": ["ppm"]})
        series = pd.Series(["-5 ppm"])
        errors = v.validate(series, column_name="test")
        assert len(errors) == 1

    def test_negative_allowed(self):
        v = self.cls(params={"units": ["°C"], "allow_negative": True})
        series = pd.Series(["-20 °C", "37 °C"])
        errors = v.validate(series, column_name="characteristics[temperature]")
        assert len(errors) == 0

    def test_decimal_disabled(self):
        v = self.cls(params={"units": ["mg"], "allow_decimal": False})
        series = pd.Series(["1.5 mg"])
        errors = v.validate(series, column_name="test")
        assert len(errors) == 1

    def test_special_values(self):
        v = self.cls(params={"units": ["°C"], "allow_negative": True, "special_values": ["room temperature"]})
        series = pd.Series(["room temperature", "37 °C", "-4 °C"])
        errors = v.validate(series, column_name="characteristics[crosslinking temperature]")
        assert len(errors) == 0

    def test_not_available_respected(self):
        v = self.cls(params={"units": ["ppm"], "allow_not_available": True})
        series = pd.Series(["not available", "10 ppm"])
        errors = v.validate(series, column_name="test")
        assert len(errors) == 0

    def test_not_available_rejected(self):
        v = self.cls(params={"units": ["ppm"], "allow_not_available": False})
        series = pd.Series(["not available"])
        errors = v.validate(series, column_name="test")
        assert len(errors) == 1

    def test_not_applicable_respected(self):
        v = self.cls(params={"units": ["ppm"], "allow_not_applicable": True})
        series = pd.Series(["not applicable"])
        errors = v.validate(series, column_name="test")
        assert len(errors) == 0

    def test_empty_values_skipped(self):
        v = self.cls(params={"units": ["ppm"]})
        series = pd.Series(["", "10 ppm", "  "])
        errors = v.validate(series, column_name="test")
        assert len(errors) == 0

    def test_no_space_between_number_and_unit(self):
        v = self.cls(params={"units": ["ppm"]})
        series = pd.Series(["10ppm"])
        errors = v.validate(series, column_name="test")
        assert len(errors) == 0

    def test_unicode_unit(self):
        v = self.cls(params={"units": ["µM", "mM"]})
        series = pd.Series(["100 µM", "1 mM"])
        errors = v.validate(series, column_name="test")
        assert len(errors) == 0


class TestAccessionValidator:
    """Tests for AccessionValidator."""

    def setup_method(self):
        cls = get_validator("accession")
        assert cls is not None, "accession validator not registered"
        self.cls = cls

    def test_biosample_format_valid(self):
        v = self.cls(params={"format": "biosample"})
        series = pd.Series(["SAMN12345678", "SAMEA9876543", "SAMD1111111"])
        errors = v.validate(series, column_name="characteristics[biosample accession number]")
        assert len(errors) == 0

    def test_biosample_format_invalid(self):
        v = self.cls(params={"format": "biosample"})
        series = pd.Series(["SAMX123", "SAM123", "12345"])
        errors = v.validate(series, column_name="characteristics[biosample accession number]")
        assert len(errors) == 3

    def test_cellosaurus_format_valid(self):
        v = self.cls(params={"format": "cellosaurus"})
        series = pd.Series(["CVCL_0030", "CVCL_A1B2"])
        errors = v.validate(series, column_name="characteristics[cellosaurus accession]")
        assert len(errors) == 0

    def test_cellosaurus_format_invalid(self):
        v = self.cls(params={"format": "cellosaurus"})
        series = pd.Series(["CVCL_lowercase", "CVCL-0030"])
        errors = v.validate(series, column_name="characteristics[cellosaurus accession]")
        assert len(errors) == 2

    def test_custom_prefix_suffix(self):
        v = self.cls(params={"prefix": "PXD", "suffix": r"\d+"})
        series = pd.Series(["PXD000001", "PXD123456"])
        errors = v.validate(series, column_name="test")
        assert len(errors) == 0

    def test_custom_prefix_default_suffix(self):
        v = self.cls(params={"prefix": "PXD"})
        series = pd.Series(["PXD000001", "PXDabc"])
        errors = v.validate(series, column_name="test")
        assert len(errors) == 1

    def test_prefix_with_regex(self):
        v = self.cls(params={"prefix": "[A-Z]+", "suffix": r"\d+"})
        series = pd.Series(["ABC123", "XY99"])
        errors = v.validate(series, column_name="comment[metagenome accession]")
        assert len(errors) == 0

    def test_not_available_respected(self):
        v = self.cls(params={"format": "biosample", "allow_not_available": True})
        series = pd.Series(["not available", "SAMN12345"])
        errors = v.validate(series, column_name="test")
        assert len(errors) == 0

    def test_not_applicable_respected(self):
        v = self.cls(params={"format": "biosample", "allow_not_applicable": True})
        series = pd.Series(["not applicable"])
        errors = v.validate(series, column_name="test")
        assert len(errors) == 0

    def test_empty_values_skipped(self):
        v = self.cls(params={"format": "biosample"})
        series = pd.Series(["", "SAMN12345", "  "])
        errors = v.validate(series, column_name="test")
        assert len(errors) == 0

    def test_unknown_format_raises(self):
        v = self.cls(params={"format": "unknown_format"})
        series = pd.Series(["ABC123"])
        errors = v.validate(series, column_name="test")
        assert len(errors) >= 1


class TestIdentifierValidator:
    """Tests for IdentifierValidator."""

    def setup_method(self):
        cls = get_validator("identifier")
        assert cls is not None, "identifier validator not registered"
        self.cls = cls

    def test_valid_default_charset(self):
        v = self.cls(params={})
        series = pd.Series(["patient_01", "sample-A", "ABC123"])
        errors = v.validate(series, column_name="characteristics[individual]")
        assert len(errors) == 0

    def test_invalid_default_charset(self):
        v = self.cls(params={})
        series = pd.Series(["patient 01", "sample@A"])
        errors = v.validate(series, column_name="characteristics[individual]")
        assert len(errors) == 2

    def test_special_values(self):
        v = self.cls(params={"special_values": ["anonymized", "pooled"]})
        series = pd.Series(["anonymized", "pooled", "patient_01"])
        errors = v.validate(series, column_name="characteristics[individual]")
        assert len(errors) == 0

    def test_custom_charset(self):
        v = self.cls(params={"charset": "[A-Za-z0-9_.-]"})
        series = pd.Series(["cell.1", "cell-2", "cell_3"])
        errors = v.validate(series, column_name="characteristics[cell identifier]")
        assert len(errors) == 0

    def test_not_available_respected(self):
        v = self.cls(params={"allow_not_available": True})
        series = pd.Series(["not available", "patient_01"])
        errors = v.validate(series, column_name="test")
        assert len(errors) == 0

    def test_not_applicable_respected(self):
        v = self.cls(params={"allow_not_applicable": True})
        series = pd.Series(["not applicable"])
        errors = v.validate(series, column_name="test")
        assert len(errors) == 0

    def test_empty_values_skipped(self):
        v = self.cls(params={})
        series = pd.Series(["", "patient_01"])
        errors = v.validate(series, column_name="test")
        assert len(errors) == 0


class TestDateValidator:
    """Tests for DateValidator."""

    def setup_method(self):
        cls = get_validator("date")
        assert cls is not None, "date validator not registered"
        self.cls = cls

    def test_full_date(self):
        v = self.cls(params={"format": "iso8601", "precision": ["year", "month", "day"]})
        series = pd.Series(["2024-01-15", "2023-12-31"])
        errors = v.validate(series, column_name="characteristics[collection date]")
        assert len(errors) == 0

    def test_year_month(self):
        v = self.cls(params={"format": "iso8601", "precision": ["year", "month", "day"]})
        series = pd.Series(["2024-01", "2023-06"])
        errors = v.validate(series, column_name="characteristics[collection date]")
        assert len(errors) == 0

    def test_year_only(self):
        v = self.cls(params={"format": "iso8601", "precision": ["year", "month", "day"]})
        series = pd.Series(["2024", "2023"])
        errors = v.validate(series, column_name="characteristics[collection date]")
        assert len(errors) == 0

    def test_restricted_precision(self):
        v = self.cls(params={"format": "iso8601", "precision": ["day"]})
        series = pd.Series(["2024", "2024-01"])
        errors = v.validate(series, column_name="test")
        assert len(errors) == 2

    def test_invalid_date_format(self):
        v = self.cls(params={"format": "iso8601", "precision": ["year", "month", "day"]})
        series = pd.Series(["01/15/2024", "Jan 2024", "2024.01.15"])
        errors = v.validate(series, column_name="test")
        assert len(errors) == 3

    def test_invalid_month(self):
        v = self.cls(params={"format": "iso8601", "precision": ["year", "month", "day"]})
        series = pd.Series(["2024-13-01", "2024-00-01"])
        errors = v.validate(series, column_name="test")
        assert len(errors) == 2

    def test_not_available_respected(self):
        v = self.cls(params={"format": "iso8601", "precision": ["year", "month", "day"], "allow_not_available": True})
        series = pd.Series(["not available", "2024-01-15"])
        errors = v.validate(series, column_name="test")
        assert len(errors) == 0

    def test_empty_values_skipped(self):
        v = self.cls(params={"format": "iso8601", "precision": ["year", "month", "day"]})
        series = pd.Series(["", "2024-01-15"])
        errors = v.validate(series, column_name="test")
        assert len(errors) == 0


class TestStructuredKVValidator:
    """Tests for StructuredKVValidator."""

    def setup_method(self):
        cls = get_validator("structured_kv")
        assert cls is not None, "structured_kv validator not registered"
        self.cls = cls

    def test_valid_crosslinker_format(self):
        v = self.cls(
            params={
                "separator": ";",
                "fields": [
                    {"key": "NT", "value": ".+"},
                    {"key": "AC", "value": r"(XLMOD|CHEBI|UNIMOD):\d+"},
                ],
            }
        )
        series = pd.Series(["NT=DSS;AC=XLMOD:02001", "NT=BS3;AC=CHEBI:73327"])
        errors = v.validate(series, column_name="comment[cross-linker]")
        assert len(errors) == 0

    def test_missing_required_key(self):
        v = self.cls(
            params={
                "separator": ";",
                "fields": [
                    {"key": "NT", "value": ".+"},
                    {"key": "AC", "value": ".+"},
                ],
            }
        )
        series = pd.Series(["NT=DSS"])
        errors = v.validate(series, column_name="test")
        assert len(errors) == 1

    def test_invalid_value_pattern(self):
        v = self.cls(
            params={
                "separator": ";",
                "fields": [
                    {"key": "NT", "value": ".+"},
                    {"key": "AC", "value": r"(XLMOD|CHEBI):\d+"},
                ],
            }
        )
        series = pd.Series(["NT=DSS;AC=INVALID:abc"])
        errors = v.validate(series, column_name="test")
        assert len(errors) == 1

    def test_sdrf_template_format(self):
        v = self.cls(
            params={
                "separator": ";",
                "fields": [
                    {"key": "NT", "value": r"[\w-]+"},
                    {"key": "VV", "value": r"v\d+\.\d+\.\d+"},
                ],
            }
        )
        series = pd.Series(["NT=ms-proteomics;VV=v1.1.0"])
        errors = v.validate(series, column_name="comment[sdrf template]")
        assert len(errors) == 0

    def test_not_available_respected(self):
        v = self.cls(
            params={
                "separator": ";",
                "fields": [{"key": "NT", "value": ".+"}],
                "allow_not_available": True,
            }
        )
        series = pd.Series(["not available", "NT=value"])
        errors = v.validate(series, column_name="test")
        assert len(errors) == 0

    def test_empty_values_skipped(self):
        v = self.cls(
            params={
                "separator": ";",
                "fields": [{"key": "NT", "value": ".+"}],
            }
        )
        series = pd.Series(["", "NT=value"])
        errors = v.validate(series, column_name="test")
        assert len(errors) == 0


class TestSemverValidator:
    """Tests for SemverValidator."""

    def setup_method(self):
        cls = get_validator("semver")
        assert cls is not None, "semver validator not registered"
        self.cls = cls

    def test_valid_semver(self):
        v = self.cls(params={"prefix": "v"})
        series = pd.Series(["v1.0.0", "v2.3.1", "v0.1.0"])
        errors = v.validate(series, column_name="comment[sdrf version]")
        assert len(errors) == 0

    def test_valid_semver_no_prefix(self):
        v = self.cls(params={})
        series = pd.Series(["1.0.0", "2.3.1"])
        errors = v.validate(series, column_name="test")
        assert len(errors) == 0

    def test_valid_prerelease(self):
        v = self.cls(params={"prefix": "v", "allow_prerelease": True})
        series = pd.Series(["v1.0.0-alpha", "v1.0.0-rc.1", "v2.0.0-beta.2"])
        errors = v.validate(series, column_name="test")
        assert len(errors) == 0

    def test_prerelease_rejected(self):
        v = self.cls(params={"prefix": "v", "allow_prerelease": False})
        series = pd.Series(["v1.0.0-alpha"])
        errors = v.validate(series, column_name="test")
        assert len(errors) == 1

    def test_invalid_semver(self):
        v = self.cls(params={"prefix": "v"})
        series = pd.Series(["v1.0", "v1", "1.0.0", "vx.y.z"])
        errors = v.validate(series, column_name="test")
        assert len(errors) == 4

    def test_empty_values_skipped(self):
        v = self.cls(params={"prefix": "v"})
        series = pd.Series(["", "v1.0.0"])
        errors = v.validate(series, column_name="test")
        assert len(errors) == 0

    def test_sentinel_values_skipped(self):
        v = self.cls(params={"prefix": "v"})
        series = pd.Series(["not available", "not applicable", "v1.0.0"])
        errors = v.validate(series, column_name="test")
        assert len(errors) == 0


class TestPatternValidatorSentinels:
    """Tests for PatternValidator sentinel handling."""

    def setup_method(self):
        cls = get_validator("pattern")
        assert cls is not None, "pattern validator not registered"
        self.cls = cls

    def test_sentinel_values_skipped(self):
        v = self.cls(params={"pattern": r"^\d+ NCE$"})
        series = pd.Series(["not available", "not applicable", "30 NCE"])
        errors = v.validate(series, column_name="comment[collision energy]")
        assert len(errors) == 0

    def test_invalid_values_still_caught(self):
        v = self.cls(params={"pattern": r"^\d+ NCE$"})
        series = pd.Series(["invalid", "30 NCE"])
        errors = v.validate(series, column_name="comment[collision energy]")
        assert len(errors) == 1

    def test_empty_values_skipped(self):
        v = self.cls(params={"pattern": r"^\d+$"})
        series = pd.Series(["", "  ", "123"])
        errors = v.validate(series, column_name="test")
        assert len(errors) == 0
