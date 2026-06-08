"""Tests for DIA-NN modification conversion."""

import pytest

from sdrf_pipelines.converters.diann.modifications import DiannModificationConverter


class TestDiannModificationConverter:
    def test_fixed_mod_carbamidomethyl(self):
        converter = DiannModificationConverter()
        result = converter.convert_modification("NT=Carbamidomethyl;TA=C;MT=fixed;AC=UNIMOD:4", is_fixed=True)
        assert result == "Carbamidomethyl,57.021464,C"

    def test_var_mod_oxidation(self):
        converter = DiannModificationConverter()
        result = converter.convert_modification("NT=Oxidation;MT=variable;TA=M;AC=UNIMOD:35", is_fixed=False)
        assert result == "Oxidation,15.994915,M"

    def test_nterm_mod(self):
        converter = DiannModificationConverter()
        result = converter.convert_modification(
            "NT=Acetyl;MT=variable;TA=N-term;AC=UNIMOD:1;PP=Any N-term", is_fixed=False
        )
        assert result == "Acetyl,42.010565,n"

    def test_protein_nterm_mod(self):
        converter = DiannModificationConverter()
        result = converter.convert_modification("NT=Acetyl;MT=variable;PP=Protein N-term;AC=UNIMOD:1", is_fixed=False)
        assert result == "Acetyl,42.010565,*n"

    def test_label_mod_tmt_gets_label_suffix(self):
        converter = DiannModificationConverter()
        result = converter.convert_modification("NT=TMT6plex;TA=K;MT=fixed;AC=UNIMOD:737", is_fixed=True)
        assert result == "TMT6plex,229.162932,K,label"

    def test_label_mod_itraq_gets_label_suffix(self):
        converter = DiannModificationConverter()
        result = converter.convert_modification("NT=iTRAQ4plex;TA=K;MT=fixed;AC=UNIMOD:214", is_fixed=True)
        assert result == "iTRAQ4plex,144.102063,K,label"

    def test_unknown_mod_raises(self):
        converter = DiannModificationConverter()
        with pytest.raises(ValueError, match="only UNIMOD"):
            converter.convert_modification("NT=FakeMod;TA=X;MT=fixed;AC=UNIMOD:99999", is_fixed=True)

    def test_convert_all_modifications(self):
        converter = DiannModificationConverter()
        fixed_mods = ["NT=Carbamidomethyl;TA=C;MT=fixed;AC=UNIMOD:4"]
        var_mods = ["NT=Oxidation;MT=variable;TA=M;AC=UNIMOD:35"]
        fixed_result, var_result = converter.convert_all_modifications(fixed_mods, var_mods)
        assert len(fixed_result) == 1
        assert fixed_result[0] == "Carbamidomethyl,57.021464,C"
        assert len(var_result) == 1
        assert var_result[0] == "Oxidation,15.994915,M"

    # --- multi-residue sites in a single SDRF cell (TA=S,T,Y) -----------------
    # DIA-NN keeps only the first residue of a comma-separated site list, so the
    # converter must concatenate the residues into one site string.

    def test_var_mod_multi_site_single_cell_phospho(self):
        converter = DiannModificationConverter()
        result = converter.convert_modification("NT=Phospho;MT=variable;TA=S,T,Y;AC=UNIMOD:21", is_fixed=False)
        assert result == "Phospho,79.966331,STY"

    def test_var_mod_multi_site_single_cell_oxidation_mp(self):
        converter = DiannModificationConverter()
        result = converter.convert_modification("NT=Oxidation;MT=variable;TA=M,P;AC=UNIMOD:35", is_fixed=False)
        assert result == "Oxidation,15.994915,MP"

    def test_var_mod_multi_site_with_spaces(self):
        converter = DiannModificationConverter()
        result = converter.convert_modification("NT=Phospho;MT=variable;TA=S, T, Y;AC=UNIMOD:21", is_fixed=False)
        assert result == "Phospho,79.966331,STY"

    # --- same modification declared across several SDRF cells ------------------
    # DIA-NN de-duplicates entries by name and would keep only the first cell,
    # so same-(name, mass) modifications must be merged into one entry.

    def test_merge_oxidation_m_and_p_across_cells(self):
        converter = DiannModificationConverter()
        var_mods = [
            "NT=Oxidation;MT=variable;TA=M;AC=UNIMOD:35",
            "NT=Oxidation;MT=variable;TA=P;AC=UNIMOD:35",
        ]
        _, var_result = converter.convert_all_modifications([], var_mods)
        assert var_result == ["Oxidation,15.994915,MP"]

    def test_merge_phospho_across_three_cells(self):
        converter = DiannModificationConverter()
        var_mods = [
            "NT=Phospho;MT=variable;TA=S;AC=UNIMOD:21",
            "NT=Phospho;MT=variable;TA=T;AC=UNIMOD:21",
            "NT=Phospho;MT=variable;TA=Y;AC=UNIMOD:21",
        ]
        _, var_result = converter.convert_all_modifications([], var_mods)
        assert var_result == ["Phospho,79.966331,STY"]

    def test_merge_preserves_distinct_modifications(self):
        converter = DiannModificationConverter()
        var_mods = [
            "NT=Oxidation;MT=variable;TA=M;AC=UNIMOD:35",
            "NT=Oxidation;MT=variable;TA=P;AC=UNIMOD:35",
            "NT=Acetyl;MT=variable;PP=Protein N-term;AC=UNIMOD:1",
        ]
        _, var_result = converter.convert_all_modifications([], var_mods)
        assert var_result == ["Oxidation,15.994915,MP", "Acetyl,42.010565,*n"]

    def test_merge_deduplicates_repeated_site(self):
        converter = DiannModificationConverter()
        var_mods = [
            "NT=Oxidation;MT=variable;TA=M;AC=UNIMOD:35",
            "NT=Oxidation;MT=variable;TA=M;AC=UNIMOD:35",
        ]
        _, var_result = converter.convert_all_modifications([], var_mods)
        assert var_result == ["Oxidation,15.994915,M"]
