from sdrf_pipelines.openms.unimod import UnimodDatabase


def test_search_mods_by_accession():
    unimod = UnimodDatabase()
    ptm = unimod.get_by_accession("UNIMOD:21")
    assert ptm.get_name() == "Phospho"


def test_search_mods_by_keyword():
    unimod = UnimodDatabase()
    ptms = [ptm.to_str() for ptm in unimod.search_mods_by_keyword("Phospho")]
    assert "UNIMOD:21 Phospho 79.966331 H O(3) P" in ptms
    assert len(ptms) == 27
