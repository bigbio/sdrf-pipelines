from sdrf_pipelines.openms.unimod import UnimodDatabase


def test_search_mods_by_accession():
    unimod = UnimodDatabase()
    ptm = unimod.get_by_accession("UNIMOD:21")
    print(ptm.get_name())


def test_search_mods_by_keyword():
    unimod = UnimodDatabase()
    ptms = unimod.search_mods_by_keyword("Phospho")
    for ptm in ptms:
        print(ptm.to_str())


if __name__ == "__main__":
    test_search_mods_by_keyword()
