"""Constants for OpenMS conversion including label plexes and enzyme mappings."""

# TMT plex definitions mapping label names to their numeric identifiers
TMT_PLEXES = {
    "tmt18plex": {
        "TMT126": 1,
        "TMT127N": 2,
        "TMT127C": 3,
        "TMT128N": 4,
        "TMT128C": 5,
        "TMT129N": 6,
        "TMT129C": 7,
        "TMT130N": 8,
        "TMT130C": 9,
        "TMT131N": 10,
        "TMT131C": 11,
        "TMT132N": 12,
        "TMT132C": 13,
        "TMT133N": 14,
        "TMT133C": 15,
        "TMT134N": 16,
        "TMT134C": 17,
        "TMT135N": 18,
    },
    "tmt16plex": {
        "TMT126": 1,
        "TMT127N": 2,
        "TMT127C": 3,
        "TMT128N": 4,
        "TMT128C": 5,
        "TMT129N": 6,
        "TMT129C": 7,
        "TMT130N": 8,
        "TMT130C": 9,
        "TMT131N": 10,
        "TMT131C": 11,
        "TMT132N": 12,
        "TMT132C": 13,
        "TMT133N": 14,
        "TMT133C": 15,
        "TMT134N": 16,
    },
    "tmt11plex": {
        "TMT126": 1,
        "TMT127N": 2,
        "TMT127C": 3,
        "TMT128N": 4,
        "TMT128C": 5,
        "TMT129N": 6,
        "TMT129C": 7,
        "TMT130N": 8,
        "TMT130C": 9,
        "TMT131N": 10,
        "TMT131C": 11,
    },
    "tmt10plex": {
        "TMT126": 1,
        "TMT127N": 2,
        "TMT127C": 3,
        "TMT128N": 4,
        "TMT128C": 5,
        "TMT129N": 6,
        "TMT129C": 7,
        "TMT130N": 8,
        "TMT130C": 9,
        "TMT131": 10,
    },
    "tmt6plex": {
        "TMT126": 1,
        "TMT127": 2,
        "TMT128": 3,
        "TMT129": 4,
        "TMT130": 5,
        "TMT131": 6,
    },
}

# iTRAQ plex definitions
ITRAQ_4PLEX = {"itraq114": 1, "itraq115": 2, "itraq116": 3, "itraq117": 4}

ITRAQ_8PLEX = {
    "itraq113": 1,
    "itraq114": 2,
    "itraq115": 3,
    "itraq116": 4,
    "itraq117": 5,
    "itraq118": 6,
    "itraq119": 7,
    "itraq121": 8,
}

# SILAC label definitions
SILAC_3PLEX = {"silac light": 1, "silac medium": 2, "silac heavy": 3}
SILAC_2PLEX = {"silac light": 1, "silac heavy": 2}

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
}

# Default iTRAQ modifications when not specified in SDRF
ITRAQ_DEFAULT_MODS = {
    "itraq4plex": ["iTRAQ4plex (K)", "iTRAQ4plex (N-term)"],
    "itraq8plex": ["iTRAQ8plex (K)", "iTRAQ8plex (N-term)"],
}
