"""Constants for OpenMS conversion including label plexes and enzyme mappings."""

# The channel vocabulary (label <-> 1-based index per plex, plus synonyms) is
# shared across all converters and lives in
# sdrf_pipelines.converters.channel_map. Re-exported here so existing importers
# of openms.constants.CHANNEL_MAP keep working unchanged.
from sdrf_pipelines.converters.channel_map import CHANNEL_MAP, normalize_label  # noqa: F401

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
