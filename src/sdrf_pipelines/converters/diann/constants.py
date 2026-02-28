"""Constants for DIA-NN conversion including plexDIA channel definitions and enzyme mappings."""

# DIA-NN enzyme cut rules
# Format: cleavage_rule where K* = cleave after K, !*P = don't cleave before P
ENZYME_SPECIFICITY = {
    "Trypsin": "K*,R*,!*P",
    "Trypsin/P": "K*,R*",
    "Arg-C": "R*,!*P",
    "Asp-N": "*B,*D",
    "Chymotrypsin": "F*,W*,Y*,L*,!*P",
    "Chymotrypsin/P": "F*,W*,Y*,L*",
    "Lys-C": "K*,!*P",
    "Lys-C/P": "K*",
    "Lys-N": "*K",
    "Glutamyl endopeptidase": "E*,!*P",
}

# Enzyme name normalization (SDRF names -> canonical names)
ENZYME_NAME_MAPPINGS = {
    "trypsin": "Trypsin",
    "trypsin/p": "Trypsin/P",
    "arg-c": "Arg-C",
    "asp-n": "Asp-N",
    "chymotrypsin": "Chymotrypsin",
    "chymotrypsin/p": "Chymotrypsin/P",
    "lys-c": "Lys-C",
    "lys-c/p": "Lys-C/P",
    "lys-n": "Lys-N",
    "glutamyl endopeptidase": "Glutamyl endopeptidase",
}

# --- plexDIA Channel Definitions ---
# Each channel: {"channel_name": str, "sites": str, "masses": list[float]}
# masses list has one delta per site character (e.g., "nK" -> [n_delta, K_delta])
# Channels MUST be ordered by ascending mass.

MTRAQ_CHANNELS = {
    "MTRAQ0": {"channel_name": "0", "sites": "nK", "masses": [0.0, 0.0]},
    "MTRAQ4": {"channel_name": "4", "sites": "nK", "masses": [4.0070994, 4.0070994]},
    "MTRAQ8": {"channel_name": "8", "sites": "nK", "masses": [8.0141988132, 8.0141988132]},
}
MTRAQ_FIXED_MOD = {"name": "mTRAQ", "mass": 140.0949630177, "sites": "nK", "is_isotopic": False}

MTRAQ_PLEXES = {
    "mtraq3plex": ["MTRAQ0", "MTRAQ4", "MTRAQ8"],
}

DIMETHYL_CHANNELS = {
    "DIMETHYL0": {"channel_name": "0", "sites": "nK", "masses": [0.0, 0.0]},
    "DIMETHYL2": {"channel_name": "2", "sites": "nK", "masses": [2.0126, 2.0126]},
    "DIMETHYL4": {"channel_name": "4", "sites": "nK", "masses": [4.0251, 4.0251]},
    "DIMETHYL6": {"channel_name": "6", "sites": "nK", "masses": [6.0377, 6.0377]},
    "DIMETHYL8": {"channel_name": "8", "sites": "nK", "masses": [8.0444, 8.0444]},
}
DIMETHYL_FIXED_MOD = {"name": "Dimethyl", "mass": 28.0313, "sites": "nK", "is_isotopic": False}

DIMETHYL_PLEXES = {
    "dimethyl2plex": ["DIMETHYL0", "DIMETHYL2"],
    "dimethyl3plex": ["DIMETHYL0", "DIMETHYL2", "DIMETHYL4"],
    "dimethyl5plex": ["DIMETHYL0", "DIMETHYL2", "DIMETHYL4", "DIMETHYL6", "DIMETHYL8"],
}

# SILAC uses existing SDRF terms: "SILAC light", "SILAC heavy", "SILAC medium"
SILAC_CHANNELS = {
    "SILAC light": {"channel_name": "L", "sites": "KR", "masses": [0.0, 0.0]},
    "SILAC medium": {"channel_name": "M", "sites": "KR", "masses": [6.020129, 6.020129]},
    "SILAC heavy": {"channel_name": "H", "sites": "KR", "masses": [8.014199, 10.008269]},
}
SILAC_FIXED_MOD = {"name": "SILAC", "mass": 0.0, "sites": "KR", "is_isotopic": True}

SILAC_PLEXES = {
    "silac2plex": ["SILAC light", "SILAC heavy"],
    "silac3plex": ["SILAC light", "SILAC medium", "SILAC heavy"],
}

# Map of all plexDIA types to their channel dicts and fixed mod info
PLEXDIA_REGISTRY = {
    "mtraq": {"channels": MTRAQ_CHANNELS, "fixed_mod": MTRAQ_FIXED_MOD, "plexes": MTRAQ_PLEXES},
    "dimethyl": {"channels": DIMETHYL_CHANNELS, "fixed_mod": DIMETHYL_FIXED_MOD, "plexes": DIMETHYL_PLEXES},
    "silac": {"channels": SILAC_CHANNELS, "fixed_mod": SILAC_FIXED_MOD, "plexes": SILAC_PLEXES},
}
