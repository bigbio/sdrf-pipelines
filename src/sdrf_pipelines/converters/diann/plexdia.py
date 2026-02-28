"""plexDIA label detection and DIA-NN channel flag generation."""

from sdrf_pipelines.converters.diann.constants import PLEXDIA_REGISTRY


def _format_mass(m: float) -> str:
    """Format a mass value: use integer representation for whole numbers."""
    if m == int(m):
        return str(int(m))
    return str(m)


def detect_plexdia_type(label_set: set[str]) -> dict | None:
    """Detect plexDIA labeling type from a set of SDRF labels.

    Args:
        label_set: Set of label strings from comment[label] column

    Returns:
        Dict with keys: type, plex, channels_used. None if label-free.

    Raises:
        ValueError: If labels are not recognized
    """
    normalized = {l.strip() for l in label_set}

    if all(l.lower() == "label free sample" for l in normalized):
        return None

    for plex_type, registry in PLEXDIA_REGISTRY.items():
        channels = registry["channels"]
        channel_names = set(channels.keys())

        if normalized.issubset(channel_names):
            for plex_name, plex_channels in registry["plexes"].items():
                if normalized == set(plex_channels):
                    return {
                        "type": plex_type,
                        "plex": plex_name,
                        "channels_used": sorted(normalized, key=lambda x: channels[x]["masses"][0]),
                    }

            for plex_name, plex_channels in sorted(registry["plexes"].items(), key=lambda x: len(x[1])):
                if normalized.issubset(set(plex_channels)):
                    return {
                        "type": plex_type,
                        "plex": plex_name,
                        "channels_used": sorted(
                            list(plex_channels),
                            key=lambda x: channels[x]["masses"][0],
                        ),
                    }

    raise ValueError(f"Unsupported label(s): {normalized}. Expected label free, mTRAQ, SILAC, or Dimethyl channels.")


def build_channels_flag(plex_info: dict) -> str:
    """Build the DIA-NN --channels flag value.

    Args:
        plex_info: Dict from detect_plexdia_type()

    Returns:
        Channels string for --channels flag
        e.g., "mTRAQ,0,nK,0:0; mTRAQ,4,nK,4.0070994:4.0070994; ..."
    """
    registry = PLEXDIA_REGISTRY[plex_info["type"]]
    channels = registry["channels"]
    fixed_mod = registry["fixed_mod"]
    mod_name = fixed_mod["name"]

    parts = []
    for channel_label in plex_info["channels_used"]:
        ch = channels[channel_label]
        masses_str = ":".join(_format_mass(m) for m in ch["masses"])
        parts.append(f"{mod_name},{ch['channel_name']},{ch['sites']},{masses_str}")

    return "; ".join(parts)


def build_fixed_mod_flag(plex_info: dict) -> str:
    """Build the DIA-NN --fixed-mod flag value for plexDIA.

    Args:
        plex_info: Dict from detect_plexdia_type()

    Returns:
        Fixed mod string, e.g., "mTRAQ,140.0949630177,nK" or "SILAC,0.0,KR,label"
    """
    fixed_mod = PLEXDIA_REGISTRY[plex_info["type"]]["fixed_mod"]
    parts = [fixed_mod["name"], str(fixed_mod["mass"]), fixed_mod["sites"]]

    if fixed_mod["is_isotopic"]:
        parts.append("label")

    return ",".join(parts)
