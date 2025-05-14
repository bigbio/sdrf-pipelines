def tsv_line(*value_list: str) -> str:
    """Compose a tab separated value line ending with a newline.

    All arguments supplied will be joined by tabs and the line completed with a newline.
    """
    return "\t".join(value_list) + "\n"
