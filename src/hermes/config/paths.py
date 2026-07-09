"""

Local paths and ADLS Gen2 paths are pretty different from eachother, so I dont
want to just wrap everything wuth Path(...)

This helper function essentially joins the base data paths with specific file names
to reference different data. For example:

    - data/lakehouse/bronze + orders
    - abfss://bronze@account.dfs.core.windows.net/hermes/bronze + orders

Where + orders (or customers or orders, etc...) is the extra part

Returns a string because Spark accepts both local paths and abfss:// paths as strings.

"""

def join_uri(base_path: str, *parts: str) -> str:

    cleaned_base: str = base_path.rstrip("/")
    cleaned_parts: str = [part.strip("/") for part in parts if part]

    if not cleaned_parts:
        return cleaned_base
    
    return "/".join([cleaned_base, *cleaned_parts])