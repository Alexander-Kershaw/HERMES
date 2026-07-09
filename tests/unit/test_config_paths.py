from hermes.config.paths import join_uri


def test_join_uri_local_path() -> None:
    assert join_uri("data/lakehouse/bronze", "orders") == "data/lakehouse/bronze/orders"


def test_join_uri_abfss_path() -> None:
    assert (
        join_uri(
            "abfss://bronze@sthermes.dfs.core.windows.net/hermes/bronze",
            "orders",
        )
        == "abfss://bronze@sthermes.dfs.core.windows.net/hermes/bronze/orders"
    )


def test_join_uri_handles_slashes() -> None:
    assert join_uri("data/lakehouse/bronze/", "/orders/") == "data/lakehouse/bronze/orders"