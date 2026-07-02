from hermes.data_generation.generate_batch_sources import (
    DataGenerationConfig,
    generate_customers,
    generate_orders_w_items,
    generate_product_inventory_snapshots,
    generate_product_promotions,
    generate_products,
    generate_stores,
)


def test_generate_customers_count() -> None:
    config = DataGenerationConfig(num_customers=10)

    customers = generate_customers(config)

    assert len(customers) == 10
    assert customers["customer_id"].is_unique
    assert customers["email"].is_unique
    assert customers["phone_number"].is_unique


def test_generate_products_count() -> None:
    config = DataGenerationConfig(num_products=20)

    products = generate_products(config)

    assert len(products) == 20
    assert products["product_id"].is_unique
    assert (products["unit_price"] > 0).all()
    assert (products["cost_price"] > 0).all()
    assert (products["cost_price"] <= products["unit_price"]).all()


def test_generate_stores_count() -> None:
    config = DataGenerationConfig(num_stores=5)

    stores = generate_stores(config)

    assert len(stores) == 5
    assert stores["store_id"].is_unique


def test_generate_orders_and_items_relationships() -> None:
    config = DataGenerationConfig(num_customers=20, num_stores=3, num_products=15, num_orders=50)

    customers = generate_customers(config)
    stores = generate_stores(config)
    products = generate_products(config)
    orders, order_items = generate_orders_w_items(config, customers, products, stores)

    assert len(orders) == 50
    assert orders["order_id"].is_unique
    assert order_items["order_item_id"].is_unique

    assert set(order_items["order_id"]).issubset(set(orders["order_id"]))
    assert set(orders["customer_id"]).issubset(set(customers["customer_id"]))
    assert set(order_items["product_id"]).issubset(set(products["product_id"]))

    assert (order_items["quantity"] > 0).all()
    assert (order_items["unit_price"] > 0).all()
    assert (order_items["net_amount"] >= 0).all()


def test_generate_inventory_snapshots_relationships() -> None:
    config = DataGenerationConfig(num_stores=4, num_products=20)

    stores = generate_stores(config)
    products = generate_products(config)
    inventory = generate_product_inventory_snapshots(config, products, stores)

    assert not inventory.empty
    assert inventory["inventory_snapshot_id"].is_unique
    assert set(inventory["store_id"]).issubset(set(stores["store_id"]))
    assert set(inventory["product_id"]).issubset(set(products["product_id"]))
    assert (inventory["stock_on_hand"] >= 0).all()


def test_generate_promotions_relationships() -> None:
    config = DataGenerationConfig(num_products=20)

    products = generate_products(config)
    promotions = generate_product_promotions(config, products)

    assert len(promotions) == 40
    assert promotions["promotion_id"].is_unique
    assert set(promotions["product_id"]).issubset(set(products["product_id"]))
    assert (promotions["discount_pct"] > 0).all()
