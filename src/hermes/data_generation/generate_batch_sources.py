import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from faker import Faker
from loguru import logger

from hermes.config.hermes_setting import hermes_settings
from hermes.data_generation.constants import CHANNELS, ORDER_STATUSES, RETAIL_CATEGORIES, UK_CITIES
from hermes.utils.logging import config_logging
from hermes.utils.paths import raw_sample_data_dir


@dataclass
class DataGenerationConfig:

    num_customers: int = 1_000
    num_products: int = 250
    num_stores: int = 25
    num_orders: int = 5_000
    start_date: str = "2023-01-01"
    end_date: str = "2023-01-31"
    generation_seed: int = hermes_settings.random_seed


def _fake(generation_seed: int) -> Faker:

    fake = Faker("en_GB")
    Faker.seed(generation_seed)
    random.seed(generation_seed)

    return fake


def generate_customers(config: DataGenerationConfig) -> pd.DataFrame:

    fake = _fake(config.generation_seed)

    customer_records = []

    for customer_num in range(1, config.num_customers + 1):
        city, region = random.choice(UK_CITIES)
        creation_datetime = fake.date_time_between(start_date="-3y", end_date="-30d")

        customer_records.append(
            {
                "customer_id": f"CUSTOMER-{customer_num:06d}",
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "email": fake.email(),
                "phone_number": fake.phone_number(),
                "city": city,
                "region": region,
                "postal_code": fake.postcode(),
                "loyalty_tier": random.choices(["Bronze", "Silver", "Gold", "Platinum"], 
                                               weights=[0.5, 0.3, 0.15, 0.05], k=1)[0],
                "creation_datetime": creation_datetime,
                "is_active": random.choices([True, False], weights=[0.9, 0.1], k=1)[0]
            }
        )

    return pd.DataFrame(customer_records)


def generate_stores(config: DataGenerationConfig) -> pd.DataFrame:

    fake = _fake(config.generation_seed + 1) # distinct seed for stores 

    store_records = []

    for store_num in range(1, config.num_stores + 1):
        city, region = random.choice(UK_CITIES)

        store_records.append(
            {
                "store_id": f"STORE-{store_num:04d}",
                "store_name": f"HERMES {city} {store_num}",
                "city": city,
                "region": region,
                "postal_code": fake.postcode(),
                "store_format": random.choices(["convenience", "standard", "flagship", "warehouse"],
                                                weights=[0.4, 0.4, 0.15, 0.05], k=1)[0],
                "opening_date": fake.date_between(start_date="-15y", end_date="-90d"),
                "is_active": random.choices([True, False], weights=[0.95, 0.05], k=1)[0]
            }

        )

    return pd.DataFrame(store_records)
    


def generate_products(config: DataGenerationConfig) -> pd.DataFrame:

    fake = _fake(config.generation_seed + 2) # Another distinct seed

    category_pairings = [
        (category, subcategory)
        for category, subcategories in RETAIL_CATEGORIES.items()
        for subcategory in subcategories
    ]

    product_records = []

    for product_num in range(1, config.num_products + 1):
        category, subcategory = random.choice(category_pairings)

        base_price = round(random.uniform(5.0, 500.0), 2)
        cost_price = round(base_price * random.uniform(0.5, 0.9), 2)

        product_records.append(
            {
                "product_id": f"PRODUCT-{product_num:06d}",
                "sku": f"SKU-{fake.unique.bothify(text='???-#####').upper()}",
                "product_name": f"{fake.word().title()} {subcategory.title()} Item",
                "category": category, 
                "subcategory": subcategory,
                "brand": fake.company(),
                "unit_price": base_price,
                "cost_price": cost_price,
                "is_active": random.choices([True, False], weights=[0.95, 0.05], k=1)[0],
                "creation_datetime": fake.date_time_between(start_date="-5y", end_date="-30d")

            }

        )

    return pd.DataFrame(product_records)


def _random_datetime_between(start_time: str, end_time: str) -> datetime:
    seconds = int((end_time - start_time).total_seconds())
    return start_time + timedelta(seconds=random.randint(0, seconds))


def generate_orders_w_items(
    config: DataGenerationConfig,
    customers_df: pd.DataFrame,
    products_df: pd.DataFrame,
    stores_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:

    fake = _fake(config.generation_seed + 3) 

    start_datetime = datetime.fromisoformat(config.start_date)
    end_datetime = datetime.fromisoformat(config.end_date) + timedelta(days=1) - timedelta(seconds=1)

    customer_ids = customers_df["customer_id"].tolist()
    store_ids = stores_df["store_id"].tolist()
    product_records = products_df[
        ["product_id", "unit_price", "cost_price", "category"]].to_dict(orient="records")

    order_rows = []
    item_rows = []

    for order_num in range(1, config.num_orders + 1):
        order_id = f"ORDER-{order_num:08d}"
        customer_id = random.choice(customer_ids)
        channel = random.choices(CHANNELS, weights=[0.6, 0.3, 0.1], k=1)[0]
        order_timestamp = _random_datetime_between(start_datetime, end_datetime)
        store_id  = random.choice(store_ids) if channel == "store" else None

        order_status = random.choices(ORDER_STATUSES, 
                                      weights=[0.72, 0.20, 0.05, 0.03], k=1)[0]
        item_count = random.choices([1, 2, 3, 4, 5], 
                                    weights=[0.5, 0.3, 0.1, 0.07, 0.03], k=1)[0]

        order_gross_amount = 0.0
        order_discount_amount = 0.0
        order_tax_amount = 0.0

        for i in range(1, item_count + 1):
            product = random.choice(product_records)
            quantity = random.choices([1, 2, 3 ,4],
                                       weights=[0.7, 0.2, 0.08, 0.02], k=1)[0]

            unit_price = float(product["unit_price"])

            product_discount_rate = random.choices([0.0, 0.05, 0.10, 0.15],
                                                   weights=[0.7, 0.2, 0.08, 0.02], k=1)[0]

            gross_amount = round(unit_price * quantity, 2)
            discount_amount = round(gross_amount * product_discount_rate, 2)
            net_amount = round(gross_amount - discount_amount, 2)
            tax_amount = round(net_amount * 0.2, 2) # tax assumed to be 20% VAT 

            order_gross_amount += gross_amount
            order_discount_amount += discount_amount
            order_tax_amount += tax_amount

            item_rows.append(
                {
                    "order_item_id": f"{order_id}-{i:03d}",
                    "order_id": order_id,
                    "line_number": i,
                    "product_id": product["product_id"],
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "gross_amount": gross_amount,
                    "discount_amount": discount_amount,
                    "tax_amount": tax_amount,
                    "net_amount": net_amount
                }

            )

        order_net_amount = round(order_gross_amount - order_discount_amount, 2)

        order_rows.append(
            {
                "order_id": order_id,
                "customer_id": customer_id,
                "store_id": store_id,
                "channel": channel,
                "order_timestamp": order_timestamp,
                "status": order_status,
                "currency": "GBP",
                "gross_amount": round(order_gross_amount, 2),
                "discount_amount": round(order_discount_amount, 2),
                "tax_amount": round(order_tax_amount, 2),
                "net_amount": order_net_amount,
                "payment_method": random.choice(["credit_card", "debit_card", "paypal",
                                                    "gift_card", "apple_pay", "google_pay"]), 
                "created_by_system": random.choice(["ecommerce_platform", "pos_system", "mobile_app"]),
                "source_file_date": order_timestamp.date(),
                "customer_note": fake.sentence(nb_words=10) if random.random() < 0.1 else None
            }

        )

    return pd.DataFrame(order_rows), pd.DataFrame(item_rows)



def generate_product_inventory_snapshots(
    config: DataGenerationConfig,
    products_df: pd.DataFrame,
    stores_df: pd.DataFrame
) -> pd.DataFrame:


    store_ids = stores_df["store_id"].tolist()
    product_ids = products_df["product_id"].tolist()
    inv_snapshot_date = datetime.fromisoformat(config.end_date).date()

    inventory_snapshots = []

    for store_id in store_ids:

        product_sample = random.sample(product_ids, k=min(len(product_ids), 120))

        for product_id in product_sample:
            stock_on_hand = max(0, int(random.gauss(mu=50, sigma=20)))
            reorder_point = random.choice([10, 15, 20, 25, 30])
            reorder_quantity = random.choice([50, 75, 100, 150])

            inventory_snapshots.append(
                {
                    "inventory_snapshot_id": f"INV-{store_id}-{product_id}-{inv_snapshot_date}",
                    "store_id": store_id,
                    "product_id": product_id,
                    "snapshot_date": inv_snapshot_date,
                    "stock_on_hand": stock_on_hand,
                    "reorder_point": reorder_point,
                    "reorder_quantity": reorder_quantity,
                    "is_stockout": stock_on_hand == 0,
                    "is_below_reorder_point": stock_on_hand < reorder_point,
                }

            )

    return pd.DataFrame(inventory_snapshots)



def generate_product_promotions(config: DataGenerationConfig, 
    products_df: pd.DataFrame) -> pd.DataFrame:

    fake = _fake(config.generation_seed + 5) 

    product_ids = products_df["product_id"].tolist()
    start_date = datetime.fromisoformat(config.start_date).date()
    end_date = datetime.fromisoformat(config.end_date).date()
    num_promotions = 40

    promotion_records = []

    for promotion_num in range(1, num_promotions + 1):
        
        promotion_start = fake.date_between(start_date=start_date, end_date=end_date)
        promotion_end = promotion_start + timedelta(days=random.randint(3, 14))

        promotion_records.append(
            {
                "promotion_id": f"PROMO-{promotion_num:04d}",
                "promotion_name": f"{fake.catch_phrase()} Campaign",
                "product_id": random.choice(product_ids),
                "discount_pct": random.choice([5, 10, 15, 20, 25, 30]),
                "start_date": promotion_start,
                "end_date": promotion_end,
                "channel": random.choice(["online", "store", "mobile_app"]),
                "is_active": promotion_start <= end_date and promotion_end >= start_date
            }

        )

    return pd.DataFrame(promotion_records)



def write_dataframes_to_csv(df: pd.DataFrame,
    output_dir: Path, filename: str) -> Path:

    output_path = output_dir / f"{filename}.csv"
    df.to_csv(output_path, index=False)
    logger.info(f"Wrote {len(df)} rows to {output_path}")
    return output_path


def generate_all_synth_retail_data(config: DataGenerationConfig) -> dict[str, pd.DataFrame]:

    output_dir = raw_sample_data_dir()

    logger.info(f"Generating synthetic retail data with config: {config}")
    logger.info(f"Output directory: {output_dir}")

    customers_df = generate_customers(config)
    stores_df = generate_stores(config)
    products_df = generate_products(config)
    orders_df, order_items_df = generate_orders_w_items(config, customers_df, products_df, stores_df)
    inventory_snapshots_df = generate_product_inventory_snapshots(config, products_df, stores_df)
    promotions_df = generate_product_promotions(config, products_df)

    all_synth_data = {
        "customers": customers_df,
        "stores": stores_df,
        "products": products_df,
        "orders": orders_df,
        "order_items": order_items_df,
        "inventory_snapshots": inventory_snapshots_df,
        "promotions": promotions_df
    }

    for name, df in all_synth_data.items():
        write_dataframes_to_csv(df, output_dir, name)

    return all_synth_data


def main() -> None:

    config_logging()

    config = DataGenerationConfig()
    generate_all_synth_retail_data(config)

if __name__ == "__main__":
    main()


