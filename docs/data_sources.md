# HERMES Data Sources

## Overview

For HERMES I elected to use synthetic retail data to simulate batch and real time source systems for an omnichannel retailer.

Synthetic data is used during early development to provide control over schemas, relationships, volumes, and data quality scenarios.

## Source systems

### Customer management system

Produces customer master data.

Output file:

```text
data/sample/raw/customers.csv
```

Main fields:

- `customer_id`
- `first_name`
- `last_name`
- `email`
- `phone_number`
- `city`
- `region`
- `postcode`
- `loyalty_tier`
- `created_at`
- `is_active`

### Store operations system

Produces store reference data.

Output file:

```text
data/samples/raw/stores.csv
```

Main fields:

- `store_id`
- `store_name`
- `city`
- `region`
- `postcode`
- `store_format`
- `opened_date`
- `is_active`

### Product information management system

Produces product catelogue data.

Output file:

```text
data/samples/raw/products.csv
```

Main fields:

- `product_id`
- `sku`
- `product_name`
- `category`
- `subcategory`
- `brand`
- `unit_price`
- `cost_price`
- `is_active`
- `created_at`

### Order management system

Produces order data.

Output file:

```text
data/samples/raw/orders.csv
```

Main fields:

- `order_id`
- `customer_id`
- `store_id`
- `channel`
- `order_status`
- `order_timestamp`
- `currency`
- `gross_amount`
- `discount_amount`
- `net_amount`
- `tax_amount`
- `payment_method`
- `created_by_system`
- `source_file_date`

### Order line system

Produces order item data.

Output file:

```text
data/sample/raw/order_items.csv
```

Main fields:

- `order_item_id`
- `order_id`
- `line_number`
- `product_id`
- `quantity`
- `unit_price`
- `gross_amount`
- `discount_amount`
- `net_amount`
- `tax_amount`

### Inventory management system

Produces product inventory snapshots at the store level.

Output file:

```text
data/sample/raw/inventory_snapshots.csv
```

Main fields:

- `inventory_snapshot_id`
- `store_id`
- `product_id`
- `snapshot_date`
- `stock_on_hand`
- `reorder_point`
- `reorder_quantity`
- `is_stockout`
- `is_below_reorder_point`

### Promotion management system

Produces product promotions data.

Output file:

```text
data/samples/raw/promotions.csv
```

Main fields:

- `promotion_id`
- `promotion_name`
- `product_id`
- `discount_pct`
- `start_date`
- `end_date`
- `channel`
- `is_active`

## Design Notes

The synthetic data being generated is intentionally simple for the sake of relational coherence.

Too complex retail data too early would likely cause a massive headache, and my focus is on ensuring everything works as intended as a baseline before expanding to greated complexity.

The initial version focuses on generating clean batch data. Down the line the following will be introduced:

- schema validation
- malformed records
- late arrival of events
- duplicate events
- streaming web and basket events
- bad record quarantine
- incremental processing

## Limitations and Advantages of Synthetic Data Generation

The use of synthetic data is beneficial for establishing a controlled schema, repeatable generation, easy scaling, ensuring predictable relationships. This foundation supports the testing of data quality rules and establishing a solid base of development.

However, the key limitation is in authenticity (as of now), as realistic retail data is likely to be much more messy, probably wont contain organic behavioural patterns. This does warrant the deliberate injection of data quality issues later, which can be tedious.

### On Public Datasets

It is entirely possible for me to introduce public datasets later to supplement operational realism, but my priority is to demonstrate key data engineering principles and produce an initial version of the data lakehouse.
