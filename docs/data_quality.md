# HERMES Data Quality

## Overview

I have used YAML data contracts and PySpark validation checks to verify the quality of silver tables.

The objective is to make data quality explicit, testable, and auditable. I do plan to potentially use great expectations down the line, but yaml probably shows more understanding of data contracts, while great expectations may obfuscate data contract design competency underneath the quite sophisticated great expectations framework.

## Why validate Silver

Bronze preserves source data. Silver is intended to become trusted data. Therefore, the silver layer warrants much more attention to data quality.

Before downstream gold modelling, silver tables need checks for:

- required fields
- primary key uniqueness
- valid ID patterns
- sensible numeric ranges
- controlled domain values
- basic business rules

To name a few.

## Contract location

Silver contracts are stored in:

```text
contracts/silver/
```

Each contract defines:

- the table name
- the primary key of the table
- the various column rules that dictate the data quality constraints

### Supported Validation Rules

The current data quality rules enforced are:

- `not_null`, fields should not contain nulls
- `unique`, uniqueness is enforced (especially for primary keys)
- `min_value`, there is a lower bound on numeric fields
- `accepted_values`, only particular accepted values are valid for a data field
- `regex`, records must conform to particular regex patterns relevant for certain fields

## Contract Validation Output

The results of the contract validation are written to:

```text
data/audit/silver_validation_report.csv
```

This validation audit report includes:

- the table name
- the column names
- the data contract rules
- validation pass / fail status
- failed row count
- total row count
- specific contract rule details

## Current Limitations

As of now, the current validation framework is relatively simple, and does not include:

- foreign key relational checks
- data freshness checks
- quarantine at the row level
- contract violation severity levels
- thresholds differentiating between complete fail or just warning
- schema drift detection

However, these will be added incrementally as development progresses.

## Custom Validation Frameworks VS External Validation Tools

External tools such as great expectations and soda are known to provide very rich validation features.

However, I am prioritising custom validation using YAML contracts and PySpark validation first for the following resons:

- It is easier to understand, great expectations in particular can be quite in-depth
- easier testing
- easier to comfortably explain 
- still directly alighned with the current project architecture and scope
- sufficient for initial development

## Relationship validation

I also added validation for table relationships in the silver layer.

Relationship contracts are stored in:

```text
contracts/silver/table_relationships/table_relationships.yml
```

Current relationship checks I've implemented include:

- `orders.customer_id` must exist in `customers.customer_id`
- `order_items.order_id` must exist in `orders.order_id`
- `order_items.product_id` must exist in `products.product_id`
- `inventory_snapshots.store_id` must exist in `stores.store_id`
- `inventory_snapshots.product_id` must exist in `products.product_id`
- `promotions.product_id` must exist in `products.product_id`

Relationship validation uses Spark left anti joins to identify orphan keys.

The validation script also writes a table relationship report to:

```text
data/audit/silver_relationship_validation_report.csv
```

---

## Quarantine handling

I have implemented a local quarantine mechanism for failed silver validation records.

When a column level validation check fails, the offending records are written to:

```text
data/quarantine/silver/<table_name>/<rule_name>_<column_name>.parquet
```

For each quarantined record, there are additional quarantine metadata columns:

- `_quarantine_table_name`
- `_quarantine_column_name`
- `_quarantine_rule_name`
- `_quarantine_failed_reason`
- `_quarantine_created_at`
- `_quarantine_created_date`


---

## Contract calibration

Data contracts require calibration against the actual source and Silver schemas.

During implementation, naming mismatches caused excessive quarantine output where entire tables were being quarantined. These were corrected before extending the validation framework.

This was an important catch and is an example of not every validation failure indicates bad source data. Some failures indicate that the contract definition, generated data, or transformation schema is misaligned.

---


