# FreshMarket Data Warehouse - dbt Project

## Overview
This dbt project transforms raw operational data from FreshMarket into a structured data warehouse following the medallion architecture (Bronze → Silver → Gold).

## Project Structure

```
dbt/
├── models/
│   ├── staging/          # Silver layer - cleaned and standardized data
│   ├── warehouse/        # Gold layer - business logic and relationships
│   │   ├── dimensions/   # Dimension tables (SCD Type 2)
│   │   └── facts/        # Fact tables (transactional data)
│   └── marts/            # Business-specific analytical views
│       ├── sales/
│       ├── inventory/
│       ├── customer/
│       └── marketing/
├── tests/               # Data quality tests
├── macros/              # Reusable SQL functions
└── snapshots/           # Historical data snapshots
```

## Data Sources

### Raw Tables (BigQuery: `raw_data` schema)
- `sales_transactions` - Individual product sales
- `customers` - Customer master data
- `products` - Product catalog
- `inventory_levels` - Current stock levels
- `supply_orders` - Purchase orders from suppliers
- `web_events` - Website user interactions
- `marketing_campaigns` - Campaign metadata

## Models Documentation

### Staging Layer (`staging/`)
Clean and standardize raw data with basic transformations:
- **stg_sales_transactions** - Standardized transaction data with calculated fields
- **stg_customers** - Customer data with privacy masking and tenure calculations
- **stg_products** - Product catalog with pricing metrics
- **stg_inventory_levels** - Inventory with reorder alerts
- **stg_supply_orders** - Supply chain data with delivery tracking
- **stg_web_events** - Web analytics with session analysis
- **stg_marketing_campaigns** - Campaign data with performance periods

### Warehouse Layer (`warehouse/`)

#### Dimensions (`dimensions/`)
- **dim_customers** - Customer dimension with SCD Type 2
- **dim_products** - Product dimension with hierarchy and pricing
- **dim_stores** - Store locations and metadata
- **dim_date** - Date dimension for time-based analysis

#### Facts (`facts/`)
- **fct_sales** - Sales transactions with surrogate keys
- **fct_inventory** - Current inventory levels with reorder metrics
- **fct_web_events** - Web events for digital analytics

### Marts Layer (`marts/`)
Business-specific analytical models:
- **sales/daily_sales_summary** - Daily sales performance by store

## Setup Instructions

### Prerequisites
- dbt installed (`pip install dbt-bigquery`)
- BigQuery access configured
- Service account key or OAuth setup

### Installation
1. Clone the repository
2. Install dependencies:
   ```bash
   dbt deps
   ```
3. Configure your `profiles.yml` with BigQuery credentials
4. Test connection:
   ```bash
   dbt debug
   ```

### Running the Project
```bash
# Run all models
dbt run

# Run specific layers
dbt run --models staging
dbt run --models warehouse  
dbt run --models marts

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

## Configuration

### Variables (`dbt_project.yml`)
- `start_date`: '2024-01-01' - Start date for incremental loads
- `end_date`: Current date - End date for data processing
- `high_value_customer_threshold`: 10000 THB - Customer segmentation threshold
- `seasonal_categories`: ['Fresh Produce', 'Beverages'] - Product seasonality

### Materializations
- **Staging**: Views (fast, minimal storage)
- **Warehouse**: Tables (optimized for joins)
- **Facts**: Partitioned tables (by transaction_date)
- **Marts**: Tables (pre-aggregated for performance)

## Data Quality & Testing

### Source Tests
- Unique keys on primary identifiers
- Not null constraints on critical fields
- Referential integrity between related tables

### Model Tests
- Data freshness checks
- Value range validations
- Business logic assertions

## Development Guidelines

### Naming Conventions
- **Staging models**: `stg_<source_table>`
- **Dimension tables**: `dim_<entity>`
- **Fact tables**: `fct_<process>`
- **Mart models**: `<business_area>_<description>`

### SQL Style
- Use meaningful CTEs for complex transformations
- Apply consistent indentation and formatting
- Include descriptive comments for business logic
- Use `{{ ref() }}` for model dependencies
- Use `{{ source() }}` for raw data references

## Deployment

### Environment Structure
- **dev**: Development schema for testing
- **prod**: Production schema for live reporting

### CI/CD Pipeline
Models are automatically deployed via GitHub Actions when changes are merged to main branch.

## Monitoring & Maintenance

### Key Metrics to Monitor
- Data freshness (last updated timestamps)
- Row count changes between runs
- Test failure alerts
- Model run times and failures

### Scheduled Runs
- **Staging models**: Every 4 hours
- **Warehouse models**: Every 6 hours  
- **Mart models**: Daily at 2 AM

## Documentation

Generate and view model documentation:
```bash
dbt docs generate
dbt docs serve --port 8001
```

## Troubleshooting

### Common Issues
- **dbt_utils errors**: Run `dbt deps` to install packages
- **Partition errors**: Ensure date fields exist in source data
- **Permission errors**: Verify BigQuery IAM roles and dataset access

### Support
For questions or issues, check the dbt logs in `logs/` directory or contact the data engineering team.