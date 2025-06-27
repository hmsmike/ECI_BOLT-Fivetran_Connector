# Bolt ECI Fivetran Connector

A custom Fivetran connector for integrating with Bolt by ECI's REST API to synchronize construction management data into your data warehouse.

## Overview

This connector provides a robust integration between Bolt ECI's construction management platform and Fivetran, enabling automated data synchronization of jobs, work orders, customers, employees, and other business entities. The connector handles both standard table endpoints and event-based endpoints with different pagination strategies.

## Features

- **Comprehensive Data Coverage**: Syncs 20+ tables including jobs, work orders, customers, employees, invoices, and more
- **Dual Pagination Support**: Handles both standard `next_batch` pagination and event-based `event_token` pagination
- **Robust Error Handling**: Graceful error recovery with detailed logging and retry logic
- **Rate Limiting**: Built-in rate limiting to respect API limits (1000 calls/hour)
- **Data Cleaning**: Automatic data normalization and cleaning for warehouse compatibility
- **State Management**: Persistent sync state tracking for incremental updates

## Architecture

### Supported Tables

#### Standard Tables (next_batch pagination)
- `jobs` - Job records and metadata
- `communities` - Community/development information
- `customers` - Customer data with flattened address/contact fields
- `employees` - Employee records with flattened address fields
- `crews` - Crew assignments and configurations
- `work_orders` - Work order records
- `work_order_types` - Work order type definitions
- `floorplans` - Floor plan specifications
- `customer_pricings` - Customer-specific pricing
- `contracts` - Contract information
- `invoices` - Invoice records (from `accounting_invoices` endpoint)
- `builder_orders` - Builder order records
- `types` - Type definitions
- `cities` - City information
- `offices` - Office locations
- `schedules` - Schedule records (from `work_orders` endpoint)
- `takeoff_types` - Takeoff type definitions
- `job_type_configuration` - Job type configurations

#### Event Tables (event_token pagination)
- `job_events` - Job change events (create, update, destroy)
- `work_order_events` - Work order change events
- `work_order_status_events` - Work order status change events

### Data Flow

```
Bolt ECI API → Fivetran Connector → Data Warehouse
     ↓              ↓                    ↓
  REST Calls → Data Processing → Structured Tables
```

## Installation & Setup

### Prerequisites

- Python 3.8+
- Fivetran account with connector SDK access
- Bolt ECI API credentials

### Dependencies

**For Local Development:**
```bash
pip install pandas>=1.3.0
```

**Note:** The following packages are automatically available in the Fivetran execution environment and should not be included in requirements.txt:
- `fivetran-connector-sdk` - Fivetran SDK (version 1.5.1+)
- `requests` - HTTP library (version 2.32+)
- `json`, `time`, `typing` - Python standard library modules

### Configuration

The connector requires the following configuration parameters:

```json
{
  "base_url": "https://app.bolttech.net",
  "api_token": "your_api_token_here"
}
```

#### Configuration Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `base_url` | string | Yes | Base URL for Bolt ECI API |
| `api_token` | string | Yes | Authentication token for API access |

## Usage

### Local Development

1. Clone the repository
2. Install dependencies
3. Set up configuration
4. Run in debug mode:

```bash
python connector.py
```

### Production Deployment

1. Package the connector
2. Deploy to Fivetran
3. Configure sync schedule
4. Monitor sync logs

## API Integration Details

### Authentication

The connector uses token-based authentication with the following header format:
```
Authorization: Token token="your_token", name="stancil-services"
```

### Pagination Strategies

#### Standard Tables
- Uses `next_batch` parameter for pagination
- Supports `refresh_token` for full resyncs
- Typical page size: 100 records

#### Event Tables
- Uses `event_token` parameter for pagination
- Each token represents a specific point in the event stream
- Events are processed chronologically

### Rate Limiting

- **Limit**: 1000 API calls per hour
- **Delay**: 3.6 seconds between requests
- **Retry Logic**: 3 attempts with exponential backoff

## Data Schema

### Key Features

- **Primary Keys**: Each table has appropriate primary key definitions
- **Data Types**: Optimized for warehouse compatibility
- **Nested Objects**: Flattened for relational storage
- **Arrays**: Serialized as JSON strings
- **Null Handling**: Proper null value management

### Schema Examples

#### Jobs Table
```sql
CREATE TABLE jobs (
    id BIGINT PRIMARY KEY,
    address VARCHAR,
    lot VARCHAR,
    block VARCHAR,
    active BOOLEAN,
    floorplan VARCHAR,
    community VARCHAR,
    city VARCHAR,
    customer VARCHAR,
    customer_id BIGINT,
    -- ... additional fields
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);
```

#### Job Events Table
```sql
CREATE TABLE job_events (
    event_id VARCHAR PRIMARY KEY,
    id BIGINT,
    event VARCHAR,
    author VARCHAR,
    created_at VARCHAR,
    changes TEXT,
    job_id BIGINT,
    job_lot VARCHAR,
    -- ... additional job fields with job_ prefix
);
```

## Error Handling

### Error Types

1. **Authentication Errors** (401)
   - Invalid API token
   - Expired credentials

2. **Rate Limiting** (429)
   - Automatic retry with exponential backoff
   - Respects API limits

3. **Server Errors** (500)
   - Temporary data conversion issues
   - Graceful handling with retry logic

4. **Data Processing Errors**
   - Type conversion issues
   - Missing required fields
   - Invalid data formats

### Error Recovery

- **Individual Table Failures**: Other tables continue syncing
- **State Preservation**: Failed tables maintain sync state
- **Detailed Logging**: Comprehensive error reporting
- **Graceful Degradation**: Partial sync completion

## Monitoring & Troubleshooting

### Log Analysis

Key log messages to monitor:

```
INFO: Starting Bolt ECI sync process
INFO: Processing table: jobs
INFO: Successfully completed sync for table: jobs
INFO: Sync process completed. Processed tables: ['jobs', 'customers']
```

### Common Issues

1. **No Data for Schedules/Invoices**
   - **Cause**: API returns data under different keys
   - **Solution**: Fixed in v1.0.0 - now looks for correct keys

2. **Event Token Loops**
   - **Cause**: API returns same event_token repeatedly
   - **Solution**: Monitor logs for duplicate tokens

3. **Rate Limiting**
   - **Cause**: Too many API calls
   - **Solution**: Built-in rate limiting with 3.6s delays

### Debug Mode

Enable debug logging for detailed troubleshooting:

```python
if __name__ == "__main__":
    log.info("Running Bolt ECI connector in debug mode")
    connector.debug()
```

## Development

### Code Structure

```
connector.py
├── Configuration Constants
├── API Endpoint Definitions
├── Main Update Function
├── Sync Functions
│   ├── Standard Table Sync
│   └── Event Table Sync
├── API Request Functions
├── Data Processing Functions
└── Schema Definition
```

### Adding New Tables

1. Add endpoint to `API_ENDPOINTS`
2. Add key mapping to `extract_records`
3. Add special handling to `clean_record`
4. Add primary key validation
5. Add schema definition

### Testing

1. **Unit Tests**: Test individual functions
2. **Integration Tests**: Test API interactions
3. **End-to-End Tests**: Test full sync process

## Version History

### v1.0.0 (2025-01-27)
- Initial production release
- Fixed schedules/invoices endpoint key mappings
- Removed builder_orders_new endpoint
- Comprehensive error handling
- Full documentation

## Support

### Documentation
- [Bolt ECI API Docs](https://app.bolttech.net/api/docs)
- [Fivetran SDK Docs](https://fivetran.com/docs/connectors/connector-sdk)

### Contact
- **Development Team**: Custom Development Team
- **Issues**: Create GitHub issue for bugs/feature requests

## License

This connector is proprietary software developed for internal use.

---

**Note**: This connector is specifically designed for Bolt ECI's API structure and may require modifications for other systems. 
