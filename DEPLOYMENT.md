# Bolt ECI Fivetran Connector - Deployment Guide

This guide provides step-by-step instructions for deploying the Bolt ECI connector to production in Fivetran.

## Prerequisites

- Fivetran account with connector SDK access
- Bolt ECI API credentials
- Python 3.8+ environment
- Git repository access

## Step 1: Repository Setup

### Clone and Prepare Repository

```bash
# Clone the repository
git clone <your-repo-url>
cd bolt-eci-fivetran-connector

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Verify Installation

```bash
# Test the connector locally
python connector.py
```

## Step 2: Fivetran Configuration

### Create Custom Connector

1. **Log into Fivetran**
   - Navigate to your Fivetran dashboard
   - Go to "Connectors" → "Custom Connectors"

2. **Upload Connector Code**
   - Click "Create Custom Connector"
   - Upload the `connector.py` file
   - Set connector name: "Bolt ECI Connector"

3. **Configure Connector Settings**
   - **Connector Type**: Custom Connector
   - **Language**: Python
   - **Entry Point**: `connector.py`

### Set Configuration Parameters

In the Fivetran connector configuration, set:

```json
{
  "base_url": "https://app.bolttech.net",
  "api_token": "your_bolt_eci_api_token"
}
```

**Important**: Store the API token securely and never commit it to version control.

## Step 3: Schema Configuration

### Review Schema

The connector automatically defines schemas for all tables. Verify the schema matches your requirements:

- **Primary Keys**: Each table has appropriate primary key definitions
- **Data Types**: Optimized for your warehouse
- **Column Names**: Follow your naming conventions

### Customize Schema (Optional)

If you need to modify the schema:

1. Edit the `schema()` function in `connector.py`
2. Update column types or add/remove columns
3. Redeploy the connector

## Step 4: Testing

### Test Configuration

1. **Test Connection**
   - Click "Test Connection" in Fivetran
   - Verify authentication works
   - Check for any configuration errors

2. **Test Sync**
   - Run a test sync with limited data
   - Verify data is extracted correctly
   - Check data quality and formatting

### Debug Issues

If testing fails:

1. **Check Logs**
   - Review Fivetran sync logs
   - Look for authentication errors
   - Verify API endpoint accessibility

2. **Common Issues**
   - Invalid API token
   - Network connectivity problems
   - Rate limiting issues

## Step 5: Production Deployment

### Schedule Sync

1. **Set Sync Frequency**
   - **Recommended**: Every 6 hours for standard tables
   - **Event Tables**: Every 2 hours for real-time updates
   - **Initial Sync**: Full sync for historical data

2. **Configure Sync Schedule**
   - Go to connector settings
   - Set sync frequency
   - Choose sync times (avoid peak hours)

### Monitor Performance

1. **Key Metrics to Track**
   - Sync duration
   - Records processed per sync
   - Error rates
   - API call frequency

2. **Set Up Alerts**
   - Sync failures
   - High error rates
   - Performance degradation

## Step 6: Maintenance

### Regular Monitoring

1. **Daily Checks**
   - Review sync logs
   - Monitor error rates
   - Check data freshness

2. **Weekly Reviews**
   - Analyze performance trends
   - Review API usage
   - Check for data quality issues

### Updates and Maintenance

1. **API Changes**
   - Monitor Bolt ECI API updates
   - Test connector with new API versions
   - Update endpoint configurations if needed

2. **Code Updates**
   - Review and test changes
   - Deploy during maintenance windows
   - Monitor post-deployment performance

## Troubleshooting

### Common Issues

#### Authentication Errors
```
ERROR: Authentication failed - check API token
```
**Solution**: Verify API token is valid and has proper permissions

#### Rate Limiting
```
WARNING: Rate limit hit, waiting before retry
```
**Solution**: Built-in rate limiting should handle this automatically

#### No Data for Specific Tables
```
INFO: No data returned for [table_name]
```
**Solution**: Check if endpoint key mappings are correct

### Debug Mode

For detailed troubleshooting, enable debug mode:

```python
# In connector.py
if __name__ == "__main__":
    log.info("Running Bolt ECI connector in debug mode")
    connector.debug()
```

### Getting Help

1. **Check Documentation**
   - Review README.md
   - Check Fivetran documentation
   - Review Bolt ECI API docs

2. **Contact Support**
   - Create GitHub issue for bugs
   - Contact development team
   - Check Fivetran support

## Security Considerations

### API Token Management

- **Never commit tokens to version control**
- **Use environment variables for local development**
- **Rotate tokens regularly**
- **Use least-privilege access**

### Data Security

- **Encrypt sensitive data in transit**
- **Follow data retention policies**
- **Audit access regularly**
- **Monitor for unusual activity**

## Performance Optimization

### Sync Optimization

1. **Incremental Syncs**
   - Use state management for incremental updates
   - Avoid full resyncs unless necessary
   - Optimize sync frequency

2. **Parallel Processing**
   - Consider parallel table processing
   - Optimize API call patterns
   - Monitor resource usage

### Monitoring and Alerting

1. **Set Up Monitoring**
   - Sync success/failure rates
   - Data volume trends
   - API response times

2. **Configure Alerts**
   - Failed syncs
   - High error rates
   - Performance degradation

---

**Note**: This deployment guide assumes you have administrative access to both Fivetran and Bolt ECI. Adjust steps based on your specific environment and permissions. 