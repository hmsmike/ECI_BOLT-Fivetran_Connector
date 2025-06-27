# Bolt by ECI Custom SDK Connector for Fivetran
# =============================================
# 
# This connector integrates with Bolt by ECI's REST API for data warehouse synchronization.
# It handles both standard table endpoints and event-based endpoints with different pagination strategies.
#
# Key Features:
# - Standard table sync with next_batch pagination
# - Event-based sync with event_token pagination (jobs, work_orders, work_order_statuses)
# - Comprehensive error handling and retry logic
# - Rate limiting to respect API limits
# - Data cleaning and normalization
#
# API Documentation: https://app.bolttech.net/api/docs
# Fivetran SDK Documentation: https://fivetran.com/docs/connectors/connector-sdk
#
# Author: Custom Development Team
# Version: 1.0.0
# Last Updated: 2025-01-27

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

import pandas as pd
import requests as rq
import time
from typing import Dict, Any, Tuple, Optional, Generator, Union
import json

# =============================================================================
# CONFIGURATION CONSTANTS
# =============================================================================

# API Configuration
PAGE_LIMIT = 100  # Bolt typically returns 100 rows per page
RATE_LIMIT_DELAY = 3.6  # 3.6 seconds between requests to stay under 1000 calls/hour
MAX_RETRIES = 3
RETRY_DELAY = 5

# Event Token Configuration
# These tokens represent the starting point for event-based pagination
# Each token is specific to a particular event stream and date range
INITIAL_EVENT_TOKEN = "BAhJIkZ7ImV2ZW50X2lkIjo2NDE2NzIsInBhZ2Vfc2l6ZSI6MTAwLCJwYXRoIjoiL29wZW4vdjEvam9icy9ldmVudHMifQY6BkVU--2ce0a3fbd3b39803fa1f520b30311553dfd7c9d68bfae2ee7e01b5a0f919f4b0"

# Work Order Events - starting from a specific date range
INITIAL_WORK_ORDER_EVENT_TOKEN = "BAhJIk57ImV2ZW50X2lkIjo0Mjg4NTA0LCJwYWdlX3NpemUiOjEwMCwicGF0aCI6Ii9vcGVuL3YxL3dvcmtfb3JkZXJzL2V2ZW50cyJ9BjoGRVQ=--32c7dba9923c9f2e3e196f308a13cf43aef26f97b79896d6d9d99e2056f0b8eb"

# Work Order Status Events - starting from a specific date range  
INITIAL_WORK_ORDER_STATUS_EVENT_TOKEN = "BAhJIlZ7ImV2ZW50X2lkIjozMTg1MDQzLCJwYWdlX3NpemUiOjEwMCwicGF0aCI6Ii9vcGVuL3YxL3dvcmtfb3JkZXJfc3RhdHVzZXMvZXZlbnRzIn0GOgZFVA==--1309f349eb6449740ff2431a58e704746e7450b498ee71022e7427ff0bad0dc2"

# =============================================================================
# API ENDPOINT CONFIGURATION
# =============================================================================
# 
# This dictionary maps table names to their corresponding API endpoints.
# The connector will iterate through these endpoints during each sync cycle.
#
# Note: Some endpoints return data under different keys than the table name:
# - schedules endpoint returns data under "work_orders" key
# - invoices endpoint returns data under "accounting_invoices" key

API_ENDPOINTS = {
    # Standard business tables
    "builder_orders": "/open/v1/builder/orders",
    "cities": "/open/v1/cities", 
    "communities": "/open/v1/communities",
    "contracts": "/open/v1/contracts",
    "crews": "/open/v1/crews",
    "customer_pricings": "/open/v1/customer_pricings",
    "customers": "/open/v1/customers",
    "employees": "/open/v1/employees",
    "floorplans": "/open/v1/floorplans",
    "invoices": "/open/v1/invoices",
    "job_type_configuration": "/open/v1/job_type_configuration",
    "jobs": "/open/v1/jobs",
    "offices": "/open/v1/offices",
    "schedules": "/open/v1/schedules",
    "takeoff_types": "/open/v1/takeoff_types",
    "types": "/open/v1/types",
    "work_order_types": "/open/v1/work_order_types",
    "work_orders": "/open/v1/work_orders",
    
    # Event-based tables (use event_token pagination)
    "job_events": "/open/v1/jobs/events",
    "work_order_events": "/open/v1/work_orders/events", 
    "work_order_status_events": "/open/v1/work_order_statuses/events"
}


def update(configuration: dict, state: dict):
    """
    Main update function called by Fivetran during each sync cycle.
    
    This function orchestrates the entire data synchronization process by:
    1. Validating configuration and extracting credentials
    2. Setting up API request headers
    3. Iterating through all configured endpoints
    4. Calling appropriate sync methods based on endpoint type
    5. Handling errors gracefully and continuing with other tables
    6. Tracking successful and failed table syncs
    
    Args:
        configuration (dict): Dictionary containing API credentials and settings
            Required keys:
            - base_url: Base URL for the Bolt ECI API (e.g., "https://app.bolttech.net")
            - api_token: Authentication token for API access
        state (dict): Dictionary containing sync state from previous runs
            Contains pagination tokens and sync metadata for each table
    
    Yields:
        Fivetran operations (upsert, checkpoint) for each processed record
        
    Raises:
        ValueError: If required configuration parameters are missing
        
    Example:
        >>> config = {"base_url": "https://app.bolttech.net", "api_token": "your_token"}
        >>> state = {"jobs": {"next_batch": "token123"}}
        >>> for operation in update(config, state):
        ...     print(operation)
    """
    log.info("Starting Bolt ECI sync process")
    
    # Extract and validate configuration parameters
    base_url = configuration.get("base_url", "").rstrip("/")
    api_token = configuration.get("api_token")
    
    if not base_url or not api_token:
        error_msg = "Missing required configuration: base_url and api_token"
        log.info(f"ERROR: {error_msg}")
        raise ValueError("base_url and api_token are required in configuration")
    
    # Set up API request headers with authentication and compatibility settings
    headers = {
        "Authorization": f'Token token="{api_token}", name="stancil-services"',
        "Accept": "application/json"
    }
    
    # Track sync results for reporting
    processed_tables = []
    failed_tables = []
    
    # Process each configured endpoint
    for table_name, endpoint in API_ENDPOINTS.items():
        log.info(f"Processing table: {table_name}")
        
        # Get state for this specific table
        table_state = state.get(table_name, {})
        
        try:
            # Route to appropriate sync method based on table type
            if table_name == "job_events":
                # Job events use event token pagination
                for operation in sync_job_events_table(base_url, endpoint, headers, table_name, table_state, state):
                    yield operation
            elif table_name == "work_order_events":
                # Work order events use event token pagination
                for operation in sync_work_order_events_table(base_url, endpoint, headers, table_name, table_state, state):
                    yield operation
            elif table_name == "work_order_status_events":
                # Work order status events use event token pagination
                for operation in sync_work_order_status_events_table(base_url, endpoint, headers, table_name, table_state, state):
                    yield operation
            else:
                # Standard tables use next_batch pagination
                try:
                    for operation in sync_table(base_url, endpoint, headers, table_name, table_state, state):
                        yield operation
                except TypeError as e:
                    # Handle specific float/integer conversion errors
                    if "'float' object cannot be interpreted as an integer" in str(e):
                        error_msg = f"Float/integer conversion error for table {table_name}: {str(e)}"
                        log.info(f"ERROR: {error_msg}")
                        
                        # Update table state with error information
                        table_state["last_sync"] = time.time()
                        table_state["sync_error"] = str(e)
                        state[table_name] = table_state
                        
                        # Mark as failed and continue with other tables
                        failed_tables.append(table_name)
                        continue
                    else:
                        # Re-raise if it's a different TypeError
                        raise
            
            # Mark table as successfully processed
            processed_tables.append(table_name)
            log.info(f"Successfully completed sync for table: {table_name}")
                
        except Exception as e:
            # Handle any other errors during table sync
            error_msg = f"Error syncing table {table_name}: {str(e)}"
            log.info(f"ERROR: {error_msg}")
            
            # Log the full error details for debugging
            import traceback
            log.info(f"Full error traceback for {table_name}: {traceback.format_exc()}")
            
            # Mark table as failed and continue with other tables
            failed_tables.append(table_name)
            
            # Update table state with error information
            table_state["last_sync"] = time.time()
            table_state["sync_error"] = str(e)
            state[table_name] = table_state
            
            # Continue with other tables instead of failing completely
            continue
    
    # Log summary of processed and failed tables
    log.info(f"Sync process completed. Processed tables: {processed_tables}")
    if failed_tables:
        log.info(f"Failed tables: {failed_tables}")
    
    log.info("Bolt ECI sync process completed")


def sync_job_events_table(base_url: str, endpoint: str, headers: dict, table_name: str, table_state: dict, state: dict) -> Generator:
    """
    Sync the job events table using Bolt's event token pagination system.
    
    This function handles the job events endpoint which uses a different pagination strategy
    than standard tables. Instead of next_batch tokens, it uses event_token for pagination.
    Each event represents a change (create, update, destroy) to a job record.
    
    The function:
    1. Retrieves the current event_token from state or uses the initial token
    2. Makes API requests with the event_token parameter
    3. Processes each event in the response
    4. Updates the event_token for the next page
    5. Continues until no more events are available
    
    Args:
        base_url (str): Base API URL (e.g., "https://app.bolttech.net")
        endpoint (str): API endpoint path (e.g., "/open/v1/jobs/events")
        headers (dict): Request headers including authentication
        table_name (str): Name of the table being synced ("job_events")
        table_state (dict): State for this specific table
        state (dict): Full state dictionary (needed for checkpointing)
    
    Yields:
        Fivetran operations (upsert, checkpoint) for each processed event
        
    Raises:
        Exception: If API requests fail or data processing errors occur
        
    Example:
        >>> for operation in sync_job_events_table(base_url, endpoint, headers, "job_events", {}, {}):
        ...     print(operation)
    """
    
    # Get event_token from state, or use initial token for first sync
    event_token = table_state.get("event_token", INITIAL_EVENT_TOKEN)
    
    log.info(f"Starting job events sync with event_token: {event_token[:50]}...")
    
    total_records = 0
    page_count = 0
    
    while True:
        try:
            page_count += 1
            
            # Build API URL with event_token parameter
            api_url = f"{base_url}{endpoint}"
            params = {"event_token": event_token}
            
            log.info(f"Requesting {api_url} with params: {params} and headers: {headers}")
            
            # Make API request with rate limiting and retry logic
            response_data, new_event_token = make_job_events_api_request(api_url, headers, params)
            
            if not response_data:
                log.info("No data returned for job events")
                break
            
            # Extract events from response
            events = response_data.get("events", [])
            
            if not events:
                log.info("No events found in response - end of data reached")
                break
            
            # Process each event in the response
            for event in events:
                if isinstance(event, dict):
                    cleaned_event = clean_job_event_record(event)
                    if cleaned_event:
                        yield op.upsert(table=table_name, data=cleaned_event)
                        total_records += 1
            
            # Update state with new event_token for next iteration
            if new_event_token:
                table_state["event_token"] = new_event_token
                event_token = new_event_token
            
            # Update table state with sync metadata
            table_state["total_records"] = total_records
            table_state["last_sync"] = time.time()
            table_state["pages_processed"] = page_count
            
            # Update the main state and checkpoint
            state[table_name] = table_state
            yield op.checkpoint(state)
            
            log.info(f"Processed {len(events)} events on page {page_count}. Total: {total_records}")
            
            # Rate limiting delay to respect API limits
            time.sleep(RATE_LIMIT_DELAY)
            
        except Exception as e:
            error_msg = f"Error processing job events page {page_count}: {str(e)}"
            log.info(f"ERROR: {error_msg}")
            raise


def sync_table(base_url: str, endpoint: str, headers: dict, table_name: str, table_state: dict, state: dict) -> Generator:
    """
    Sync a specific table using Bolt's pagination system.
    
    Args:
        base_url: Base API URL
        endpoint: API endpoint for this table
        headers: Request headers
        table_name: Name of the table being synced
        table_state: State for this specific table
        state: Full state dictionary (needed for checkpointing)
    
    Yields:
        Fivetran operations (upsert, checkpoint)
    """
    next_batch = table_state.get("next_batch")
    refresh_token = table_state.get("refresh_token")
    total_records = 0
    while True:
        try:
            api_url = f"{base_url}{endpoint}"
            params: Dict[str, Union[str, int]] = {}
            if next_batch:
                params["next_batch"] = next_batch
            elif refresh_token:
                params["refresh_token"] = refresh_token
            # else: no params for initial full sync

            log.info(f"Requesting {api_url} with params: {params} and headers: {headers}")
            response_data, next_batch_new, refresh_token_new = make_api_request(api_url, headers, params)

            if response_data is None:
                log.warning(f"Server error prevented data retrieval for {table_name}. Skipping this table for now.")
                table_state["last_sync"] = time.time()
                table_state["sync_error"] = "Server error - datetime conversion issue"
                state[table_name] = table_state
                yield op.checkpoint(state)
                return

            if not response_data:
                log.info(f"No data returned for {table_name}")
                break

            records = extract_records(response_data, table_name)
            if not records:
                log.info(f"No records found in response for {table_name}")
                break

            for record in records:
                yield op.upsert(table=table_name, data=record)
                total_records += 1

            # Update state and checkpoint
            table_state["total_records"] = total_records
            table_state["last_sync"] = time.time()

            # Pagination logic
            if next_batch_new:
                table_state["next_batch"] = str(next_batch_new)
                next_batch = str(next_batch_new)
            else:
                table_state["next_batch"] = None
                next_batch = None

            if refresh_token_new:
                table_state["refresh_token"] = str(refresh_token_new)
                refresh_token = str(refresh_token_new)

            state[table_name] = table_state
            yield op.checkpoint(state)

            log.info(f"Processed {len(records)} records for {table_name}. Total: {total_records}")

            # If no next_batch, we've reached the end
            if not next_batch:
                log.info(f"Reached end of data for {table_name}")
                break

            time.sleep(RATE_LIMIT_DELAY)
        except Exception as e:
            error_msg = f"Error processing {table_name}: {str(e)}"
            log.info(f"ERROR: {error_msg}")
            table_state["last_sync"] = time.time()
            table_state["sync_error"] = str(e)
            state[table_name] = table_state
            yield op.checkpoint(state)
            return


def sync_work_order_events_table(base_url: str, endpoint: str, headers: dict, table_name: str, table_state: dict, state: dict) -> Generator:
    event_token = table_state.get("event_token", INITIAL_WORK_ORDER_EVENT_TOKEN)
    log.info(f"Starting work order events sync with event_token: {event_token[:50]}...")
    total_records = 0
    page_count = 0
    while True:
        try:
            page_count += 1
            api_url = f"{base_url}{endpoint}"
            params = {"event_token": event_token}
            log.info(f"Requesting {api_url} with params: {params} and headers: {headers}")
            response_data, new_event_token = make_job_events_api_request(api_url, headers, params)
            if not response_data:
                log.info("No data returned for work order events")
                break
            events = response_data.get("events", [])
            if not events:
                log.info("No events found in response - end of data reached")
                break
            for event in events:
                if isinstance(event, dict):
                    cleaned_event = clean_work_order_event_record(event)
                    if cleaned_event:
                        yield op.upsert(table=table_name, data=cleaned_event)
                        total_records += 1
            if new_event_token:
                table_state["event_token"] = new_event_token
                event_token = new_event_token
            table_state["total_records"] = total_records
            table_state["last_sync"] = time.time()
            table_state["pages_processed"] = page_count
            state[table_name] = table_state
            yield op.checkpoint(state)
            log.info(f"Processed {len(events)} events on page {page_count}. Total: {total_records}")
            time.sleep(RATE_LIMIT_DELAY)
        except Exception as e:
            error_msg = f"Error processing work order events page {page_count}: {str(e)}"
            log.info(f"ERROR: {error_msg}")
            raise


def sync_work_order_status_events_table(base_url: str, endpoint: str, headers: dict, table_name: str, table_state: dict, state: dict) -> Generator:
    event_token = table_state.get("event_token", INITIAL_WORK_ORDER_STATUS_EVENT_TOKEN)
    log.info(f"Starting work order status events sync with event_token: {event_token[:50]}...")
    total_records = 0
    page_count = 0
    while True:
        try:
            page_count += 1
            api_url = f"{base_url}{endpoint}"
            params = {"event_token": event_token}
            log.info(f"Requesting {api_url} with params: {params} and headers: {headers}")
            response_data, new_event_token = make_job_events_api_request(api_url, headers, params)
            if not response_data:
                log.info("No data returned for work order status events")
                break
            events = response_data.get("events", [])
            if not events:
                log.info("No events found in response - end of data reached")
                break
            for event in events:
                if isinstance(event, dict):
                    cleaned_event = clean_work_order_status_event_record(event)
                    if cleaned_event:
                        yield op.upsert(table=table_name, data=cleaned_event)
                        total_records += 1
            if new_event_token:
                table_state["event_token"] = new_event_token
                event_token = new_event_token
            table_state["total_records"] = total_records
            table_state["last_sync"] = time.time()
            table_state["pages_processed"] = page_count
            state[table_name] = table_state
            yield op.checkpoint(state)
            log.info(f"Processed {len(events)} events on page {page_count}. Total: {total_records}")
            time.sleep(RATE_LIMIT_DELAY)
        except Exception as e:
            error_msg = f"Error processing work order status events page {page_count}: {str(e)}"
            log.info(f"ERROR: {error_msg}")
            raise


def make_job_events_api_request(url: str, headers: dict, params: dict) -> Tuple[Optional[dict], Optional[str]]:
    """
    Make API request for job events with retry logic and error handling.
    
    Args:
        url: API endpoint URL
        headers: Request headers
        params: Query parameters including event_token
        
    Returns:
        Tuple of (response_data, new_event_token)
    """
    # Add extra headers for compatibility
    request_headers = headers.copy()
    request_headers["User-Agent"] = "Mozilla/5.0 (compatible; FivetranConnector/1.0)"
    request_headers["Accept-Encoding"] = "gzip, deflate, br"
    request_headers["Connection"] = "keep-alive"
    request_headers["Referer"] = "https://app.bolttech.net/"
    request_headers["Origin"] = "https://app.bolttech.net"
    
    for attempt in range(MAX_RETRIES):
        try:
            # Commented out debug logs for production
            # log.info(f"DEBUG: Requesting {url} with params: {params} and headers: {request_headers}")
            response = rq.get(url, headers=request_headers, params=params, timeout=30)
            # Commented out debug logs for production
            # log.info(f"DEBUG: Response status: {response.status_code}")
            # log.info(f"DEBUG: Response headers: {dict(response.headers)}")
            # log.info(f"DEBUG: Response text: {response.text[:1000]}")  # Truncate for large responses
            
            # Handle rate limiting
            if response.status_code == 429:
                log.warning("Rate limit hit, waiting before retry")
                time.sleep(RETRY_DELAY * int(attempt + 1))
                continue
            
            # Handle authentication errors
            if response.status_code == 401:
                error_msg = "Authentication failed - check API token"
                log.info(f"ERROR: {error_msg}")
                raise ValueError("Invalid API token or authentication failed")
            
            # Handle other client errors
            if response.status_code >= 400:
                error_msg = f"API request failed with status {response.status_code}: {response.text}"
                log.info(f"ERROR: {error_msg}")
                response.raise_for_status()
            
            # Parse JSON response
            data = response.json()
            
            # Extract new event_token for next page
            new_event_token = data.get("event_token")
            
            return data, new_event_token
            
        except rq.exceptions.RequestException as e:
            log.warning(f"Request attempt {attempt + 1} failed: {str(e)}")
            if attempt == MAX_RETRIES - 1:
                error_msg = f"All retry attempts failed for {url}"
                log.info(f"ERROR: {error_msg}")
                raise
            time.sleep(RETRY_DELAY * int(attempt + 1))
        
        except json.JSONDecodeError as e:
            error_msg = f"Failed to parse JSON response: {str(e)}"
            log.info(f"ERROR: {error_msg}")
            raise
    
    return None, None


def make_api_request(url: str, headers: dict, params: Dict[str, Union[str, int]]) -> Tuple[Optional[Union[dict, list]], Optional[str], Optional[str]]:
    """
    Make API request with retry logic and error handling.
    
    Args:
        url: API endpoint URL
        headers: Request headers
        params: Query parameters
        
    Returns:
        Tuple of (response_data, next_batch_token, refresh_token)
    """
    # Add extra headers for compatibility
    request_headers = headers.copy()
    request_headers["User-Agent"] = "Mozilla/5.0 (compatible; FivetranConnector/1.0)"
    request_headers["Accept-Encoding"] = "gzip, deflate, br"
    request_headers["Connection"] = "keep-alive"
    request_headers["Referer"] = "https://app.bolttech.net/"
    request_headers["Origin"] = "https://app.bolttech.net"
    
    for attempt in range(int(MAX_RETRIES)):
        try:
            # Commented out debug logs for production
            # log.info(f"DEBUG: Requesting {url} with params: {params} and headers: {request_headers}")
            response = rq.get(url, headers=request_headers, params=params, timeout=30)
            # Commented out debug logs for production
            # log.info(f"DEBUG: Response status: {response.status_code}")
            # log.info(f"DEBUG: Response headers: {dict(response.headers)}")
            # log.info(f"DEBUG: Response text: {response.text[:1000]}")  # Truncate for large responses
            
            # Handle rate limiting
            if response.status_code == 429:
                log.warning("Rate limit hit, waiting before retry")
                time.sleep(RETRY_DELAY * int(attempt + 1))
                continue
            
            # Handle authentication errors
            if response.status_code == 401:
                error_msg = "Authentication failed - check API token"
                log.info(f"ERROR: {error_msg}")
                raise ValueError("Invalid API token or authentication failed")
            
            # Handle 500 server errors - these are often temporary data issues
            if response.status_code == 500:
                try:
                    error_data = response.json()
                    if "to_datetime" in error_data.get("exception", ""):
                        log.warning(f"Server error with datetime conversion issue. This may be due to null date fields in the API data. Attempt {attempt + 1}/{MAX_RETRIES}")
                        if attempt == MAX_RETRIES - 1:
                            log.info(f"ERROR: Server error after {MAX_RETRIES} attempts. The API has data issues that need to be resolved on their end.")
                            return None, None, None
                        time.sleep(RETRY_DELAY * int(attempt + 1))
                        continue
                    else:
                        log.warning(f"Server error: {error_data.get('message', 'Unknown server error')}. Attempt {attempt + 1}/{MAX_RETRIES}")
                        if attempt == MAX_RETRIES - 1:
                            log.info(f"ERROR: Server error after {MAX_RETRIES} attempts")
                            return None, None, None
                        time.sleep(RETRY_DELAY * int(attempt + 1))
                        continue
                except json.JSONDecodeError:
                    log.warning(f"Server error with non-JSON response. Attempt {attempt + 1}/{MAX_RETRIES}")
                    if attempt == MAX_RETRIES - 1:
                        log.info(f"ERROR: Server error after {MAX_RETRIES} attempts")
                        return None, None, None
                    time.sleep(RETRY_DELAY * int(attempt + 1))
                    continue
            
            # Handle other client errors
            if response.status_code >= 400:
                error_msg = f"API request failed with status {response.status_code}: {response.text}"
                log.info(f"ERROR: {error_msg}")
                response.raise_for_status()
            
            # Parse JSON response
            data = response.json()
            
            # Handle different response formats
            if isinstance(data, list):
                # Some endpoints return a list directly (like job_type_configuration)
                return data, None, None
            elif isinstance(data, dict):
                # Standard response with next_batch and refresh_token
                next_batch = data.get("next_batch")
                refresh_token = data.get("refresh_token")
                return data, next_batch, refresh_token
            else:
                # Unexpected response format
                log.warning(f"Unexpected response format: {type(data)}")
                return data, None, None
            
        except rq.exceptions.RequestException as e:
            log.warning(f"Request attempt {attempt + 1} failed: {str(e)}")
            if attempt == MAX_RETRIES - 1:
                error_msg = f"All retry attempts failed for {url}"
                log.info(f"ERROR: {error_msg}")
                raise
            time.sleep(RETRY_DELAY * int(attempt + 1))
        
        except json.JSONDecodeError as e:
            error_msg = f"Failed to parse JSON response: {str(e)}"
            log.info(f"ERROR: {error_msg}")
            raise
    
    return None, None, None


def extract_records(response_data: Union[dict, list], table_name: str) -> list:
    """
    Extract records from Bolt ECI API response.
    Based on the YAML structure, records are returned in arrays named after the table.
    """
    records = []
    
    # Handle case where response_data is a list directly
    if isinstance(response_data, list):
        if table_name == "job_type_configuration":
            records = response_data
        else:
            log.warning(f"Unexpected list response for {table_name}")
            records = []
    elif isinstance(response_data, dict):
        # For each table, data is in an array named after the table
        if table_name == "jobs" and "jobs" in response_data:
            records = response_data["jobs"]
        elif table_name == "communities" and "communities" in response_data:
            records = response_data["communities"]
        elif table_name == "customers" and "customers" in response_data:
            records = response_data["customers"]
        elif table_name == "employees" and "employees" in response_data:
            records = response_data["employees"]
        elif table_name == "crews" and "crews" in response_data:
            records = response_data["crews"]
        elif table_name == "work_orders" and "work_orders" in response_data:
            records = response_data["work_orders"]
        elif table_name == "work_order_types" and "work_order_types" in response_data:
            records = response_data["work_order_types"]
        elif table_name == "floorplans" and "floorplans" in response_data:
            records = response_data["floorplans"]
        elif table_name == "customer_pricings" and "customer_pricings" in response_data:
            records = response_data["customer_pricings"]
        elif table_name == "contracts" and "contracts" in response_data:
            records = response_data["contracts"]
        elif table_name == "invoices" and "accounting_invoices" in response_data:
            records = response_data["accounting_invoices"]
        elif table_name == "builder_orders" and "builder_orders" in response_data:
            records = response_data["builder_orders"]
        elif table_name == "types" and "types" in response_data:
            records = response_data["types"]
        elif table_name == "cities" and "cities" in response_data:
            records = response_data["cities"]
        elif table_name == "offices" and "offices" in response_data:
            records = response_data["offices"]
        elif table_name == "schedules" and "work_orders" in response_data:
            records = response_data["work_orders"]
        elif table_name == "takeoff_types" and "takeoff_types" in response_data:
            records = response_data["takeoff_types"]
        elif table_name == "job_type_configuration":
            # Handle job_type_configuration - check if it's a list directly or nested
            if "job_type_configuration" in response_data:
                records = response_data["job_type_configuration"]
            else:
                # Log the actual response structure for debugging
                log.info(f"job_type_configuration response structure: {list(response_data.keys()) if isinstance(response_data, dict) else type(response_data)}")
                log.info(f"job_type_configuration response data: {response_data}")
                records = []
        else:
            possible_keys = [
                "data",
                "results", 
                "items",
                table_name,
                f"{table_name}_list"
            ]
            for key in possible_keys:
                if key in response_data:
                    potential_records = response_data[key]
                    if isinstance(potential_records, list):
                        records = potential_records
                        break
    else:
        log.warning(f"Unexpected response_data type for {table_name}: {type(response_data)}")
        records = []
    
    # Ensure records is a list
    if not isinstance(records, list):
        log.warning(f"Records for {table_name} is not a list: {type(records)}")
        records = []
    
    cleaned_records = []
    skipped_count = 0
    for record in records:
        if isinstance(record, dict):
            cleaned_record = clean_record(record, table_name)
            if cleaned_record:
                cleaned_records.append(cleaned_record)
            else:
                skipped_count += 1
        else:
            log.warning(f"Skipping non-dict record for {table_name}: {type(record)}")
            skipped_count += 1
    
    if skipped_count > 0:
        log.info(f"Skipped {skipped_count} invalid records for {table_name}")
    
    return cleaned_records


def clean_job_event_record(event: dict) -> Optional[dict]:
    """
    Clean and normalize a job event record.
    
    Args:
        event: Raw event dictionary from API
        
    Returns:
        Cleaned event record dictionary, or None if record should be skipped
    """
    
    cleaned = {}
    
    # Extract main event fields
    cleaned["id"] = event.get("id")  # Event ID from API response
    cleaned["event"] = event.get("event")  # create, update, destroy
    cleaned["author"] = event.get("author")  # Author of the event
    cleaned["created_at"] = event.get("created_at")
    
    # Handle changes field (only present for update events)
    changes = event.get("changes")
    if changes:
        cleaned["changes"] = json.dumps(changes)
    else:
        cleaned["changes"] = None
    
    # Extract job data
    job_data = event.get("job", {})
    if job_data:
        # Flatten job fields with job_ prefix to avoid conflicts
        for key, value in job_data.items():
            clean_key = f"job_{str(key).lower().replace(' ', '_').replace('-', '_')}"
            
            if value is None:
                cleaned[clean_key] = None
            elif isinstance(value, (str, int, float, bool)):
                # Handle empty strings
                if isinstance(value, str) and value.strip() == "":
                    cleaned[clean_key] = None
                else:
                    cleaned[clean_key] = value
            else:
                # Convert complex types to JSON string
                cleaned[clean_key] = json.dumps(value)
    
    # Create a unique identifier for the event record
    # Combination of event id, job_id, event type, and created_at should be unique
    if cleaned.get("id") and cleaned.get("job_id") and cleaned.get("event") and cleaned.get("created_at"):
        # Create a composite key for the event
        event_id = str(cleaned["id"])
        job_id = str(cleaned["job_id"])
        event_type = str(cleaned["event"])
        created_at = str(cleaned["created_at"])
        cleaned["event_id"] = f"{event_id}_{job_id}_{event_type}_{created_at}".replace(":", "").replace("-", "").replace(" ", "_")
    else:
        log.warning(f"Skipping job event record with missing required fields: {cleaned}")
        return None
    
    return cleaned


def clean_record(record: dict, table_name: Optional[str] = None) -> Optional[dict]:
    cleaned = {}
    for key, value in record.items():
        clean_key = str(key).lower().replace(" ", "_").replace("-", "_")
        # Special handling for customers table nested objects
        if table_name == "customers" and isinstance(value, dict):
            if key in ["corporate_address", "billing_address"]:
                prefix = clean_key + "_"
                for addr_key, addr_value in value.items():
                    addr_clean_key = prefix + str(addr_key).lower().replace(" ", "_").replace("-", "_")
                    cleaned[addr_clean_key] = addr_value if addr_value is not None else None
            elif key in ["corporate_contact", "billing_contact"]:
                prefix = clean_key + "_"
                for contact_key, contact_value in value.items():
                    contact_clean_key = prefix + str(contact_key).lower().replace(" ", "_").replace("-", "_")
                    cleaned[contact_clean_key] = contact_value if contact_value is not None else None
            else:
                cleaned[clean_key] = json.dumps(value)
        # Special handling for employees table nested objects
        elif table_name == "employees" and isinstance(value, dict):
            if key == "address":
                prefix = "address_"
                for addr_key, addr_value in value.items():
                    addr_clean_key = prefix + str(addr_key).lower().replace(" ", "_").replace("-", "_")
                    cleaned[addr_clean_key] = addr_value if addr_value is not None else None
            elif key == "auto_lunch":
                prefix = "auto_lunch_"
                for lunch_key, lunch_value in value.items():
                    lunch_clean_key = prefix + str(lunch_key).lower().replace(" ", "_").replace("-", "_")
                    cleaned[lunch_clean_key] = lunch_value if lunch_value is not None else None
            else:
                cleaned[clean_key] = json.dumps(value)
        # Special handling for work_orders table nested/array fields
        elif table_name == "work_orders":
            # Serialize arrays and dicts as JSON
            if isinstance(value, (dict, list)):
                cleaned[clean_key] = json.dumps(value)
            else:
                cleaned[clean_key] = value if value != "" else None
        # Special handling for work_order_types table nested/array fields
        elif table_name == "work_order_types":
            # Serialize arrays and dicts as JSON
            if isinstance(value, (dict, list)):
                cleaned[clean_key] = json.dumps(value)
            else:
                cleaned[clean_key] = value if value != "" else None
        # Special handling for floorplans table nested/array fields
        elif table_name == "floorplans":
            # Serialize arrays and dicts as JSON for extended_labor_costs
            if clean_key == "extended_labor_costs" and isinstance(value, (dict, list)):
                cleaned[clean_key] = json.dumps(value)
            elif isinstance(value, (dict, list)):
                cleaned[clean_key] = json.dumps(value)
            else:
                cleaned[clean_key] = value if value != "" else None
        # Special handling for customer_pricings table nested/array fields
        elif table_name == "customer_pricings":
            # Serialize arrays and dicts as JSON
            if isinstance(value, (dict, list)):
                cleaned[clean_key] = json.dumps(value)
            else:
                cleaned[clean_key] = value if value != "" else None
        # Special handling for contracts table nested/array fields
        elif table_name == "contracts":
            # Serialize arrays and dicts as JSON
            if isinstance(value, (dict, list)):
                cleaned[clean_key] = json.dumps(value)
            else:
                cleaned[clean_key] = value if value != "" else None
        # Special handling for invoices table nested/array fields
        elif table_name == "invoices":
            # Serialize arrays and dicts as JSON for extras, contract_details, job_ids, work_order_ids, po_numbers
            if clean_key in ["extras", "contract_details", "job_ids", "work_order_ids", "po_numbers"] and isinstance(value, (dict, list)):
                cleaned[clean_key] = json.dumps(value)
            elif isinstance(value, (dict, list)):
                cleaned[clean_key] = json.dumps(value)
            else:
                cleaned[clean_key] = value if value != "" else None
        # Special handling for builder_orders table nested/array fields
        elif table_name == "builder_orders":
            # Serialize arrays and dicts as JSON, especially item_details
            if clean_key == "item_details" and isinstance(value, list):
                cleaned[clean_key] = json.dumps(value)
            elif isinstance(value, (dict, list)):
                cleaned[clean_key] = json.dumps(value)
            else:
                cleaned[clean_key] = value if value != "" else None
        # Special handling for types table nested/array fields
        elif table_name == "types":
            # Serialize arrays and dicts as JSON
            if isinstance(value, (dict, list)):
                cleaned[clean_key] = json.dumps(value)
            else:
                cleaned[clean_key] = value if value != "" else None
        # Special handling for cities table nested/array fields
        elif table_name == "cities":
            # Serialize arrays and dicts as JSON
            if isinstance(value, (dict, list)):
                cleaned[clean_key] = json.dumps(value)
            else:
                cleaned[clean_key] = value if value != "" else None
        # Special handling for offices table nested/array fields
        elif table_name == "offices":
            # Serialize arrays and dicts as JSON
            if isinstance(value, (dict, list)):
                cleaned[clean_key] = json.dumps(value)
            else:
                cleaned[clean_key] = value if value != "" else None
        # Special handling for schedules table nested/array fields
        elif table_name == "schedules":
            # Serialize arrays and dicts as JSON for crews, crew_ids, floorplan_options_charges, job_extras_charges
            if clean_key in ["crews", "crew_ids", "floorplan_options_charges", "job_extras_charges"] and isinstance(value, (dict, list)):
                cleaned[clean_key] = json.dumps(value)
            elif isinstance(value, (dict, list)):
                cleaned[clean_key] = json.dumps(value)
            else:
                cleaned[clean_key] = value if value != "" else None
        # Special handling for takeoff_types table nested/array fields
        elif table_name == "takeoff_types":
            # Serialize arrays and dicts as JSON
            if isinstance(value, (dict, list)):
                cleaned[clean_key] = json.dumps(value)
            else:
                cleaned[clean_key] = value if value != "" else None
        # Special handling for job_type_configuration table nested/array fields
        elif table_name == "job_type_configuration":
            # Serialize arrays and dicts as JSON
            if isinstance(value, (dict, list)):
                cleaned[clean_key] = json.dumps(value)
            else:
                cleaned[clean_key] = value if value != "" else None
        # Special handling for crews table nested objects
        elif table_name == "crews" and isinstance(value, dict):
            if key == "member_pays":
                cleaned[clean_key] = json.dumps(value)
            else:
                cleaned[clean_key] = json.dumps(value)
        elif value is None:
            cleaned[clean_key] = None
        elif isinstance(value, (str, int, float, bool)):
            if isinstance(value, str) and value.strip() == "":
                cleaned[clean_key] = None
            else:
                cleaned[clean_key] = value
        elif isinstance(value, dict):
            cleaned[clean_key] = json.dumps(value)
        elif isinstance(value, list):
            cleaned[clean_key] = json.dumps(value)
        else:
            cleaned[clean_key] = str(value)
    # Validate primary key fields - skip records with missing primary keys
    if table_name == "employees":
        if not cleaned.get("id"):
            log.warning(f"Skipping employee record with missing id: {cleaned}")
            return None
    elif table_name == "customers":
        if not cleaned.get("id"):
            log.warning(f"Skipping customer record with missing id: {cleaned}")
            return None
    elif table_name == "communities":
        if not cleaned.get("id"):
            log.warning(f"Skipping community record with missing id: {cleaned}")
            return None
    elif table_name == "jobs":
        if not cleaned.get("id"):
            log.warning(f"Skipping job record with missing id: {cleaned}")
            return None
    elif table_name == "crews":
        if not cleaned.get("id"):
            log.warning(f"Skipping crew record with missing id: {cleaned}")
            return None
    elif table_name == "work_orders":
        if not cleaned.get("id"):
            log.warning(f"Skipping work_order record with missing id: {cleaned}")
            return None
    elif table_name == "work_order_types":
        if not cleaned.get("id"):
            log.warning(f"Skipping work_order_type record with missing id: {cleaned}")
            return None
    elif table_name == "floorplans":
        if not cleaned.get("id"):
            log.warning(f"Skipping floorplan record with missing id: {cleaned}")
            return None
    elif table_name == "customer_pricings":
        if not cleaned.get("id"):
            log.warning(f"Skipping customer_pricing record with missing id: {cleaned}")
            return None
    elif table_name == "contracts":
        if not cleaned.get("id"):
            log.warning(f"Skipping contract record with missing id: {cleaned}")
            return None
    elif table_name == "invoices":
        if not cleaned.get("id"):
            log.warning(f"Skipping invoice record with missing id: {cleaned}")
            return None
    elif table_name == "builder_orders":
        if not cleaned.get("id"):
            log.warning(f"Skipping builder_order record with missing id: {cleaned}")
            return None
    elif table_name == "job_type_configuration":
        if not cleaned.get("job_type_id"):
            log.warning(f"Skipping job_type_configuration record with missing job_type_id: {cleaned}")
            return None
    elif table_name == "types":
        if not cleaned.get("id"):
            log.warning(f"Skipping types record with missing id: {cleaned}")
            return None
    elif table_name == "cities":
        if not cleaned.get("id"):
            log.warning(f"Skipping cities record with missing id: {cleaned}")
            return None
    elif table_name == "offices":
        if not cleaned.get("id"):
            log.warning(f"Skipping offices record with missing id: {cleaned}")
            return None
    elif table_name == "schedules":
        if not cleaned.get("id"):
            log.warning(f"Skipping schedules record with missing id: {cleaned}")
            return None
    elif table_name == "takeoff_types":
        if not cleaned.get("id"):
            log.warning(f"Skipping takeoff_types record with missing id: {cleaned}")
            return None
    return cleaned


def clean_work_order_event_record(event: dict) -> Optional[dict]:
    """
    Clean and normalize a work order event record.
    
    Args:
        event: Raw event dictionary from API
        
    Returns:
        Cleaned event record dictionary, or None if record should be skipped
    """
    
    cleaned = {}
    
    # Extract main event fields
    cleaned["event"] = event.get("event")  # create, update, destroy
    cleaned["created_at"] = event.get("created_at")
    cleaned["author"] = event.get("author")  # Added based on actual API response
    
    # Handle changes field (only present for update events)
    changes = event.get("changes")
    if changes:
        cleaned["changes"] = json.dumps(changes)
    else:
        cleaned["changes"] = None
    
    # Extract work_order data
    work_order_data = event.get("work_order", {})
    if work_order_data:
        # Flatten work_order fields with work_order_ prefix to avoid conflicts
        for key, value in work_order_data.items():
            clean_key = f"work_order_{str(key).lower().replace(' ', '_').replace('-', '_')}"
            
            if value is None:
                cleaned[clean_key] = None
            elif isinstance(value, (str, int, float, bool)):
                # Handle empty strings
                if isinstance(value, str) and value.strip() == "":
                    cleaned[clean_key] = None
                else:
                    cleaned[clean_key] = value
            else:
                # Convert complex types to JSON string
                cleaned[clean_key] = json.dumps(value)
    
    # Create a unique identifier for the event record
    # Combination of work_order_id, event type, and created_at should be unique
    if cleaned.get("work_order_id") and cleaned.get("event") and cleaned.get("created_at"):
        # Create a composite key for the event
        work_order_id = str(cleaned["work_order_id"])
        event_type = str(cleaned["event"])
        created_at = str(cleaned["created_at"])
        cleaned["event_id"] = f"{work_order_id}_{event_type}_{created_at}".replace(":", "").replace("-", "").replace(" ", "_")
    else:
        log.warning(f"Skipping work order event record with missing required fields: {cleaned}")
        return None
    
    return cleaned


def clean_work_order_status_event_record(event: dict) -> Optional[dict]:
    cleaned = {}
    cleaned["id"] = event.get("id")
    cleaned["event"] = event.get("event")
    cleaned["author"] = event.get("author")
    cleaned["created_at"] = event.get("created_at")
    # Handle changes field (may be missing)
    changes = event.get("changes")
    if changes:
        cleaned["changes"] = json.dumps(changes)
    else:
        cleaned["changes"] = None
    # Flatten status object
    status = event.get("status", {})
    if status:
        for key, value in status.items():
            clean_key = f"status_{str(key).lower().replace(' ', '_').replace('-', '_')}"
            cleaned[clean_key] = value if not isinstance(value, (dict, list)) else json.dumps(value)
    # Compose event_id as unique key
    if cleaned.get("id") and cleaned.get("status_work_order_id") and cleaned.get("event") and cleaned.get("created_at"):
        event_id = str(cleaned["id"])
        work_order_id = str(cleaned["status_work_order_id"])
        event_type = str(cleaned["event"])
        created_at = str(cleaned["created_at"])
        cleaned["event_id"] = f"{event_id}_{work_order_id}_{event_type}_{created_at}".replace(":", "").replace("-", "").replace(" ", "_")
    else:
        log.warning(f"Skipping work order status event record with missing required fields: {cleaned}")
        return None
    return cleaned


def schema(configuration: dict):
    """
    Define the schema for all Bolt ECI tables including Job Events.
    This function returns a list containing table schema definitions.
    
    Args:
        configuration: Configuration dictionary
    
    Returns:
        List containing table schema dictionaries
    """
    
    # Return a list of table schemas - this is what Fivetran SDK expects
    return [
        {
            "table": "jobs",
            "primary_key": ["id"],
            "columns": {
                "id": "LONG",
                "address": "STRING", 
                "lot": "STRING",
                "block": "STRING",
                "active": "BOOLEAN",
                "floorplan": "STRING",
                "community": "STRING",
                "city": "STRING",
                "customer": "STRING",
                "customer_id": "LONG",
                "office": "STRING",
                "home_owner": "STRING",
                "home_owner_email": "STRING",
                "home_owner_phone": "STRING",
                "permit_number": "STRING",
                "permit_date": "STRING",
                "start_date": "NAIVE_DATE",
                "builder_job_number": "STRING",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
                "notes": "STRING",
                "zip": "STRING",  
                "accounting_number": "STRING",
                "under_warranty": "BOOLEAN",
                "municipality": "STRING",
                "customer_external_id": "STRING",
                "community_external_id": "STRING",
                "job_type_id": "LONG",
                "job_type": "STRING",
                "close_date": "NAIVE_DATE",
                "floorplan_id": "LONG",
                "etag": "STRING"
            }
        },
        {
            "table": "communities",
            "primary_key": ["id"],
            "columns": {
                "id": "LONG",
                "name": "STRING",
                "city": "STRING",
                "customer": "STRING",
                "office": "STRING",
                "supervisor": "STRING",
                "project_manager": "STRING",
                "active": "BOOLEAN",
                "security": "BOOLEAN",
                "warranties": "STRING",  # May be null/empty in API
                "email_confirmation_to_supervisor": "BOOLEAN",
                "under_warranty": "BOOLEAN",
                "map_location": "STRING",  # May be null/empty in API
                "external_id": "STRING",
                "etag": "STRING"
            }
        },
        {
            "table": "customers",
            "primary_key": ["id"],
            "columns": {
                "id": "LONG",
                "name": "STRING",
                "status": "STRING",
                "phone": "STRING",
                "email": "STRING",
                "notes": "STRING",
                "po_required": "BOOLEAN",
                "work_order_notes": "STRING",
                "email_confirmation_to_supervisor": "BOOLEAN",
                "opt_out_email_confirmation": "BOOLEAN",
                "opt_out_auto_confirmation": "BOOLEAN",
                "supervisor": "STRING",
                # Corporate address fields (flattened)
                "corporate_address_address1": "STRING",
                "corporate_address_address2": "STRING",
                "corporate_address_city": "STRING",
                "corporate_address_state": "STRING",
                "corporate_address_zip": "STRING",
                # Corporate contact fields (flattened)
                "corporate_contact_fullname": "STRING",
                "corporate_contact_email": "STRING",
                "corporate_contact_phone": "STRING",
                "corporate_contact_cellphone": "STRING",
                "corporate_contact_fax": "STRING",
                # Billing address fields (flattened)
                "billing_address_address1": "STRING",
                "billing_address_address2": "STRING",
                "billing_address_city": "STRING",
                "billing_address_state": "STRING",
                "billing_address_zip": "STRING",
                # Billing contact fields (flattened)
                "billing_contact_fullname": "STRING",
                "billing_contact_email": "STRING",
                "billing_contact_phone": "STRING",
                "billing_contact_cellphone": "STRING",
                "billing_contact_fax": "STRING",
                # Other fields
                "external_id": "STRING",
                "etag": "STRING"
            }
        },
        {
            "table": "employees",
            "primary_key": ["id"],  # Corrected: id is the primary key, not username
            "columns": {
                "id": "LONG",
                "username": "STRING",
                "fullname": "STRING",
                "email": "STRING",
                "personal_email": "STRING",
                "phone": "STRING",
                "cellphone": "STRING",
                "crew": "STRING",
                "fax": "STRING",
                "active": "BOOLEAN",
                "holiday_pay": "BOOLEAN",
                "weekly_hours": "LONG",
                "user_class": "STRING",
                "user_type_id": "LONG",
                "birth_date": "STRING",  # Can be empty string in API
                "rate": "STRING",  # Can be empty string in API
                "piece_pay": "BOOLEAN",
                "title": "STRING",
                "notes": "STRING",
                "elligible_for_rehire": "BOOLEAN",  # Note: keeping original spelling from API
                "hire_date": "STRING",  # Can be null/empty in API
                "office_ids": "STRING",  # JSON array converted to string
                "payroll_id": "STRING",
                "auto_lunch": "STRING",  # JSON object or null
                "department_type_id": "LONG",
                "offices": "STRING",  # JSON array converted to string
                "reports_to": "STRING",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
                # Address fields (flattened)
                "address_address1": "STRING",
                "address_address2": "STRING",
                "address_city": "STRING",
                "address_state": "STRING",  
                "address_zip": "STRING",
                "etag": "STRING",
                "timezone": "STRING"
            }
        },
        {
            "table": "crews",
            "primary_key": ["id"],
            "columns": {
                "id": "LONG",
                "name": "STRING",
                "active": "BOOLEAN",
                "phone": "STRING",
                "calendar_color": "STRING",
                "truck_number": "STRING",
                "update_member_ids": "STRING",  # JSON array converted to string
                "members": "STRING",  # JSON array converted to string  
                "member_pays": "STRING",  # JSON object converted to string
                "leader_id": "LONG",
                "leader": "STRING",
                "office_ids": "STRING",  # JSON array converted to string
                "offices": "STRING",  # JSON array converted to string
                "crew_type_ids": "STRING",  # JSON array converted to string
                "crew_types": "STRING",  # JSON array converted to string
                "etag": "STRING"
            }
        },
        {
            "table": "work_orders",
            "primary_key": ["id"],
            "columns": {
                "id": "LONG",
                "work_order_type": "STRING",
                "work_order_type_id": "LONG",
                "job_number": "LONG",
                "supervisor_id": "LONG",
                "reschedule_date": "STRING",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
                "notes": "STRING",
                "stage": "STRING",
                "crew_notes": "STRING",
                "home_owner": "STRING",
                "home_owner_email": "STRING",
                "home_owner_phone": "STRING",
                "completed_date": "STRING",
                "estimated_completion_date": "STRING",
                "percentage_of_completion": "LONG",
                "crews": "STRING",
                "crew_ids": "STRING",
                "floorplan_options_charges": "STRING",
                "job_extras_charges": "STRING",
                "labor_total": "FLOAT",
                "price_total": "FLOAT",
                "floorplan_contract_charge": "FLOAT",
                "floorplan_labor_charge": "FLOAT",
                "crew_pay_extra_added_pay": "FLOAT",
                "appointment_start_time": "STRING",
                "appointment_end_time": "STRING",
                "schedule_change_comment": "STRING",
                "schedule_change_reason": "STRING",
                "warranty_issue_id": "LONG",
                "warranty_issue": "STRING",
                "department_type": "STRING",
                "etag": "STRING"
            }
        },
        {
            "table": "job_type_configuration",
            "primary_key": ["job_type_id"],
            "columns": {
                "job_type_id": "LONG",
                "description": "STRING",
                "offices": "STRING"
            }
        },
        {
            "table": "work_order_types",
            "primary_key": ["id"],
            "columns": {
                "id": "LONG",
                "name": "STRING",
                "active": "BOOLEAN",
                "display_order": "LONG",
                "device_display": "BOOLEAN",
                "forecast_order": "LONG",
                "completed_email": "STRING",
                "office_ids": "STRING",
                "billing_contract_type_ids": "STRING",
                "offices": "STRING",
                "billing_contract_types": "STRING",
                "secondary_service": "STRING",
                "recurring_workorder": "STRING",
                "standard_billing": "STRING",
                "work_type_id": "LONG",
                "work_type": "STRING",
                "department_type_id": "LONG",
                "department_type": "STRING",
                "printing_options": "STRING",
                "auto_send_confirmation_email": "STRING",
                "custom_field1": "STRING",
                "auto_confirm": "STRING",
                "etag": "STRING"
            }
        },
        {
            "table": "floorplans",
            "primary_key": ["id"],
            "columns": {
                "id": "LONG",
                "name": "STRING",
                "community": "STRING",
                "customer": "STRING",
                "city": "STRING",
                "customer_id": "LONG",
                "customer_external_id": "STRING",
                "community_external_id": "STRING",
                "active_date": "STRING",
                "inactive_date": "STRING",
                "bid_number": "STRING",
                "square_footage": "FLOAT",
                "labor_hours": "FLOAT",
                "contract_price": "FLOAT",
                "work_unit": "STRING",
                "etag": "STRING",
                "extended_labor_costs": "STRING"
            }
        },
        {
            "table": "customer_pricings",
            "primary_key": ["id"],
            "columns": {
                "id": "LONG",
                "description": "STRING",
                "sku": "STRING",
                "item_number": "STRING",
                "price": "FLOAT",
                "labor": "FLOAT",
                "active_date": "STRING",
                "inactive_date": "STRING",
                "favorite": "BOOLEAN",
                "print_on_all_contractuals": "BOOLEAN",
                "city_id": "LONG",
                "city": "STRING",
                "customer_id": "LONG",
                "customer": "STRING",
                "community_id": "LONG",
                "community": "STRING",
                "floorplan_id": "LONG",
                "floorplan": "STRING",
                "retail_price": "FLOAT",
                "etag": "STRING",
                "extended_labor_costs": "STRING"
            }
        },
        {
            "table": "contracts",
            "primary_key": ["id"],
            "columns": {
                "id": "LONG",
                "number": "LONG",
                "status": "STRING",
                "category": "STRING",
                "po_number": "STRING",
                "notes": "STRING",
                "description": "STRING",
                "work_order_id": "LONG",
                "assigned_to": "STRING",
                "pricing_mode": "STRING",
                "job": "STRING",
                "total": "FLOAT",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
                "etag": "STRING"
            }
        },
        {
            "table": "invoices",
            "primary_key": ["id"],
            "columns": {
                "id": "LONG",
                "work_order_id": "LONG",
                "accounting_id": "STRING",
                "job_id": "LONG",
                "customer_id": "LONG",
                "status": "STRING",
                "object": "STRING",
                "extras": "STRING",
                "total": "FLOAT",
                "created_by": "STRING",
                "created_by_id": "LONG",
                "number": "STRING",
                "summary": "STRING",
                "contract_details": "STRING",
                "last_error": "STRING",
                "job_ids": "STRING",
                "work_order_ids": "STRING",
                "po_numbers": "STRING",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
                "work_order_type": "STRING",
                "etag": "STRING"
            }
        },
        {
            "table": "builder_orders",
            "primary_key": ["id"],
            "columns": {
                "id": "LONG",
                "order_type": "STRING",
                "stage": "LONG",
                "stage_description": "STRING",
                "work_order_id": "LONG",
                "work_order_type_id": "LONG",
                "start_date": "STRING",
                "job_address": "STRING",
                "floorplan_name": "STRING",
                "city_name": "STRING",
                "community_name": "STRING",
                "customer_name": "STRING",
                "notes": "STRING",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
                "order_number": "STRING",
                "total": "FLOAT",
                "task_name": "STRING",
                "type": "STRING",
                "accepted_at": "STRING",
                "task_descriptions": "STRING",
                "item_details": "STRING"
            }
        },
        {
            "table": "job_events",
            "primary_key": ["event_id"],
            "columns": {
                "event_id": "STRING",
                "id": "LONG",
                "event": "STRING",
                "author": "STRING",
                "created_at": "STRING",
                "changes": "STRING",
                # Job fields with job_ prefix
                "job_id": "LONG",
                "job_lot": "STRING",
                "job_zip": "STRING",
                "job_city": "STRING",
                "job_etag": "STRING",
                "job_block": "STRING",
                "job_notes": "STRING",
                "job_active": "BOOLEAN",
                "job_office": "STRING",
                "job_address": "STRING",
                "job_customer": "STRING",
                "job_community": "STRING",
                "job_floorplan": "STRING",
                "job_created_at": "UTC_DATETIME",
                "job_home_owner": "STRING",
                "job_start_date": "STRING",
                "job_updated_at": "UTC_DATETIME",
                "job_customer_id": "LONG",
                "job_job_type_id": "LONG",
                "job_permit_date": "STRING",
                "job_floorplan_id": "LONG",
                "job_municipality": "STRING",
                "job_permit_number": "STRING",
                "job_under_warranty": "BOOLEAN",
                "job_home_owner_email": "STRING",
                "job_home_owner_phone": "STRING",
                "job_accounting_number": "STRING",
                "job_builder_job_number": "STRING",
                "job_customer_external_id": "STRING",
                "job_community_external_id": "STRING"
            }
        },
        {
            "table": "work_order_events",
            "primary_key": ["event_id"],
            "columns": {
                "event_id": "STRING",
                "event": "STRING",
                "created_at": "STRING",
                "author": "STRING",
                "changes": "STRING",
                # Work order fields with work_order_ prefix
                "work_order_id": "LONG",
                "work_order_etag": "STRING",
                "work_order_crews": "STRING",
                "work_order_notes": "STRING",
                "work_order_stage": "STRING",
                "work_order_crew_ids": "STRING",
                "work_order_created_at": "UTC_DATETIME",
                "work_order_crew_notes": "STRING",
                "work_order_home_owner": "STRING",
                "work_order_job_number": "LONG",
                "work_order_updated_at": "UTC_DATETIME",
                "work_order_labor_total": "FLOAT",
                "work_order_price_total": "FLOAT",
                "work_order_supervisor_id": "LONG",
                "work_order_completed_date": "STRING",
                "work_order_reschedule_date": "STRING",
                "work_order_work_order_type": "STRING",
                "work_order_home_owner_email": "STRING",
                "work_order_home_owner_phone": "STRING",
                "work_order_job_extras_charges": "STRING",
                "work_order_work_order_type_id": "LONG",
                "work_order_appointment_end_time": "STRING",
                "work_order_appointment_start_time": "STRING",
                "work_order_floorplan_labor_charge": "FLOAT",
                "work_order_schedule_change_reason": "STRING",
                "work_order_schedule_change_comment": "STRING",
                "work_order_crew_pay_extra_added_pay": "FLOAT",
                "work_order_percentage_of_completion": "LONG",
                "work_order_estimated_completion_date": "STRING",
                "work_order_floorplan_contract_charge": "FLOAT",
                "work_order_floorplan_options_charges": "STRING"
            }
        },
        {
            "table": "types",
            "primary_key": ["id"],
            "columns": {
                "id": "LONG",
                "description": "STRING",
                "web_display": "BOOLEAN",
                "display_order": "LONG",
                "category": "STRING",
                "device_display": "BOOLEAN",
                "work_order_type_id": "LONG",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
                "etag": "STRING"
            }
        },
        {
            "table": "cities",
            "primary_key": ["id"],
            "columns": {
                "id": "LONG",
                "name": "STRING",
                "state": "STRING",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
                "etag": "STRING"
            }
        },
        {
            "table": "offices",
            "primary_key": ["id"],
            "columns": {
                "id": "LONG",
                "name": "STRING",
                "address": "STRING",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
                "etag": "STRING"
            }
        },
        {
            "table": "schedules",
            "primary_key": ["id"],
            "columns": {
                "id": "LONG",
                "work_order_type": "STRING",
                "work_order_type_id": "LONG",
                "job_number": "LONG",
                "supervisor_id": "LONG",
                "reschedule_date": "STRING",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
                "notes": "STRING",
                "stage": "STRING",
                "crew_notes": "STRING",
                "home_owner": "STRING",
                "home_owner_email": "STRING",
                "home_owner_phone": "STRING",
                "completed_date": "STRING",
                "estimated_completion_date": "STRING",
                "percentage_of_completion": "LONG",
                "crews": "STRING",
                "crew_ids": "STRING",
                "floorplan_options_charges": "STRING",
                "job_extras_charges": "STRING",
                "labor_total": "FLOAT",
                "price_total": "FLOAT",
                "floorplan_contract_charge": "FLOAT",
                "floorplan_labor_charge": "FLOAT",
                "crew_pay_extra_added_pay": "FLOAT",
                "appointment_start_time": "STRING",
                "appointment_end_time": "STRING",
                "schedule_change_comment": "STRING",
                "schedule_change_reason": "STRING",
                "warranty_issue_id": "LONG",
                "warranty_issue": "STRING",
                "department_type": "STRING",
                "etag": "STRING"
            }
        },
        {
            "table": "takeoff_types",
            "primary_key": ["id"],
            "columns": {
                "id": "LONG",
                "work_order_type": "STRING",
                "work_order_type_id": "LONG",
                "description": "STRING",
                "allow_notes": "BOOLEAN",
                "device_display": "BOOLEAN",
                "transpose": "BOOLEAN",
                "print_landscape": "BOOLEAN",
                "print_differential_upon_submit": "BOOLEAN",
                "display_order": "LONG",
                "active": "BOOLEAN",
                "takeoff_columns": "STRING",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
                "etag": "STRING"
            }
        },
        {
            "table": "work_order_status_events",
            "primary_key": ["event_id"],
            "columns": {
                "event_id": "STRING",
                "id": "LONG",
                "event": "STRING",
                "author": "STRING",
                "created_at": "STRING",
                "changes": "STRING",
                # Status fields with status_ prefix
                "status_id": "LONG",
                "status_status": "BOOLEAN",
                "status_updated_at": "STRING",
                "status_description": "STRING",
                "status_work_order_id": "LONG",
                "status_work_order_status_type_id": "LONG"
            }
        }
    ]
	
# Create the connector object
connector = Connector(update=update, schema=schema)

# Debug entry point for local testing
if __name__ == "__main__":
    log.info("Running Bolt ECI connector in debug mode")
    connector.debug()