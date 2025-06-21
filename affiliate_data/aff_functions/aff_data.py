import requests
import pandas as pd
import ast
from pandas import json_normalize
from datetime import datetime
import calendar
import time

def generate_monthly_date_ranges(start_date_str, end_date_str):
    """Generate monthly date ranges from start to end date"""
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    current_date = start_date.replace(day=1)
    ranges = []
    
    while current_date <= end_date:
        year = current_date.year
        month = current_date.month
        last_day = calendar.monthrange(year, month)[1]
        month_start = current_date
        month_end = current_date.replace(day=last_day)
        
        # Adjust month_end if it goes beyond end_date
        if month_end > end_date:
            month_end = end_date
        
        ranges.append((
            month_start.strftime("%Y-%m-%dT00:00:00.000+03:00"), 
            month_end.strftime("%Y-%m-%dT23:59:59.000+03:00"),
            f"{year}-{month:02d}"  # Add year-month identifier
        ))
        
        # Move to next month
        if month == 12:
            current_date = current_date.replace(year=year+1, month=1)
        else:
            current_date = current_date.replace(month=month+1)
    
    return ranges

def fetch_month_data(cookie, start_date, end_date, month_id):
    """Fetch data for a single month"""
    url = "https://admin-affiliate.quadcode.com/api/admin/reports/roi"
    headers = {
        "accept": "application/json, text/plain, */*",
        "cookie": cookie
    }
    
    all_data = []
    skip = 0
    batch_size = 500  # Maximum allowed by API
    
    # print(f"Fetching data for {month_id}...")
    
    while True:
        params = {
            "first": batch_size,
            "skip": skip,
            "group_by": "aff",
            "order": "desc", 
            "sort": "aff_id",
            "period_date_start": start_date,
            "period_date_end": end_date
        }
        
        try:
            response = requests.get(url, headers=headers, params=params)
            
            if response.status_code == 400:
                # Remove skip parameter and try with just first
                params.pop('skip', None)
                response = requests.get(url, headers=headers, params=params)
            
            response.raise_for_status()
            batch_data = response.json()
            
            # Handle different response formats
            if isinstance(batch_data, dict):
                if 'data' in batch_data:
                    records = batch_data['data']
                    total_count = batch_data.get('totalCount', 0)
                else:
                    records = [batch_data]
            else:
                records = batch_data if isinstance(batch_data, list) else []
            
            if not records:
                break
                
            # Add month identifier to each record
            for record in records:
                record['month_year'] = month_id
                
            all_data.extend(records)
            
            # If we got fewer records than requested, we've reached the end
            if len(records) < batch_size:
                break
                
            # If skip parameter doesn't work, we can only get one batch
            if 'skip' not in params:
                break
                
            skip += batch_size
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {month_id}: {e}")
            break
    
    print(f"Fetched {len(all_data)} records for {month_id}")
    return all_data

def fetch_all_months_data(cookie, start_date_str="2024-10-01", end_date_str=None):
    """Fetch data for all months from start_date to end_date"""
    
    if end_date_str is None:
        end_date_str = datetime.now().strftime("%Y-%m-%d")
    
    # print(f"Fetching data from {start_date_str} to {end_date_str}")
    
    # Generate monthly ranges
    monthly_ranges = generate_monthly_date_ranges(start_date_str, end_date_str)
    # print(f"Will fetch data for {len(monthly_ranges)} months")
    
    all_data = []
    
    for start_date, end_date, month_id in monthly_ranges:
        month_data = fetch_month_data(cookie, start_date, end_date, month_id)
        all_data.extend(month_data)
        time.sleep(5)
    
    print(f"\nTotal records fetched across all months: {len(all_data)}")
    return all_data

def parse_quadcode_data(raw_data):
    """Parse the nested JSON strings in the data column"""
    
    parsed_records = []
    
    for item in raw_data:
        if isinstance(item, dict) and 'data' in item:
            # Parse the data string into a dictionary
            try:
                if isinstance(item['data'], str):
                    parsed_data = ast.literal_eval(item['data'])
                else:
                    parsed_data = item['data']
                
                # Add month_year from top level if available
                if 'month_year' in item:
                    parsed_data['month_year'] = item['month_year']
                
                parsed_records.append(parsed_data)
                
            except (ValueError, SyntaxError) as e:
                print(f"Error parsing record: {e}")
                continue
        else:
            parsed_records.append(item)
    
    return parsed_records

def clean_quadcode_dataframe(df):
    """Clean and structure the normalized DataFrame to include all required columns + month/year"""
    
    df_clean = df.copy()

    # Create mapping of original columns to your required column names
    column_mapping = {
        'group_by.aff_id': 'Affiliate',
        'hits': 'Hits',
        'uniques': 'Uniques',
        'installs': 'Installs',
        'registrations': 'Registrations',
        'ftd': 'FTD',
        'pnl.amount': 'PNL',
        'inout.amount': 'In-Out',
        'our_profit.amount': 'Our Profit',
        'aff_profit.amount': 'Aff Profit',
        'roi': 'ROI',
        'roas': 'ROAS',
        'clicks': 'Clicks',
        'deposits_count': 'Deposit count',
        'deposits_amount.amount': 'Deposit sum',
        'month_year': 'Month'  # Add month column
    }
    
    # Create the final DataFrame with all required columns
    required_columns = []
    rename_mapping = {}
    
    for original_col, new_col in column_mapping.items():
        if original_col in df_clean.columns:
            required_columns.append(original_col)
            rename_mapping[original_col] = new_col
    
    # Check if we have all required columns
    final_df = df_clean[required_columns].rename(columns=rename_mapping)
    
    # Convert currency amount columns to float
    currency_columns = ['PNL', 'In-Out', 'Our Profit', 'Aff Profit', 'Deposit sum']
    for col in currency_columns:
        if col in final_df.columns:
            final_df[col] = final_df[col].astype(str).str.replace(',', '').astype(float)
    
    # Ensure the column order matches your requirements (with Month at the end)
    desired_order = [
        'Month', 'Affiliate', 'Hits', 'Uniques', 'Installs', 'Registrations', 'FTD',
        'PNL', 'In-Out', 'Our Profit', 'Aff Profit', 'ROI', 'ROAS',
        'Clicks', 'Deposit count', 'Deposit sum'
    ]
    
    # Only include columns that exist
    available_columns = [col for col in desired_order if col in final_df.columns]
    final_df = final_df[available_columns]
    
    return final_df