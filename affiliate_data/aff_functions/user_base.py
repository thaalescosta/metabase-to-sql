import requests
import pandas as pd
import time
import ast
from pandas import json_normalize

def fetch_affiliate_table(cookie):
    """Fetch data for a single month"""
    url = "https://admin-affiliate.quadcode.com/api/admin/reports/users"
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
            "sort": "id",
            "order": "desc"
        }
        
        try:
            response = requests.get(url, headers=headers, params=params)
            print(response)
            time.sleep(5)
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
                    # total_count = batch_data.get('totalCount', 0)
                else:
                    records = [batch_data]
            else:
                records = batch_data if isinstance(batch_data, list) else []
            
            if not records:
                break
                
            all_data.extend(records)
            
            # If we got fewer records than requested, we've reached the end
            if len(records) < batch_size:
                break
                
            # If skip parameter doesn't work, we can only get one batch
            if 'skip' not in params:
                break
                
            skip += batch_size
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            break
    
    print(f"Fetched {len(all_data)} records")
    return all_data

def parse_user_data(raw_data):
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
                
                parsed_records.append(parsed_data)
                
            except (ValueError, SyntaxError) as e:
                print(f"Error parsing record: {e}")
                continue
        else:
            parsed_records.append(item)
    
    return parsed_records

def clean_dataframe(df):
    """Clean and structure the normalized DataFrame to include all required columns + month/year"""
    
    df_clean = df.copy()
    
    column_mapping = {
      "aff_segment": "aff_segment",
      "country_code": "country_code",
      "created_at": "created_at",
      "email": "email",
      "first_name": "first_name",
      "id": "id",
      "last_name": "last_name",
      "legal_type": "legal_type",
      "manager_id": "manager_id",
      "phone": "phone",
      "preferable_locale": "preferable_locale",
      "program": "program",
      "site_id": "site_id",
      "supported_language": "supported_language",
      "ui_language": "ui_language",
      "user_type": "user_type"
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
    
    return final_df
