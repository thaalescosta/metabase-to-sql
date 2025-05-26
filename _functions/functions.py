import os, json, time, requests, io
import pandas as pd
from io import StringIO
from tqdm import tqdm
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from bs4 import BeautifulSoup
from abc import ABC, abstractmethod


# Configurações de conexão
user = "root"
password = "root"
host = "localhost"
database = "polarium"

# Cria a engine do SQLAlchemy com connection pooling
engine = create_engine(
    f"mysql+mysqlconnector://{user}:{password}@{host}/{database}",
    pool_size=5,
    max_overflow=10,
    pool_timeout=30,
    pool_recycle=1800
)
# Define report configurations
reports = {
    'deposit_reports': {
        'name': 'deposits',
        'url': 'https://bi.quadcode.com/api/dashboard/63/dashcard/277/card/199/query/csv',
        'body_template': r'''parameters=%5B%7B%22type%22%3A%22string%2F%3D%22%2C%22value%22%3Anull%2C%22id%22%3A%229a620e47%22%2C%22target%22%3A%5B%22dimension%22%2C%5B%22field%22%2C%22Transaction+platform+name%22%2C%7B%22base-type%22%3A%22type%2FText%22%7D%5D%2C%7B%22stage-number%22%3A0%7D%5D%7D%2C%7B%22type%22%3A%22number%2F%3D%22%2C%22value%22%3Anull%2C%22id%22%3A%22f85ecdf4%22%2C%22target%22%3A%5B%22dimension%22%2C%5B%22field%22%2C%22User+ID%22%2C%7B%22base-type%22%3A%22type%2FBigInteger%22%7D%5D%2C%7B%22stage-number%22%3A0%7D%5D%7D%2C%7B%22type%22%3A%22date%2Fall-options%22%2C%22value%22%3A"{date_range}"%2C%22id%22%3A%22b6927d0e%22%2C%22target%22%3A%5B%22dimension%22%2C%5B%22field%22%2C%22Transaction+created%22%2C%7B%22base-type%22%3A%22type%2FDateTime%22%7D%5D%2C%7B%22stage-number%22%3A0%7D%5D%7D%2C%7B%22type%22%3A%22number%2F%3D%22%2C%22value%22%3Anull%2C%22id%22%3A%2282378901%22%2C%22target%22%3A%5B%22dimension%22%2C%5B%22field%22%2C%22Transaction+ID%22%2C%7B%22base-type%22%3A%22type%2FBigInteger%22%7D%5D%2C%7B%22stage-number%22%3A0%7D%5D%7D%2C%7B%22type%22%3A%22string%2F%3D%22%2C%22value%22%3Anull%2C%22id%22%3A%22189a7553%22%2C%22target%22%3A%5B%22dimension%22%2C%5B%22field%22%2C%22Transaction+type%22%2C%7B%22base-type%22%3A%22type%2FText%22%7D%5D%2C%7B%22stage-number%22%3A0%7D%5D%7D%2C%7B%22type%22%3A%22string%2F%3D%22%2C%22value%22%3Anull%2C%22id%22%3A%22e3add5a%22%2C%22target%22%3A%5B%22dimension%22%2C%5B%22field%22%2C%22Payment+method%22%2C%7B%22base-type%22%3A%22type%2FText%22%7D%5D%2C%7B%22stage-number%22%3A0%7D%5D%7D%2C%7B%22type%22%3A%22string%2F%3D%22%2C%22value%22%3Anull%2C%22id%22%3A%223c62a8f7%22%2C%22target%22%3A%5B%22dimension%22%2C%5B%22field%22%2C%22Aff+model%22%2C%7B%22base-type%22%3A%22type%2FText%22%7D%5D%2C%7B%22stage-number%22%3A0%7D%5D%7D%2C%7B%22type%22%3A%22string%2F%3D%22%2C%22value%22%3Anull%2C%22id%22%3A%22b41b0856%22%2C%22target%22%3A%5B%22dimension%22%2C%5B%22field%22%2C%22Country+name%22%2C%7B%22base-type%22%3A%22type%2FText%22%7D%5D%2C%7B%22stage-number%22%3A0%7D%5D%7D%2C%7B%22type%22%3A%22date%2Fall-options%22%2C%22value%22%3Anull%2C%22id%22%3A%22904cbd9e%22%2C%22target%22%3A%5B%22dimension%22%2C%5B%22field%22%2C%22Registration+date%22%2C%7B%22base-type%22%3A%22type%2FDateTime%22%7D%5D%2C%7B%22stage-number%22%3A0%7D%5D%7D%2C%7B%22type%22%3A%22string%2F%3D%22%2C%22value%22%3Anull%2C%22id%22%3A%22f3c1f635%22%2C%22target%22%3A%5B%22dimension%22%2C%5B%22field%22%2C%22Paygate%22%2C%7B%22base-type%22%3A%22type%2FText%22%7D%5D%2C%7B%22stage-number%22%3A0%7D%5D%7D%2C%7B%22type%22%3A%22number%2F%3D%22%2C%22value%22%3Anull%2C%22id%22%3A%2274ad39c5%22%2C%22target%22%3A%5B%22dimension%22%2C%5B%22field%22%2C%22Aff+ID%22%2C%7B%22base-type%22%3A%22type%2FBigInteger%22%7D%5D%2C%7B%22stage-number%22%3A0%7D%5D%7D%2C%7B%22type%22%3A%22string%2F%3D%22%2C%22value%22%3Anull%2C%22id%22%3A%229e93279e%22%2C%22target%22%3A%5B%22dimension%22%2C%5B%22field%22%2C%22Traffic+group%22%2C%7B%22base-type%22%3A%22type%2FText%22%7D%5D%2C%7B%22stage-number%22%3A0%7D%5D%7D%2C%7B%22type%22%3A%22string%2F%3D%22%2C%22value%22%3Anull%2C%22id%22%3A%229072959b%22%2C%22target%22%3A%5B%22dimension%22%2C%5B%22field%22%2C%22Registration+platform+group%22%2C%7B%22base-type%22%3A%22type%2FText%22%7D%5D%2C%7B%22stage-number%22%3A0%7D%5D%7D%2C%7B%22type%22%3A%22string%2F%3D%22%2C%22value%22%3Anull%2C%22id%22%3A%222bf2c266%22%2C%22target%22%3A%5B%22dimension%22%2C%5B%22field%22%2C%22Registration+platform+name%22%2C%7B%22base-type%22%3A%22type%2FText%22%7D%5D%2C%7B%22stage-number%22%3A0%7D%5D%7D%5D&format_rows=false&pivot_results=false''',
    },
    'trading_reports': {
        'name': 'trading',
        'url': 'https://bi.quadcode.com/api/dashboard/65/dashcard/280/card/201/query/csv',
        'body_template': r'''parameters=%5B%7B%22type%22%3A%22string%2F%3D%22%2C%22value%22%3Anull%2C%22id%22%3A%22f071ae69%22%2C%22target%22%3A%5B%22dimension%22%2C%5B%22field%22%2C%22Registration+platform+group%22%2C%7B%22base-type%22%3A%22type%2FText%22%7D%5D%2C%7B%22stage-number%22%3A0%7D%5D%7D%2C%7B%22type%22%3A%22date%2Fall-options%22%2C%22value%22%3A"{date_range}"%2C%22id%22%3A%228c2a75e4%22%2C%22target%22%3A%5B%22dimension%22%2C%5B%22field%22%2C%22Position+created%22%2C%7B%22base-type%22%3A%22type%2FDateTime%22%7D%5D%2C%7B%22stage-number%22%3A0%7D%5D%7D%2C%7B%22type%22%3A%22string%2F%3D%22%2C%22value%22%3Anull%2C%22id%22%3A%22335b8d9d%22%2C%22target%22%3A%5B%22dimension%22%2C%5B%22field%22%2C%22Instrument+type%22%2C%7B%22base-type%22%3A%22type%2FText%22%7D%5D%2C%7B%22stage-number%22%3A0%7D%5D%7D%2C%7B%22type%22%3A%22number%2F%3D%22%2C%22value%22%3Anull%2C%22id%22%3A%22f2ea0ae1%22%2C%22target%22%3A%5B%22dimension%22%2C%5B%22field%22%2C%22User+ID%22%2C%7B%22base-type%22%3A%22type%2FBigInteger%22%7D%5D%2C%7B%22stage-number%22%3A0%7D%5D%7D%2C%7B%22type%22%3A%22string%2F%3D%22%2C%22value%22%3Anull%2C%22id%22%3A%2239bd4ef9%22%2C%22target%22%3A%5B%22dimension%22%2C%5B%22field%22%2C%22Registration+platform+name%22%2C%7B%22base-type%22%3A%22type%2FText%22%7D%5D%2C%7B%22stage-number%22%3A0%7D%5D%7D%2C%7B%22type%22%3A%22number%2F%3D%22%2C%22value%22%3Anull%2C%22id%22%3A%226b46e3e0%22%2C%22target%22%3A%5B%22dimension%22%2C%5B%22field%22%2C%22Position+ID%22%2C%7B%22base-type%22%3A%22type%2FBigInteger%22%7D%5D%2C%7B%22stage-number%22%3A0%7D%5D%7D%2C%7B%22type%22%3A%22string%2F%3D%22%2C%22value%22%3Anull%2C%22id%22%3A%22ee0ebf76%22%2C%22target%22%3A%5B%22dimension%22%2C%5B%22field%22%2C%22Close+reason%22%2C%7B%22base-type%22%3A%22type%2FText%22%7D%5D%2C%7B%22stage-number%22%3A0%7D%5D%7D%2C%7B%22type%22%3A%22string%2F%3D%22%2C%22value%22%3Anull%2C%22id%22%3A%22a5ae4e6b%22%2C%22target%22%3A%5B%22dimension%22%2C%5B%22field%22%2C%22Asset%22%2C%7B%22base-type%22%3A%22type%2FText%22%7D%5D%2C%7B%22stage-number%22%3A0%7D%5D%7D%2C%7B%22type%22%3A%22string%2F%3D%22%2C%22value%22%3Anull%2C%22id%22%3A%221f903f5f%22%2C%22target%22%3A%5B%22dimension%22%2C%5B%22field%22%2C%22Trading+platform+group%22%2C%7B%22base-type%22%3A%22type%2FText%22%7D%5D%2C%7B%22stage-number%22%3A0%7D%5D%7D%2C%7B%22type%22%3A%22string%2F%3D%22%2C%22value%22%3Anull%2C%22id%22%3A%22c461df97%22%2C%22target%22%3A%5B%22dimension%22%2C%5B%22field%22%2C%22Trading+platform+name%22%2C%7B%22base-type%22%3A%22type%2FText%22%7D%5D%2C%7B%22stage-number%22%3A0%7D%5D%7D%2C%7B%22type%22%3A%22string%2F%3D%22%2C%22value%22%3Anull%2C%22id%22%3A%226145c6e%22%2C%22target%22%3A%5B%22dimension%22%2C%5B%22field%22%2C%22Traffic+group%22%2C%7B%22base-type%22%3A%22type%2FText%22%7D%5D%2C%7B%22stage-number%22%3A0%7D%5D%7D%2C%7B%22type%22%3A%22date%2Fall-options%22%2C%22value%22%3Anull%2C%22id%22%3A%22839e5519%22%2C%22target%22%3A%5B%22dimension%22%2C%5B%22field%22%2C%22Position+closed%22%2C%7B%22base-type%22%3A%22type%2FDateTime%22%7D%5D%2C%7B%22stage-number%22%3A0%7D%5D%7D%2C%7B%22type%22%3A%22number%2F%3D%22%2C%22value%22%3Anull%2C%22id%22%3A%22844ed90a%22%2C%22target%22%3A%5B%22dimension%22%2C%5B%22field%22%2C%22Aff+ID%22%2C%7B%22base-type%22%3A%22type%2FBigInteger%22%7D%5D%2C%7B%22stage-number%22%3A0%7D%5D%7D%2C%7B%22type%22%3A%22string%2F%3D%22%2C%22value%22%3Anull%2C%22id%22%3A%22d0cdf664%22%2C%22target%22%3A%5B%22dimension%22%2C%5B%22field%22%2C%22Aff+model%22%2C%7B%22base-type%22%3A%22type%2FText%22%7D%5D%2C%7B%22stage-number%22%3A0%7D%5D%7D%2C%7B%22type%22%3A%22string%2F%3D%22%2C%22value%22%3Anull%2C%22id%22%3A%22f24cfe19%22%2C%22target%22%3A%5B%22dimension%22%2C%5B%22field%22%2C%22Country+name%22%2C%7B%22base-type%22%3A%22type%2FText%22%7D%5D%2C%7B%22stage-number%22%3A0%7D%5D%7D%5D&format_rows=false&pivot_results=false''',
    }
}

def load_credentials(service_name, credentials_file="credentials.json"):
    """Load credentials from JSON file"""
    try:
        # Get the directory where the script is located
        script_dir = os.path.dirname(os.path.abspath(__file__))
        credentials_path = os.path.join(script_dir, credentials_file)
        
        with open(credentials_path, 'r') as f:
            credentials = json.load(f)
        
        return credentials.get(service_name)
    except FileNotFoundError:
        print(f"Credentials file '{credentials_file}' not found")
        return None
    except Exception as e:
        print(f"Error loading credentials: {e}")
        return None

class DataFetcher(ABC):
    def __init__(self, max_retries=3, retry_delay=5, chunk_size_days=30):
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.session = requests.Session()
        self.chunk_size_days = chunk_size_days

    def get_date_chunks(self, start_date, end_date):
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
        
        chunks = []
        current_start = start_date
        
        while current_start < end_date:
            current_end = min(current_start + timedelta(days=self.chunk_size_days - 1), end_date)
            chunks.append((current_start, current_end))
            current_start = current_end + timedelta(days=1)
        
        return chunks
    
    def download_with_retry(self, fetch_func, *args, **kwargs):
        """Generic retry mechanism for downloads"""
        for attempt in range(self.max_retries):
            try:
                result = fetch_func(*args, **kwargs)
                if result is not None:
                    return result
                time.sleep(self.retry_delay)
            except Exception as e:
                if attempt == self.max_retries - 1:
                    print(f"Error after {self.max_retries} attempts: {str(e)[:100]}...")
                    return None
                time.sleep(self.retry_delay)
        return None
    
    @abstractmethod
    def fetch_data(self, start_date, end_date):
        """Abstract method to be implemented by specific fetchers"""
        pass

class QuadCodeReportFetcher(DataFetcher):
    def __init__(self, report_config):
        # Use smaller chunks for trading reports (5 days) and larger for deposits (30 days)
        chunk_size = 7
        super().__init__(chunk_size_days=chunk_size)
        self.report_config = report_config
        self.session_id = None
    
    def authenticate(self):
        """Get session ID for QuadCode BI"""
        url = "https://bi.quadcode.com/api/session"
        
        # Load credentials from JSON file
        creds = load_credentials("quadcode_bi")
        if not creds:
            raise ValueError("Failed to load QuadCode BI credentials")
        
        payload = {
            "username": creds["username"],
            "password": creds["password"],
            "remember": True
        }
        
        headers = {
            "accept": "application/json",
            "content-type": "application/json"
        }
        
        response = self.session.post(url, json=payload, headers=headers)
        response.raise_for_status()
        self.session_id = response.json().get('id') or response.json().get('session_id') or response.json().get('token')
        return self.session_id
    
    def fetch_chunk(self, start_date, end_date):
        """Fetch a single chunk of report data"""
        if not self.session_id:
            self.authenticate()
        
        url = self.report_config['url']
        
        # Format dates properly for Metabase
        start_formatted = start_date.strftime("%Y-%m-%d")
        end_formatted = end_date.strftime("%Y-%m-%d")
        
        # Use the correct Metabase date range format
        date_range = f"{start_formatted}~{end_formatted}"
        
        body = self.report_config['body_template'].replace("{date_range}", date_range)
        
        headers = {
            "accept": "application/json",
            "content-type": "application/x-www-form-urlencoded;charset=UTF-8",
            "cookie": f"metabase.DEVICE=080a5e36-919c-4d58-9f79-7717001b7f35; metabase.TIMEOUT=alive; metabase.SESSION={self.session_id}"
        }
        
        response = self.session.post(url, headers=headers, data=body)
        response.raise_for_status()
        # print(f'Response: {response.text}')
        return response.text
    
    def process_chunk(self, raw_data):
        """Process raw data into DataFrame"""
        if not raw_data or len(raw_data.strip()) == 0:
            return None
            
        try:
            df = pd.read_csv(
                StringIO(raw_data),
                dtype=str,
                na_values=['', 'NULL', 'null', 'None'],
                keep_default_na=False,
                encoding='utf-8',
                on_bad_lines='skip'
            )
            
            if len(df.columns) == 0:
                return None
            
            df.columns = [str(col).strip().lower().replace(" ", "_") for col in df.columns]
            df = df.dropna(how='all').reset_index(drop=True)
            return df
            
        except Exception as e:
            print(f"Error processing chunk: {str(e)[:100]}...")
            return None
    
    def fetch_data(self, start_date, end_date):
        """Fetch complete report data"""
        if not self.session_id:
            self.authenticate()
        chunks = self.get_date_chunks(start_date, end_date)
        print(f"\nFetching {self.report_config['name']} in {len(chunks)} chunks")
        
        all_data = []
        stats = {'total_chunks': len(chunks), 'data_chunks': 0, 'empty_chunks': 0, 'failed_chunks': 0}
        
        for i, (chunk_start, chunk_end) in enumerate(tqdm(chunks, desc="Downloading")):
            if i > 0 and i % 10 == 0:
                self.authenticate()
            
            raw_data = self.download_with_retry(self.fetch_chunk, chunk_start, chunk_end)
            if raw_data:
                df = self.process_chunk(raw_data)
                if df is not None and len(df) > 0:
                    all_data.append(df)
                    stats['data_chunks'] += 1
                else:
                    stats['empty_chunks'] += 1
            else:
                stats['failed_chunks'] += 1
            
            time.sleep(1)
        
        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            final_df = final_df.drop_duplicates()
            print(f"Final dataset: {len(final_df):,} records")
            return final_df
        
        print("📭 No data retrieved")
        return pd.DataFrame()

class QuadCodeUsersFetcher(DataFetcher):
    def __init__(self):
        super().__init__(chunk_size_days=30)
        self.cookies = None
        self.field_names = [
            'userId', 'brand', 'email', 'phone', 'affId', 'affModel', 'affTrack',
            'platform', 'firstName', 'lastName', 'registered', 'country', 'currency',
            'balance', 'trainingCurrency', 'trainingBalance', 'age', 'gender', 'city',
            'pnl', 'deposits', 'withdrawals', 'depositsCount', 'androidAdvertisingId', 'idfa'
        ]
    
    def authenticate(self):
        """Login to QuadCode admin platform"""
        # Load credentials from JSON file
        creds = load_credentials("quadcode_admin")
        if not creds:
            raise ValueError("Failed to load QuadCode admin credentials")
        
        email = creds["username"]
        password = creds["password"]
        
        login_page_url = "https://admin-platform.quadcode.com/login"
        login_page_resp = self.session.get(login_page_url)
        
        soup = BeautifulSoup(login_page_resp.text, "html.parser")
        token = None
        
        token_input = soup.find("input", {"name": "token"})
        if token_input:
            token = token_input.get("value")
        
        if not token:
            for script in soup.find_all("script"):
                if script.string and "token" in script.string:
                    import re
                    match = re.search(r'token["\']?\s*:\s*["\']([^"\']+)', script.string)
                    if match:
                        token = match.group(1)
                        break
        
        if not token:
            raise ValueError("Token not found in login page")
        
        login_url = "https://admin-platform.quadcode.com/login"
        login_payload = {
            "email": email,
            "password": password,
            "token": token
        }
        
        login_headers = {
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "*/*"
        }
        
        self.session.post(login_url, data=login_payload, headers=login_headers)
        
        url = "https://reports.quadcode.com/users"
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
            "Referer": "https://admin-platform.quadcode.com/",
            "Sec-Fetch-Site": "same-site",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-User": "?1",
            "Sec-Fetch-Dest": "document",
            "Upgrade-Insecure-Requests": "1"
        }
        
        self.session.get(url, headers=headers, allow_redirects=True)
        cookies = self.session.cookies.get_dict()
        self.cookies = {
            'acc_tkn': cookies.get("acc_tkn"),
            'ref_tkn': cookies.get("ref_tkn")
        }
        return self.cookies
    
    def fetch_chunk(self, start_date, end_date):
        """Fetch a single chunk of user data"""
        if not self.cookies:
            self.authenticate()
        
        url = "https://reports.quadcode.com/api/report/users/csv?sort=registered&order=desc"
        headers = {
            "accept": "*/*",
            "accept-language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "referer": "https://reports.quadcode.com/users"
        }
        
        files = []
        for field in self.field_names:
            files.append(('fields[]', (None, field)))
        
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        files.append(('registered', (None, f"{start_str} - {end_str}")))
        files.append(('investorType', (None, '2')))
        
        response = self.session.post(url, headers=headers, files=files, cookies=self.cookies)
        response.raise_for_status()
        return response.text
    
    def process_chunk(self, raw_data):
        """Process raw user data into DataFrame"""
        try:
            return pd.read_csv(io.StringIO(raw_data))
        except Exception as e:
            print(f"Error processing user chunk: {str(e)[:100]}...")
            return None
    
    def fetch_data(self, start_date, end_date):
        """Fetch complete user data"""
        if not self.cookies:
            self.authenticate()
        
        chunks = self.get_date_chunks(start_date, end_date)
        print(f"\nFetching users in {len(chunks)} chunks")
        
        all_data = []
        for chunk_start, chunk_end in tqdm(chunks, desc="Downloading"):
            raw_data = self.download_with_retry(self.fetch_chunk, chunk_start, chunk_end)
            if raw_data:
                df = self.process_chunk(raw_data)
                if df is not None:
                    all_data.append(df)
            else:
                try:
                    tqdm.write("Token expired, attempting to refresh...")
                    self.authenticate()
                    raw_data = self.fetch_chunk(chunk_start, chunk_end)
                    if raw_data:
                        df = self.process_chunk(raw_data)
                        if df is not None:
                            all_data.append(df)
                except Exception as e:
                    tqdm.write(f"Error fetching data for period {chunk_start} to {chunk_end}: {e}")
                    continue
        
        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            tqdm.write(f"Final dataset contains {len(final_df):,} records".replace(",", "."))
            return final_df
        
        return None

class DataCleaner:
    """Class to handle data cleaning operations for different data types"""
    
    @staticmethod
    def _safe_astype(df, column_types):
        """Safely convert column types, skipping missing columns"""
        existing_columns = {col: dtype for col, dtype in column_types.items() if col in df.columns}
        if existing_columns:
            return df.astype(existing_columns)
        return df
    
    @staticmethod
    def clean_deposit(deposit_df):
        """Clean deposit data"""
        if deposit_df is None or deposit_df.empty:
            return pd.DataFrame()

        # Drop unique_id if it exists
        if 'unique_id' in deposit_df.columns:
            deposit_df = deposit_df.drop(columns=['unique_id'])

        # Clean currency values BEFORE type conversion
        if 'transaction_amount' in deposit_df.columns:
            # Only apply string operations if the column contains strings
            if deposit_df['transaction_amount'].dtype == 'object':
                deposit_df['transaction_amount'] = deposit_df['transaction_amount'].str.replace(
                    "$", "", case=False, regex=False
                )

        # Convert numeric columns AFTER cleaning
        deposit_df = DataCleaner._safe_astype(deposit_df, {'transaction_amount': 'float64'})

        # Convert string columns
        string_columns = [
            'country_name', 'transaction_type', 'paygate', 'payment_method',
            'paysystem', 'transaction_platform_name', 'transaction_platform_group',
            'aff_model', 'traffic_group', 'registration_platform_name',
            'registration_platform_group'
        ]
        deposit_df = DataCleaner._safe_astype(deposit_df, {col: 'string' for col in string_columns})

        # Convert datetime columns
        datetime_columns = [
            'transaction_created', 'transaction_updated', 'registration_date'
        ]
        for col in datetime_columns:
            if col in deposit_df.columns:
                deposit_df[col] = pd.to_datetime(deposit_df[col], utc=True, errors='coerce').dt.tz_localize(None)

        return deposit_df
    
    @staticmethod
    def clean_trading(trading_df):
        """Clean trading data"""
        if trading_df is None or trading_df.empty:
            return pd.DataFrame()
            
        # Drop unique_id if it exists
        if 'unique_id' in trading_df.columns:
            trading_df = trading_df.drop(columns=['unique_id'])
        
        # Clean currency values
        currency_columns = [
            'total_investment', 'investment_real', 'investment_bonus',
            'total_volume', 'volume_real', 'volume_bonus', 'gross_pnl',
            'net_pnl_real', 'pnl_bonus', 'total_result', 'result_real',
            'result_bonus', 'overnight', 'custodial', 'margin_call',
            'dividends'
        ]
        for col in currency_columns:
            if col in trading_df.columns and trading_df[col].dtype == 'object':
                trading_df[col] = trading_df[col].str.replace("$", "", case=False, regex=False)
        
        # Convert string columns
        string_columns = [
            'instrument_type', 'asset', 'instrument_direction',
            'trading_platform_name', 'close_reason', 'trading_platform_group',
            'state'
        ]
        trading_df = DataCleaner._safe_astype(trading_df, {col: 'string' for col in string_columns})
        
        # Convert datetime columns
        datetime_columns = ['position_created', 'position_closed']
        trading_df = DataCleaner._safe_astype(trading_df, {col: 'datetime64[ns]' for col in datetime_columns})
        
        # Convert numeric columns
        numeric_columns = [
            'total_investment', 'investment_real', 'investment_bonus',
            'total_volume', 'volume_real', 'volume_bonus', 'gross_pnl',
            'net_pnl_real', 'pnl_bonus', 'total_result', 'result_real',
            'result_bonus', 'leverage', 'commission', 'overnight',
            'custodial', 'margin_call', 'dividends'
        ]
        trading_df = DataCleaner._safe_astype(trading_df, {col: 'float64' for col in numeric_columns})
        trading_df = DataCleaner._safe_astype(trading_df, {col: 'float64' for col in currency_columns})
        
        return trading_df
    
    @staticmethod
    def clean_user(users_df):
        """Clean user data"""
        if users_df is None or users_df.empty:
            return pd.DataFrame()
            
        # Rename columns if they exist
        if 'id' in users_df.columns:
            users_df = users_df.rename(columns={'id': 'user_id'})
        
        # Convert string columns
        string_columns = [
            'brand', 'email', 'phone', 'aff_model', 'afftrack',
            'platform', 'first_name', 'last_name', 'country',
            'currency', 'training_currency', 'age', 'gender',
            'city', 'android_advertising_id', 'idfa'
        ]
        users_df = DataCleaner._safe_astype(users_df, {col: 'string' for col in string_columns})
        
        # Convert datetime columns
        users_df = DataCleaner._safe_astype(users_df, {'registered': 'datetime64[ns]'})
        
        # Convert numeric columns
        numeric_columns = [
            'withdrawals', 'deposits', 'pnl', 'training_balance', 'balance'
        ]
        users_df = DataCleaner._safe_astype(users_df, {col: 'float64' for col in numeric_columns})
        
        return users_df
    
    @classmethod
    def clean_all_data(cls, data_dict):
        """Clean all data in the dictionary"""
        return {
            'deposit_reports': cls.clean_deposit(data_dict.get('deposit_reports')),
            'trading_reports': cls.clean_trading(data_dict.get('trading_reports')),
            'user_reports': cls.clean_user(data_dict.get('user_reports'))
        }

def get_all_reports(start_date, end_date):
    """Get all reports using the new fetcher classes"""
    # Initialize fetchers
    trading_fetcher = QuadCodeReportFetcher(reports['trading_reports'])
    deposit_fetcher = QuadCodeReportFetcher(reports['deposit_reports'])
    users_fetcher = QuadCodeUsersFetcher()
    
    # Convert dates
    start_date_users = datetime.strptime("2024-08-01", "%Y-%m-%d")
    end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")
    
    # Fetch data
    trading_df = trading_fetcher.fetch_data(start_date, end_date)
    deposit_df = deposit_fetcher.fetch_data(start_date, end_date)
    user_df = users_fetcher.fetch_data(start_date_users, end_date_dt)
    
    user_df.to_parquet('users_original.parquet', compression='snappy', index=False)
    trading_df.to_parquet('trading_original.parquet', compression='snappy', index=False)
    deposit_df.to_parquet('deposits_original.parquet', compression='snappy', index=False)
    
    # Clean data
    data_dict = {
        "deposit_reports": deposit_df,
        "trading_reports": trading_df,
        "user_reports": user_df
    }
    
    return DataCleaner.clean_all_data(data_dict)

def insert_dataframe_to_mysql(df, table_name, engine):
    """
    Insert a DataFrame into MySQL database with different strategies based on the table.
    """
    if df.empty:
        tqdm.write(f"No data to insert into {table_name}")
        return
    
    try:
        # Create a copy of the DataFrame to avoid modifying the original
        df_to_insert = df.copy()
        
        # Convert all columns to appropriate types
        for column in df_to_insert.columns:
            # Handle datetime columns
            if 'date' in column.lower() or 'created' in column.lower() or 'updated' in column.lower() or column in ['registered']:
                df_to_insert[column] = pd.to_datetime(df_to_insert[column], errors='coerce')
            # Handle numeric columns
            elif df_to_insert[column].dtype in ['int64', 'float64'] or column in ['user_id', 'aff', 'balance']:
                df_to_insert[column] = pd.to_numeric(df_to_insert[column], errors='coerce')
            # Handle string columns
            else:
                df_to_insert[column] = df_to_insert[column].fillna('').astype(str)
        
        with engine.connect() as connection:
            if table_name == 'users':
                # For users table, use temporary table approach
                temp_table = f"temp_{table_name}"
                df_to_insert.to_sql(
                    name=temp_table,
                    con=engine,
                    if_exists='replace',
                    index=False,
                    method='multi',
                    chunksize=1000
                )
                
                # Insert/Update from temporary table
                update_str = ', '.join([f"{col} = t.{col}" for col in df_to_insert.columns if col != 'user_id'])
                query = f"""
                INSERT INTO {table_name} ({', '.join(df_to_insert.columns)})
                SELECT {', '.join(df_to_insert.columns)}
                FROM {temp_table} t
                ON DUPLICATE KEY UPDATE {update_str}
                """
                connection.execute(text(query))
                
                # Drop temporary table
                connection.execute(text(f"DROP TABLE {temp_table}"))
                
            elif table_name == 'trades':
                # For trades table, first ensure all user_ids exist in users table
                existing_users = pd.read_sql("SELECT user_id FROM users", connection)
                df_to_insert = df_to_insert[df_to_insert['user_id'].isin(existing_users['user_id'])]
                
                if not df_to_insert.empty:
                    # Insert data in chunks
                    chunk_size = 5000
                    total_rows = len(df_to_insert)
                    for i in tqdm(range(0, total_rows, chunk_size), desc=f"Inserting into {table_name}"):
                        chunk = df_to_insert.iloc[i:i + chunk_size]
                        chunk.to_sql(
                            name=table_name,
                            con=engine,
                            if_exists='append',
                            index=False,
                            method='multi'
                        )
            else:
                # For deposits table, use append
                df_to_insert.to_sql(
                    name=table_name,
                    con=engine,
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=1000
                )
            
            connection.commit()
            tqdm.write(f"Successfully inserted {len(df_to_insert)} records into {table_name}")
            
    except Exception as e:
        tqdm.write(f"Error inserting data into {table_name}: {str(e)}")
        raise


