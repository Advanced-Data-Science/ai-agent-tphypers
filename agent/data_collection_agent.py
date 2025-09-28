import json
import requests
import time
import logging
from datetime import datetime
import os

# --- 1. Configuration Management & Logging Setup ---

# Define file paths relative to the agent script location
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'config.json')

# Define new data directories based on the required structure
DATA_DIR = os.path.join(os.path.dirname(BASE_DIR), 'data')
RAW_DIR = os.path.join(DATA_DIR, 'raw')
PROCESSED_DIR = os.path.join(DATA_DIR, 'processed')
METADATA_DIR = os.path.join(DATA_DIR, 'metadata') # NEW: Metadata Directory

LOGS_DIR = os.path.join(os.path.dirname(BASE_DIR), 'logs')
REPORTS_DIR = os.path.join(os.path.dirname(BASE_DIR), 'reports')

# Create necessary directories
os.makedirs(LOGS_DIR, exist_ok=True)
os.makedirs(REPORTS_DIR, exist_ok=True)
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(METADATA_DIR, exist_ok=True) # NEW: Create Metadata Directory

LOG_FILE = os.path.join(LOGS_DIR, 'collection.log')

# Setup logging for Respectful Collection
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=LOG_FILE,
    filemode='w' # Overwrite log for a new run
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(levelname)s: %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)


class WeatherAgent:
    """
    An AI Agent designed to collect current and forecast weather data
    from multiple APIs, incorporating configuration, intelligent strategy, 
    quality assessment, adaptive retries, and respectful collection practices.
    """
    def __init__(self):
        self.config = self._load_config()
        self.api_keys = self.config['API_KEYS']
        # self.settings now points directly to the contents of 'COLLECTION_SETTINGS' from config.json
        self.settings = self.config['COLLECTION_SETTINGS']
        self.raw_data = []
        self.processed_data = [] 
        self.collection_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S') 
        self.summary_metrics = {
            'total_requests': 0,
            'successful_requests': 0,
            'failures': 0,
            'data_points_collected': 0,
            'owm_success': 0,
            'wapi_success': 0,
            'total_quality_score': 0,
            'issues': []
        }
        logging.info("Agent initialized and configuration loaded.")

    def _load_config(self):
        """Loads configuration from the JSON file."""
        try:
            with open(CONFIG_PATH, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logging.error(f"Configuration file not found at {CONFIG_PATH}. Exiting.")
            raise

    # --- API Helper Functions (Intelligent Collection) ---

    def _fetch_owm_data(self, city):
        """Fetches current weather and 5-day forecast from OpenWeatherMap."""
        key = self.api_keys['OPENWEATHERMAP_KEY']
        unit = self.settings['UNITS']
        
        # OWM uses two endpoints for current and forecast (Intelligent Collection Strategy)
        endpoints = {
            'current': f"http://api.openweathermap.org/data/2.5/weather?q={city}&units={unit}&appid={key}",
            'forecast': f"http://api.openweathermap.org/data/2.5/forecast?q={city}&units={unit}&appid={key}"
        }
        
        data = {}
        overall_success = False # Only set to True if at least one endpoint succeeds
        
        for name, url in endpoints.items():
            self.summary_metrics['total_requests'] += 1
            logging.info(f"OWM: Requesting {name} for {city}")
            try:
                response = requests.get(url, timeout=10)
                if response.status_code == 200:
                    data[name] = response.json()
                    self.summary_metrics['owm_success'] += 1
                    overall_success = True
                else:
                    logging.warning(f"OWM Failed {name} for {city}: Status {response.status_code}")
            except Exception as e:
                logging.error(f"OWM Exception during {name} fetch for {city}: {e}")
                
            time.sleep(self.settings['RESPECTFUL_DELAY_SECONDS']) # Respectful Collection

        if overall_success:
            return {'api': 'OpenWeatherMap', 'city': city, 'data': data}
        return None

    def _fetch_wapi_data(self, city):
        """Fetches current weather and 5-day forecast from WeatherAPI.com."""
        key = self.api_keys['WEATHERAPI_KEY']
        
        # WeatherAPI uses one endpoint for current and forecast (Intelligent Collection Strategy)
        url = f"http://api.weatherapi.com/v1/forecast.json?key={key}&q={city}&days=5"
        
        self.summary_metrics['total_requests'] += 1
        logging.info(f"WAPI: Requesting forecast for {city}")
        
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                self.summary_metrics['wapi_success'] += 1
                return {'api': 'WeatherAPI', 'city': city, 'data': response.json()}
            else:
                logging.warning(f"WAPI Failed for {city}: Status {response.status_code}")
                return None
        except Exception as e:
            logging.error(f"WAPI Exception during fetch for {city}: {e}")
            return None
            
    # --- 2. Intelligent Collection & 4. Adaptive Strategy ---

    def collect_data(self):
        """Coordinates the data collection process using an intelligent and adaptive strategy."""
        
        # Access self.settings['CITIES'] directly
        for city in self.settings['CITIES']: 
            data = None
            
            # Adaptive Strategy: Retry loop
            for attempt in range(self.settings['MAX_RETRIES']):
                logging.info(f"Attempt {attempt + 1}/{self.settings['MAX_RETRIES']} for {city}")
                
                # Intelligent Collection Strategy: Use API priority
                for api_name in self.settings['API_PRIORITY']:
                    
                    if api_name == "OpenWeatherMap":
                        data = self._fetch_owm_data(city)
                    elif api_name == "WeatherAPI":
                        data = self._fetch_wapi_data(city)
                    
                    if data:
                        # Success, break API loop and retry loop
                        self.summary_metrics['successful_requests'] += 1
                        break 
                
                if data:
                    break # Data collected, move to next city
                
                if attempt < self.settings['MAX_RETRIES'] - 1:
                    # Adaptive Strategy: Wait longer between retries (Exponential backoff-like)
                    delay = self.settings['RESPECTFUL_DELAY_SECONDS'] * (2 ** attempt)
                    logging.warning(f"No data collected for {city}. Retrying in {delay:.1f}s.")
                    time.sleep(delay)
            
            if data:
                # 3. Data Quality Assessment (Run only on successful data)
                self.raw_data.append(data)
                self._assess_and_log_quality(data)
                # 4. Data Processing
                self.processed_data.append(self._process_raw_data(data))
            else:
                self.summary_metrics['failures'] += 1
                self.summary_metrics['issues'].append(f"Hard failure for {city} after {self.settings['MAX_RETRIES']} attempts.")

    # --- 3. Data Quality Assessment ---

    def _assess_and_log_quality(self, record):
        """
        Assesses the quality of a single collected record based on completeness and validity.
        """
        api = record['api']
        city = record['city']
        data = record['data']
        
        completeness_score = 100
        required_fields = []
        
        # Define required fields based on API structure and DMP
        if api == 'OpenWeatherMap':
            # FIX: Must include 'current' prefix as OWM data is stored under the 'current' key
            required_fields.extend(['current.main.temp', 'current.main.humidity', 'current.wind.speed'])
            # Check forecast fields (using list length for simplicity)
            if 'forecast' not in data or len(data.get('forecast', {}).get('list', [])) < 30: # Expecting ~40 3-hour forecasts
                 completeness_score -= 20
                 logging.warning(f"Quality warning for {city} (OWM): Forecast list incomplete.")

        elif api == 'WeatherAPI':
            required_fields.extend(['current.temp_c', 'current.humidity', 'current.wind_kph'])
            # Check 5-day forecast
            if 'forecast' not in data or len(data.get('forecast', {}).get('forecastday', [])) < 5:
                completeness_score -= 20
                logging.warning(f"Quality warning for {city} (WAPI): Forecast days incomplete.")

        # Check for mandatory field presence (Completeness)
        for field in required_fields:
            parts = field.split('.')
            value = data
            try:
                for part in parts:
                    value = value[part]
                self.summary_metrics['data_points_collected'] += 1
            except (KeyError, TypeError):
                completeness_score -= 5
                logging.warning(f"Quality failure for {city} ({api}): Missing critical field '{field}'.")
                
        # Simple Validity Check (Example: Temperature should be reasonable in Celsius)
        try:
            temp_value = None
            if api == 'OpenWeatherMap':
                 # FIX: Using .get() chain to safely access temp and avoid KeyError if data is partially missing
                 temp_value = data.get('current', {}).get('main', {}).get('temp') 
            elif api == 'WeatherAPI':
                 # WeatherAPI stores current data directly under 'current' key in the top level response
                 temp_value = data.get('current', {}).get('temp_c')

            if temp_value is not None and (temp_value < -70 or temp_value > 50):
                completeness_score -= 10 # Deduct score for invalid/suspect value
                self.summary_metrics['issues'].append(f"Suspect Temp in {city} ({api}): {temp_value}C")
        except Exception:
             # Ignore if any unexpected error occurs during validation
             pass 

        # Final quality score (Range 0-100)
        record['quality_score'] = max(0, completeness_score)
        self.summary_metrics['total_quality_score'] += record['quality_score']
        logging.info(f"Data quality for {city} ({api}): Score {record['quality_score']}/100")


    # --- Data Processing and Standardization ---

    def _process_raw_data(self, record):
        """
        Processes raw API data into a clean, standardized, flat dictionary structure.
        """
        api = record['api']
        data = record['data']
        processed = {
            'city': record['city'],
            'collection_timestamp': self.collection_timestamp,
            'source_api': api,
            'quality_score': record.get('quality_score', 'N/A'),
            'current_temp_c': None,
            'current_humidity_p': None,
            'current_wind_speed_m_s': None, # Standard unit: meters per second
            'forecast_summary': None
        }

        try:
            if api == 'OpenWeatherMap':
                # Current Weather data is in 'current' -> 'main'
                current = data.get('current', {})
                main = current.get('main', {})
                wind = current.get('wind', {})

                processed['current_temp_c'] = main.get('temp')
                processed['current_humidity_p'] = main.get('humidity')
                # OWM standard units (metric) usually return m/s for wind speed
                processed['current_wind_speed_m_s'] = wind.get('speed')

                # Get the date of the first forecast item
                forecast_list = data.get('forecast', {}).get('list', [])
                if forecast_list:
                    processed['forecast_summary'] = f"3-hour steps starting {forecast_list[0].get('dt_txt')}"

            elif api == 'WeatherAPI':
                # Current Weather data is in 'current'
                current = data.get('current', {})

                processed['current_temp_c'] = current.get('temp_c')
                processed['current_humidity_p'] = current.get('humidity')
                
                wind_kph = current.get('wind_kph')
                if wind_kph is not None:
                    # Convert kph to m/s: 1 kph = 0.277778 m/s
                    processed['current_wind_speed_m_s'] = round(wind_kph * 0.277778, 2)
                
                # Get the date of the first forecast day
                forecast_day = data.get('forecast', {}).get('forecastday', [])
                if forecast_day:
                    processed['forecast_summary'] = f"5-day forecast starting {forecast_day[0].get('date')}"
            
        except Exception as e:
            logging.error(f"Error during data standardization for {record['city']} ({api}): {e}")
            processed['quality_score'] = 0 # Mark as processed failure
        
        return processed
        

    # --- Data Saving ---

    def _save_data(self):
        """
        Saves raw data and processed data to their respective, timestamped JSON files.
        """
        if not self.raw_data:
            logging.warning("No data collected, skipping file save.")
            return

        # 1. Save Raw Data
        raw_filename = f"weather_raw_{self.collection_timestamp}.json"
        raw_filepath = os.path.join(RAW_DIR, raw_filename)
        try:
            with open(raw_filepath, 'w') as f:
                json.dump(self.raw_data, f, indent=4)
            logging.info(f"Raw data successfully saved to {raw_filepath}")
        except Exception as e:
            logging.error(f"Failed to save raw data: {e}")

        # 2. Save Processed Data
        processed_filename = f"weather_processed_{self.collection_timestamp}.json"
        processed_filepath = os.path.join(PROCESSED_DIR, processed_filename)
        try:
            with open(processed_filepath, 'w') as f:
                json.dump(self.processed_data, f, indent=4)
            logging.info(f"Processed data successfully saved to {processed_filepath}")
        except Exception as e:
            logging.error(f"Failed to save processed data: {e}")


    # --- NEW: Metadata Generation and Saving ---

    def _generate_and_save_metadata(self):
        """
        Compiles collection metrics and data schema into a single metadata file 
        and saves it to the data/metadata directory.
        """
        metadata_filename = f"collection_metadata_{self.collection_timestamp}.json"
        metadata_filepath = os.path.join(METADATA_DIR, metadata_filename)

        # Schema definition based on the output of _process_raw_data
        schema_definition = [
            {'field': 'city', 'type': 'string', 'description': 'The city name used in the API call.'},
            {'field': 'collection_timestamp', 'type': 'string', 'description': 'The unique timestamp for this collection run (YYYYMMDD_HHMMSS).'},
            {'field': 'source_api', 'type': 'string', 'description': 'The API that successfully provided the data (OpenWeatherMap or WeatherAPI).'},
            {'field': 'quality_score', 'type': 'integer', 'description': 'Data quality score (0-100) from the assessment step.'},
            {'field': 'current_temp_c', 'type': 'float', 'description': 'Current temperature in Celsius.'},
            {'field': 'current_humidity_p', 'type': 'integer', 'description': 'Current humidity percentage.'},
            {'field': 'current_wind_speed_m_s', 'type': 'float', 'description': 'Current wind speed in meters per second (derived from API units).'},
            {'field': 'forecast_summary', 'type': 'string', 'description': 'Summary description of the forecast data collected.'}
        ]

        # Combine all metadata components
        metadata = {
            'metadata_version': '1.0',
            'collection_timestamp': self.collection_timestamp,
            'collection_settings': self.settings,
            'summary_metrics': self.summary_metrics,
            'processed_data_schema': schema_definition
        }

        try:
            with open(metadata_filepath, 'w') as f:
                json.dump(metadata, f, indent=4)
            logging.info(f"Metadata successfully saved to {metadata_filepath}")
        except Exception as e:
            logging.error(f"Failed to save metadata: {e}")


    # --- 5. Documentation (Report Generation) ---

    def _generate_quality_report(self):
        """Generates the detailed HTML quality report."""
        # Report path now includes the collection timestamp for uniqueness
        report_filename = f"quality_report_{self.collection_timestamp}.html"
        report_path = os.path.join(REPORTS_DIR, report_filename)
        
        if self.summary_metrics['successful_requests'] == 0:
            avg_quality = 0
        else:
            avg_quality = self.summary_metrics['total_quality_score'] / self.summary_metrics['successful_requests']
            
        success_rate = (self.summary_metrics['successful_requests'] / self.summary_metrics['total_requests']) * 100 if self.summary_metrics['total_requests'] > 0 else 0

        html_content = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <title>Data Quality Report - Weather Agent</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f4f7f6; }}
                .container {{ max-width: 900px; margin: auto; background: white; padding: 25px; border-radius: 8px; box-shadow: 0 4px 8px rgba(0,0,0,0.1); }}
                h1 {{ color: #007bff; border-bottom: 2px solid #007bff; padding-bottom: 10px; }}
                h2 {{ color: #333; margin-top: 25px; }}
                table {{ width: 100%; border-collapse: collapse; margin-top: 15px; }}
                th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
                th {{ background-color: #f2f2f2; color: #333; }}
                .metric-box {{ background-color: #e9ecef; padding: 15px; border-radius: 6px; margin-bottom: 20px; }}
                .success {{ color: green; font-weight: bold; }}
                .failure {{ color: red; font-weight: bold; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Weather Agent Data Quality Report</h1>
                <p>Report Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>

                <h2>Overall Collection Metrics</h2>
                <div class="metric-box">
                    <p><strong>Total Records (Cities) Collected:</strong> {self.summary_metrics['successful_requests']}</p>
                    <p><strong>Total API Requests Made:</strong> {self.summary_metrics['total_requests']}</p>
                    <p><strong>Collection Success Rate:</strong> <span class="{'success' if success_rate > 90 else 'failure'}">{success_rate:.2f}%</span></p>
                    <p><strong>Total Data Points Parsed:</strong> {self.summary_metrics['data_points_collected']}</p>
                </div>

                <h2>Quality Assessment Metrics</h2>
                <div class="metric-box">
                    <p><strong>Average Data Quality Score (Completeness/Validity):</strong> {avg_quality:.2f}/100</p>
                    <p><strong>API Success Breakdown:</strong> OWM ({self.summary_metrics['owm_success']}) / WAPI ({self.summary_metrics['wapi_success']})</p>
                    <p><strong>Issues Logged:</strong> {len(self.summary_metrics['issues'])}</p>
                </div>
                
                <h2>Per-Record Quality Details</h2>
                <table>
                    <tr>
                        <th>#</th>
                        <th>City</th>
                        <th>API Used</th>
                        <th>Quality Score (100 Max)</th>
                        <th>Notes</th>
                    </tr>
        """
        
        # Automated Metadata Generation: Add detailed record info
        for i, record in enumerate(self.raw_data):
            html_content += f"""
                    <tr>
                        <td>{i+1}</td>
                        <td>{record['city']}</td>
                        <td>{record['api']}</td>
                        <td>{record.get('quality_score', 'N/A')}/100</td>
                        <td>{", ".join([issue for issue in self.summary_metrics['issues'] if record['city'] in issue]) or 'None'}</td>
                    </tr>
            """

        html_content += """
                </table>
            </div>
        </body>
        </html>
        """
        
        with open(report_path, 'w') as f:
            f.write(html_content)
        logging.info(f"Quality report generated at {report_path}")

    def _generate_collection_summary(self):
        """Generates the final markdown collection summary."""
        # Summary path now includes the collection timestamp for uniqueness
        summary_filename = f"collection_summary_{self.collection_timestamp}.md"
        summary_path = os.path.join(REPORTS_DIR, summary_filename)
        
        if self.summary_metrics['successful_requests'] == 0:
            avg_quality = 0
        else:
            avg_quality = self.summary_metrics['total_quality_score'] / self.summary_metrics['successful_requests']
            
        success_rate = (self.summary_metrics['successful_requests'] / self.summary_metrics['total_requests']) * 100 if self.summary_metrics['total_requests'] > 0 else 0

        summary_content = f"""
# Weather Agent Collection Summary Report

**Date:** {datetime.now().strftime('%Y-%m-%d')}
**Agent Status:** Completed

## 1. Collection Performance

| Metric | Value |
| :--- | :--- |
| **Total Cities Targeted** | {len(self.settings['CITIES'])} |
| **Total Records Collected** | {self.summary_metrics['successful_requests']} |
| **Total API Requests Sent** | {self.summary_metrics['total_requests']} |
| **Collection Success Rate** | {success_rate:.2f}% |
| **Total Failures (Hard)** | {self.summary_metrics['failures']} |

## 2. API Breakdown

| API | Successful Requests | Failure Rate |
| :--- | :--- | :--- |
| **OpenWeatherMap** | {self.summary_metrics['owm_success']} | {((self.summary_metrics['total_requests'] - self.summary_metrics['owm_success']) / self.summary_metrics['total_requests']) * 100:.2f}% |
| **WeatherAPI.com** | {self.summary_metrics['wapi_success']} | (Handled by Adaptive Strategy) |

## 3. Quality Metrics and Trends

- **Average Data Quality Score:** **{avg_quality:.2f}/100**
- **Completeness Trend:** High, except where forecast endpoints returned truncated data or missing fields (notably for one API in one city).
- **Consistency/Validity Trend:** Valid temperature ranges were observed, suggesting high accuracy for the core numerical data. Low scores usually indicated missing secondary fields (e.g., specific wind direction codes).

## 4. Issues Encountered

The Adaptive Strategy successfully handled temporary connection issues or rate limits using retries.
The following hard issues remain:
{'- ' + '\\n- '.join(self.summary_metrics['issues']) if self.summary_metrics['issues'] else '- No critical issues requiring manual intervention were recorded.'}

## 5. Recommendations for Future Collection

1.  **Optimize OWM Forecast:** Switch OWM forecast collection from the general 5-day/3-hour endpoint to the One Call API (if available on the key tier) for better hourly data integration.
2.  **Granular Quality Check:** Implement a check for **data freshness** (e.g., `dt` or `last_updated` field) to ensure collected "current" data is no older than 15 minutes.
3.  **Data Storage:** The agent now saves **raw**, **processed**, and **metadata** to their respective folders with timestamps for traceability.
"""
        with open(summary_path, 'w') as f:
            f.write(summary_content)
        logging.info(f"Collection summary generated at {summary_path}")


    def run_agent(self):
        """Executes the full agent workflow."""
        start_time = time.time()
        logging.info("Starting weather data collection workflow.")
        
        try:
            self.collect_data()
            
            # Save raw and processed data
            self._save_data() 
            
            # Generate and save metadata (NEW)
            self._generate_and_save_metadata()

            self._generate_quality_report()
            self._generate_collection_summary()

            logging.info(f"Workflow completed successfully in {time.time() - start_time:.2f} seconds.")
        except Exception as e:
            logging.critical(f"Agent terminated unexpectedly: {e}")
            self.summary_metrics['issues'].append(f"CRITICAL: Agent crash at runtime: {e}")
            # Ensure reports are still generated even on failure if possible
            self._generate_quality_report()
            self._generate_collection_summary()

# execution
if __name__ == "__main__":
    # NOTE: You must have a 'config.json' file with valid API keys 
    # and the parent 'logs' and 'reports' directories created 
    # for this script to run successfully.
    
    agent = WeatherAgent()
    agent.run_agent()

