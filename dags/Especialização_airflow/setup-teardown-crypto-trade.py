import requests
import time

from airflow.decorators import dag, task, setup, teardown
from datetime import datetime, timedelta
from typing import Dict, List, Any


# TODO simulated database connection class
class CryptoDBConnection:
    def __init__(self, db_name: str):
        self.db_name = db_name
        self.connected = False

    def connect(self):
        time.sleep(2)
        self.connected = True
        return f"Connected to {self.db_name}"

    def disconnect(self):
        time.sleep(1)
        self.connected = False
        return f"Disconnected from {self.db_name}"


# TODO Simulated trading API client
class TradingAPIClient:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = None

    def initialize(self):
        self.session = requests.Session()
        return f"API client initialized with key {self.api_key}"

    def cleanup(self):
        if self.session:
            self.session.close()
        return "API session closed"


@dag(
    dag_id='setup-teardown-crypto-trade',
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    schedule_interval='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['crypto', 'trading']
)
def crypto_trade_pipeline():

    @setup
    def setup_database():
        """Set up database connection for storing crypto data"""

        print("Initializing database connection...")
        db = CryptoDBConnection("crypto_market_db")
        conn_info = db.connect()
        return {
            "db_name": "crypto_market_db",
            "conn_info": conn_info,
            "timestamp": datetime.now().isoformat()
        }

    @setup
    def setup_api_client():
        """Set up trading API client"""

        print("Initializing trading API client...")
        client = TradingAPIClient("simulation_api_key_123")
        init_info = client.initialize()
        return {
            "api_key": "simulation_api_key_123",
            "init_info": init_info,
            "timestamp": datetime.now().isoformat()
        }

    @task
    def fetch_market_data(api_config: dict) -> Dict[str, Any]:
        """Fetch current market data for top cryptocurrencies"""

        print(f"Using API config: {api_config['init_info']}")

        url = "https://api.coingecko.com/api/v3/simple/price"
        params = {
            'ids': 'bitcoin,ethereum,ripple,cardano,solana',
            'vs_currencies': 'usd',
            'include_24hr_change': 'true',
            'include_market_cap': 'true',
            'include_24hr_vol': 'true'
        }

        response = requests.get(url, params=params)
        data = response.json()

        return {
            "market_data": data,
            "timestamp": datetime.now().isoformat(),
            "status": "success"
        }

    @task
    def analyze_trading_signals(market_data: dict) -> Dict[str, Any]:
        """Analyze market data and generate trading signals"""

        signals = {}

        for crypto, data in market_data["market_data"].items():
            change_24h = data.get('usd_24h_change', 0)
            volume = data.get('usd_24h_vol', 0)
            market_cap = data.get('usd_market_cap', 0)

            signal = {
                'price': data['usd'],
                'change_24h': change_24h,
                'volume': volume,
                'market_cap': market_cap,
                'signal': 'BUY' if change_24h < -5 and volume > 1000000 else 'SELL' if change_24h > 5 else 'HOLD'
            }

            signals[crypto] = signal

        return {
            "trading_signals": signals,
            "timestamp": datetime.now().isoformat(),
            "analysis_version": "1.0"
        }

    @task
    def generate_trade_orders(trading_signals: dict, db_config: dict) -> Dict[str, Any]:
        """Generate trade orders based on signals"""

        print(f"Using DB config: {db_config['conn_info']}")

        orders = []
        for crypto, signal in trading_signals["trading_signals"].items():
            if signal['signal'] != 'HOLD':
                order = {
                    'symbol': crypto,
                    'action': signal['signal'],
                    'price': signal['price'],
                    'timestamp': datetime.now().isoformat(),
                    'volume': round(10000 / signal['price'], 8),  # Simulate $10k orders
                    'reason': f"{signal['change_24h']:.2f}% 24h change"
                }
                orders.append(order)

        return {
            "orders": orders,
            "timestamp": datetime.now().isoformat(),
            "order_batch_id": f"batch_{int(time.time())}"
        }

    @teardown
    def teardown_database(db_config: dict):
        """Clean up database connection"""

        print(f"Cleaning up database connection for {db_config['db_name']}")
        db = CryptoDBConnection(db_config['db_name'])
        return db.disconnect()

    @teardown
    def teardown_api_client(api_config: dict):
        """Clean up API client"""

        print(f"Cleaning up API client for key {api_config['api_key']}")
        client = TradingAPIClient(api_config['api_key'])
        return client.cleanup()

    # TODO setup resources
    db_config = setup_database()
    api_config = setup_api_client()

    market_data = fetch_market_data(api_config)
    trading_signals = analyze_trading_signals(market_data)
    trade_orders = generate_trade_orders(trading_signals, db_config)

    trade_orders >> teardown_database(db_config)
    trade_orders >> teardown_api_client(api_config)


dag = crypto_trade_pipeline()
