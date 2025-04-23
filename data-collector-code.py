# data_collector.py
import asyncio
import json
import websockets
import aiohttp
import time
from datetime import datetime
from database import CryptoDatabase

class DataCollector:
    def __init__(self, config_path="config/api_config.json"):
        """Inicializador del recolector de datos de criptomonedas"""
        self.db = CryptoDatabase()
        self.active_streams = {}
        self.api_keys = {}
        self.exchanges = []
        self.load_config(config_path)
        
    def load_config(self, config_path):
        """Carga la configuración de APIs desde un archivo JSON"""
        with open(config_path, 'r') as file:
            config = json.load(file)
            self.api_keys = config.get('api_keys', {})
            self.exchanges = config.get('exchanges', [])
            
    async def initialize_streams(self):
        """Inicializa los streams de datos para todos los exchanges configurados"""
        tasks = []
        for exchange in self.exchanges:
            if exchange == 'binance':
                tasks.append(self.connect_binance())
            elif exchange == 'coinbase':
                tasks.append(self.connect_coinbase())
            # Agregar más exchanges según sea necesario
        
        await asyncio.gather(*tasks)
        
    async def connect_binance(self):
        """Establece conexión con WebSocket de Binance para datos en tiempo real"""
        uri = "wss://stream.binance.com:9443/ws/!ticker@arr"
        
        async with websockets.connect(uri) as websocket:
            self.active_streams['binance'] = websocket
            while True:
                try:
                    response = await websocket.recv()
                    data = json.loads(response)
                    
                    # Procesamiento de los datos recibidos
                    for ticker in data:
                        symbol = ticker['s']
                        if symbol.endswith('USDT'):  # Filtramos pares con USDT
                            crypto = symbol[:-4]  # Eliminamos 'USDT' del final
                            price = float(ticker['c'])  # Precio actual
                            timestamp = int(time.time() * 1000)
                            
                            # Almacenamos en la base de datos
                            await self.db.store_price_data({
                                'exchange': 'binance',
                                'symbol': crypto,
                                'price': price,
                                'timestamp': timestamp,
                                'volume_24h': float(ticker['v']),
                                'change_24h': float(ticker['p'])
                            })
                            
                except Exception as e:
                    print(f"Error en Binance WebSocket: {e}")
                    # Reconexión tras error
                    await asyncio.sleep(5)
                    return await self.connect_binance()
    
    async def connect_coinbase(self):
        """Establece conexión con la API de Coinbase para datos en tiempo real"""
        uri = "wss://ws-feed.pro.coinbase.com"
        
        # Configuración de la suscripción
        subscription = {
            "type": "subscribe",
            "channels": [{"name": "ticker", "product_ids": ["BTC-USD", "ETH-USD", "SOL-USD"]}]
        }
        
        async with websockets.connect(uri) as websocket:
            self.active_streams['coinbase'] = websocket
            
            # Enviar mensaje de suscripción
            await websocket.send(json.dumps(subscription))
            
            while True:
                try:
                    response = await websocket.recv()
                    data = json.loads(response)
                    
                    if data.get('type') == 'ticker':
                        product_id = data.get('product_id', '')
                        if '-USD' in product_id:
                            crypto = product_id.split('-')[0]
                            price = float(data.get('price', 0))
                            timestamp = int(datetime.fromisoformat(data.get('time').replace('Z', '+00:00')).timestamp() * 1000)
                            
                            # Almacenamos en la base de datos
                            await self.db.store_price_data({
                                'exchange': 'coinbase',
                                'symbol': crypto,
                                'price': price,
                                'timestamp': timestamp,
                                'volume_24h': float(data.get('volume_24h', 0)),
                                'change_24h': 0  # Coinbase no proporciona este dato directamente
                            })
                            
                except Exception as e:
                    print(f"Error en Coinbase WebSocket: {e}")
                    # Reconexión tras error
                    await asyncio.sleep(5)
                    return await self.connect_coinbase()
    
    async def fetch_rest_data(self):
        """Obtiene datos adicionales a través de APIs REST para exchanges que no soportan WebSockets"""
        while True:
            for exchange in self.exchanges:
                if exchange == 'kraken':
                    await self.fetch_kraken_data()
                # Agregar más exchanges según sea necesario
                
            # Actualizamos cada 30 segundos para APIs REST
            await asyncio.sleep(30)
    
    async def fetch_kraken_data(self):
        """Obtiene datos de precios desde la API REST de Kraken"""
        url = "https://api.kraken.com/0/public/Ticker"
        params = {"pair": "BTCUSD,ETHUSD,SOLUSD"}
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        result = data.get('result', {})
                        
                        # Mapeamos los pares de Kraken a símbolos estándar
                        mapping = {
                            'XXBTZUSD': 'BTC',
                            'XETHZUSD': 'ETH',
                            'SOLUSD': 'SOL'
                        }
                        
                        for pair, info in result.items():
                            if pair in mapping:
                                crypto = mapping[pair]
                                price = float(info['c'][0])  # Precio actual
                                timestamp = int(time.time() * 1000)
                                
                                # Almacenamos en la base de datos
                                await self.db.store_price_data({
                                    'exchange': 'kraken',
                                    'symbol': crypto,
                                    'price': price,
                                    'timestamp': timestamp,
                                    'volume_24h': float(info['v'][1]),  # Volumen 24h
                                    'change_24h': 0  # Calcularlo manualmente si es necesario
                                })
            except Exception as e:
                print(f"Error obteniendo datos de Kraken: {e}")
    
    async def run(self):
        """Ejecuta todos los recolectores de datos en paralelo"""
        await asyncio.gather(
            self.initialize_streams(),
            self.fetch_rest_data()
        )

if __name__ == "__main__":
    collector = DataCollector()
    asyncio.run(collector.run())