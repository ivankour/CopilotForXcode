# database.py
import asyncio
import motor.motor_asyncio
from pymongo import ASCENDING, DESCENDING, IndexModel
import redis
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import time
import json

class CryptoDatabase:
    def __init__(self, config_path="config/db_config.json"):
        """Inicializa la conexión a las bases de datos"""
        self.load_config(config_path)
        
        # Conexión a MongoDB para almacenamiento persistente
        self.mongo_client = motor.motor_asyncio.AsyncIOMotorClient(self.mongo_uri)
        self.db = self.mongo_client[self.mongo_db_name]
        
        # Conexión a Redis para caché y datos en tiempo real
        self.redis_client = redis.Redis(
            host=self.redis_host,
            port=self.redis_port,
            password=self.redis_password,
            decode_responses=True
        )
        
        # Inicializar colecciones e índices
        asyncio.create_task(self.init_collections())
        
    def load_config(self, config_path):
        """Carga la configuración de la base de datos desde un archivo JSON"""
        with open(config_path, 'r') as file:
            config = json.load(file)
            
            # MongoDB config
            mongo_config = config.get('mongodb', {})
            self.mongo_uri = mongo_config.get('uri', 'mongodb://localhost:27017')
            self.mongo_db_name = mongo_config.get('db_name', 'crypto_assistant')
            
            # Redis config
            redis_config = config.get('redis', {})
            self.redis_host = redis_config.get('host', 'localhost')
            self.redis_port = redis_config.get('port', 6379)
            self.redis_password = redis_config.get('password', '')
    
    async def init_collections(self):
        """Inicializa las colecciones e índices en MongoDB"""
        # Colección para datos de precios históricos
        price_collection = self.db.price_data
        # Índices para búsqueda eficiente
        await price_collection.create_indexes([
            IndexModel([('symbol', ASCENDING), ('timestamp', DESCENDING)]),
            IndexModel([('exchange', ASCENDING), ('symbol', ASCENDING)]),
            IndexModel([('timestamp', DESCENDING)])
        ])
        
        # Colección para órdenes
        orders_collection = self.db.orders
        await orders_collection.create_indexes([
            IndexModel([('user_id', ASCENDING), ('timestamp', DESCENDING)]),
            IndexModel([('status', ASCENDING)]),
            IndexModel([('symbol', ASCENDING)])
        ])
        
        # Colección para predicciones de usuarios
        predictions_collection = self.db.user_predictions
        await predictions_collection.create_indexes([
            IndexModel([('user_id', ASCENDING), ('timestamp', DESCENDING)]),
            IndexModel([('symbol', ASCENDING), ('timestamp', DESCENDING)]),
            IndexModel([('prediction_type', ASCENDING)])
        ])
        
        # Colección para configuración de alertas
        alerts_collection = self.db.alerts
        await alerts_collection.create_indexes([
            IndexModel([('user_id', ASCENDING), ('active', ASCENDING)]),
            IndexModel([('symbol', ASCENDING), ('alert_type', ASCENDING)])
        ])
        
        # Colección para usuarios
        users_collection = self.db.users
        await users_collection.create_indexes([
            IndexModel([('email', ASCENDING)], unique=True),
            IndexModel([('username', ASCENDING)], unique=True)
        ])
    
    async def store_price_data(self, data: Dict[str, Any]):
        """
        Almacena datos de precio en Redis (para acceso en tiempo real) y MongoDB (para histórico)
        
        Args:
            data: Diccionario con datos de precio (exchange, symbol, price, timestamp, etc.)
        """
        # Guardar en Redis para acceso rápido en tiempo real (TTL 1 hora)
        redis_key = f"price:{data['exchange']}:{data['symbol']}"
        self.redis_client.hmset(redis_key, {
            'price': data['price'],
            'timestamp': data['timestamp'],
            'volume_24h': data.get('volume_24h', 0),
            'change_24h': data.get('change_24h', 0)
        })
        self.redis_client.expire(redis_key, 3600)  # 1 hora TTL
        
        # También almacenamos en un conjunto ordenado para rápido acceso a datos recientes
        sorted_key = f"recent_prices:{data['symbol']}"
        self.redis_client.zadd(sorted_key, {json.dumps({
            'exchange': data['exchange'],
            'price': data['price'],
            'timestamp': data['timestamp']
        }): data['timestamp']})
        self.redis_client.zremrangebyrank(sorted_key, 0, -101)  # Mantener solo 100 entradas más recientes
        self.redis_client.expire(sorted_key, 3600)  # 1 hora TTL
        
        # Guardar en MongoDB para histórico
        # Para no saturar la BD, podemos almacenar en intervalos (ej: cada minuto)
        current_minute = int(data['timestamp'] / (60 * 1000)) * (60 * 1000)
        cache_key = f"last_stored:{data['exchange']}:{data['symbol']}:{current_minute}"
        
        if not self.redis_client.exists(cache_key):
            # Solo almacenamos una vez por minuto para cada par
            await self.db.price_data.insert_one(data)
            self.redis_client.set(cache_key, 1, ex=70)  # TTL de 70 segundos
    
    async def get_latest_price(self, symbol: str, exchange: Optional[str] = None) -> Dict[str, Any]:
        """
        Obtiene el precio más reciente para un símbolo específico
        
        Args:
            symbol: Símbolo de la criptomoneda (ej: BTC)
            exchange: Exchange específico o None para obtener de cualquier exchange
            
        Returns:
            Diccionario con los datos de precio más reciente
        """
        if exchange:
            # Buscar en Redis primero
            redis_key = f"price:{exchange}:{symbol}"
            price_data = self.redis_client.hgetall(redis_key)
            
            if price_data:
                return {
                    'exchange': exchange,
                    'symbol': symbol,
                    'price': float(price_data['price']),
                    'timestamp': int(price_data['timestamp']),
                    'volume_24h': float(price_data.get('volume_24h', 0)),
                    'change_24h': float(price_data.get('change_24h', 0))
                }
        else:
            # Buscar el más reciente de cualquier exchange usando el conjunto ordenado
            sorted_key = f"recent_prices:{symbol}"
            latest = self.redis_client.zrevrange(sorted_key, 0, 0, withscores=True)
            
            if latest:
                latest_data = json.loads(latest[0][0])
                # Ahora buscar los datos completos
                redis_key = f"price:{latest_data['exchange']}:{symbol}"
                price_data = self.redis_client.hgetall(redis_key)
                
                if price_data:
                    return {
                        'exchange': latest_data['exchange'],
                        'symbol': symbol,
                        'price': float(price_data['price']),
                        'timestamp': int(price_data['timestamp']),
                        'volume_24h': float(price_data.get('volume_24h', 0)),
                        'change_24h': float(price_data.get('change_24h', 0))
                    }
        
        # Si no encontramos en Redis, buscamos en MongoDB
        query = {'symbol': symbol}
        if exchange:
            query['exchange'] = exchange
            
        latest = await self.db.price_data.find_one(
            query,
            sort=[('timestamp', DESCENDING)]
        )
        
        return latest if latest else {'symbol': symbol, 'price': None, 'timestamp': None}
    
    async def get_historical_prices(self, symbol: str, 
                                   interval: str = '1h',
                                   start_time: Optional[int] = None, 
                                   end_time: Optional[int] = None,
                                   limit: int = 100,
                                   exchange: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Obtiene datos históricos de precios con diferentes intervalos
        
        Args:
            symbol: Símbolo de la criptomoneda
            interval: Intervalo de tiempo ('1m', '5m', '1h', '4h', '1d')
            start_time: Timestamp de inicio en ms (opcional)
            end_time: Timestamp de fin en ms (opcional)
            limit: Número máximo de registros a devolver
            exchange: Exchange específico (opcional)
            
        Returns:
            Lista de registros de precios históricos
        """
        if not end_time:
            end_time = int(time.time() * 1000)
            
        if not start_time:
            # Por defecto, recuperar datos según el intervalo
            intervals = {
                '1m': 60 * 60 * 1000,  # 1 hora en ms
                '5m': 5 * 60 * 60 * 1000,  # 5 horas en ms
                '1h': 24 * 60 * 60 * 1000,  # 1 día en ms
                '4h': 7 * 24 * 60 * 60 * 1000,  # 7 días en ms
                '1d': 30 * 24 * 60 * 60 * 1000  # 30 días en ms
            }
            start_time = end_time - intervals.get(interval, 24 * 60 * 60 * 1000)
        
        # Construir la consulta
        query = {
            'symbol': symbol,
            'timestamp': {'$gte': start_time, '$lte': end_time}
        }
        
        if exchange:
            query['exchange'] = exchange
            
        # Agregación para obtener datos agrupados por intervalo
        pipeline = [
            {'$match': query},
            {'$sort': {'timestamp': 1}}
        ]
        
        # Si no es intervalo de 1 minuto, agrupamos los datos
        if interval != '1m':
            # Convertir intervalo a milisegundos
            interval_ms = {
                '5m': 5 * 60 * 1000,
                '1h': 60 * 60 * 1000,
                '4h': 4 * 60 * 60 * 1000,
                '1d': 24 * 60 * 60 * 1000
            }.get(interval, 60 * 60 * 1000)
            
            pipeline.extend([
                {
                    '$group': {
                        '_id': {
                            '$subtract': [
                                '$timestamp',
                                {'$mod': ['$timestamp', interval_ms]}
                            ]
                        },
                        'open': {'$first': '$price'},
                        'high': {'$max': '$price'},
                        'low': {'$min': '$price'},
                        'close': {'$last': '$price'},
                        'volume': {'$sum': '$volume_24h'},
                        'count': {'$sum': 1}
                    }
                },
                {
                    '$project': {
                        '_id': 0,
                        'timestamp': '$_id',
                        'open': 1,
                        'high': 1,
                        'low': 1,
                        'close': 1,
                        'volume': 1
                    }
                },
                {'$sort': {'timestamp': 1}},
                {'$limit': limit}
            ])
        else:
            # Para intervalo 1m, simplemente limitamos
            pipeline.append({'$limit': limit})
            
        # Ejecutar la agregación
        cursor = self.db.price_data.aggregate(pipeline)
        return await cursor.to_list(length=limit)
    
    async def store_order(self, order_data: Dict[str, Any]) -> str:
        """
        Almacena una orden en la base de datos
        
        Args:
            order_data: Datos de la orden a almacenar
            
        Returns:
            ID de la orden insertada
        """
        # Asegurar que la orden tiene un timestamp
        if 'timestamp' not in order_data:
            order_data['timestamp'] = int(time.time() * 1000)
            
        # Almacenar la orden
        result = await self.db.orders.insert_one(order_data)
        return str(result.inserted_id)
    
    async def update_order_status(self, order_id: str, new_status: str, 
                                 execution_data: Optional[Dict[str, Any]] = None) -> bool:
        """
        Actualiza el estado de una orden
        
        Args:
            order_id: ID de la orden
            new_status: Nuevo estado ('pending', 'filled', 'cancelled', 'partial')
            execution_data: Datos de ejecución adicionales (opcional)
            
        Returns:
            True si se actualizó correctamente, False en caso contrario
        """
        update_data = {
            '$set': {
                'status': new_status,
                'updated_at': int(time.time() * 1000)
            }
        }
        
        if execution_data:
            update_data['$set'].update(execution_data)
            
        result = await self.db.orders.update_one(
            {'_id': order_id},
            update_data
        )
        
        return result.modified_count > 0
    
    async def store_user_prediction(self, prediction_data: Dict[str, Any]) -> str:
        """
        Almacena una predicción de usuario en la base de datos
        
        Args:
            prediction_data: Datos de la predicción
            
        Returns:
            ID de la predicción insertada
        """
        # Asegurar que la predicción tiene un timestamp
        if 'timestamp' not in prediction_data:
            prediction_data['timestamp'] = int(time.time() * 1000)
            
        # Almacenar la predicción
        result = await self.db.user_predictions.insert_one(prediction_data)
        
        # Actualizar caché de sentimiento del mercado
        await self.update_market_sentiment(prediction_data['symbol'])
        
        return str(result.inserted_id)
    
    async def update_market_sentiment(self, symbol: str) -> Dict[str, Any]:
        """
        Actualiza y recupera el sentimiento del mercado para un símbolo específico
        basado en las predicciones de los usuarios
        
        Args:
            symbol: Símbolo de la criptomoneda
            
        Returns:
            Datos de sentimiento del mercado
        """
        # Obtener predicciones de las últimas 24 horas
        one_day_ago = int(time.time() * 1000) - (24 * 60 * 60 * 1000)
        
        pipeline = [
            {
                '$match': {
                    'symbol': symbol,
                    'timestamp': {'$gte': one_day_ago}
                }
            },
            {
                '$group': {
                    '_id': '$prediction_type',
                    'count': {'$sum': 1},
                    'avg_price_target': {'$avg': '$price_target'},
                    'predictions': {'$push': {
                        'user_id': '$user_id',
                        'price_target': '$price_target',
                        'timestamp': '$timestamp',
                        'confidence': '$confidence'
                    }}
                }
            }
        ]
        
        cursor = self.db.user_predictions.aggregate(pipeline)
        result = await cursor.to_list(length=None)
        
        # Procesar resultados
        sentiment_data = {
            'symbol': symbol,
            'timestamp': int(time.time() * 1000),
            'bullish_count': 0,
            'bearish_count': 0,
            'neutral_count': 0,
            'avg_price_target': 0,
            'weighted_sentiment': 0  # -1 (muy bearish) a +1 (muy bullish)
        }
        
        total_predictions = 0
        weighted_sum = 0
        price_targets = []
        
        for group in result:
            pred_type = group['_id']
            count = group['count']
            
            if pred_type == 'bullish':
                sentiment_data['bullish_count'] = count
                weighted_sum += count * 1
            elif pred_type == 'bearish':
                sentiment_data['bearish_count'] = count
                weighted_sum += count * -1
            elif pred_type == 'neutral':
                sentiment_data['neutral_count'] = count
                
            total_predictions += count
            
            # Recopilar objetivos de precio
            for pred in group.get('predictions', []):
                if 'price_target' in pred and pred['price_target']:
                    price_targets.append(pred['price_target'])
        
        # Calcular sentimiento ponderado
        if total_predictions > 0:
            sentiment_data['weighted_sentiment'] = weighted_sum / total_predictions
            
        # Calcular objetivo de precio promedio
        if price_targets:
            sentiment_data['avg_price_target'] = sum(price_targets) / len(price_targets)
        
        # Almacenar en Redis para acceso rápido
        redis_key = f"sentiment:{symbol}"
        self.redis_client.hmset(redis_key, {
            'bullish': sentiment_data['bullish_count'],
            'bearish': sentiment_data['bearish_count'],
            'neutral': sentiment_data['neutral_count'],
            'weighted': sentiment_data['weighted_sentiment'],
            'avg_target': sentiment_data['avg_price_target'],
            'timestamp': sentiment_data['timestamp']
        })
        self.redis_client.expire(redis_key, 3600)  # 1 hora TTL
        
        # También almacenar en MongoDB para histórico
        await self.db.market_sentiment.insert_one(sentiment_data)
        
        return sentiment_data
    
    async def store_alert(self, alert_data: Dict[str, Any]) -> str:
        """
        Almacena una configuración de alerta en la base de datos
        
        Args:
            alert_data: Datos de la alerta
            
        Returns:
            ID de la alerta insertada
        """
        # Validar datos mínimos requeridos
        required_fields = ['user_id', 'symbol', 'alert_type', 'conditions']
        for field in required_fields:
            if field not in alert_data:
                raise ValueError(f"Campo requerido faltante: {field}")
                
        # Campos por defecto
        alert_data.setdefault('active', True)
        alert_data.setdefault('created_at', int(time.time() * 1000))
        
        # Almacenar la alerta
        result = await self.db.alerts.insert_one(alert_data)
        return str(result.inserted_id)
    
    async def get_active_alerts_for_symbol(self, symbol: str) -> List[Dict[str, Any]]:
        """
        Obtiene todas las alertas activas para un símbolo específico
        
        Args:
            symbol: Símbolo de la criptomoneda
            
        Returns:
            Lista de alertas activas
        """
        cursor = self.db.alerts.find({
            'symbol': symbol,
            'active': True
        })
        
        return await cursor.to_list(length=None)
    
    async def deactivate_alert(self, alert_id: str) -> bool:
        """
        Desactiva una alerta
        
        Args:
            alert_id: ID de la alerta
            
        Returns:
            True si se desactivó correctamente, False en caso contrario
        """
        result = await self.db.alerts.update_one(
            {'_id': alert_id},
            {'$set': {'active': False, 'deactivated_at': int(time.time() * 1000)}}
        )
        
        return result.modified_count > 0
    
    async def create_user(self, user_data: Dict[str, Any]) -> str:
        """
        Crea un nuevo usuario en la base de datos
        
        Args:
            user_data: Datos del usuario
            
        Returns:
            ID del usuario creado
        """
        # Asegurar campos requeridos
        required_fields = ['username', 'email', 'password_hash']
        for field in required_fields:
            if field not in user_data:
                raise ValueError(f"Campo requerido faltante: {field}")
        
        # Campos por defecto
        user_data