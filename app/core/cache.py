# app/core/cache.py

import json
import logging
from typing import Dict, Any, Optional, List, Union, Set, Tuple
from redis.asyncio import Redis
import decimal # Import Decimal for type hinting and serialization
import time
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.models import User, DemoUser

logger = logging.getLogger(__name__)

logger.setLevel(logging.DEBUG)
from app.core.logging_config import cache_logger
# Keys for storing data in Redis
REDIS_USER_DATA_KEY_PREFIX = "user_data:" # Stores group_name, leverage, etc.
REDIS_USER_PORTFOLIO_KEY_PREFIX = "user_portfolio:" # Stores balance, positions
# New key prefix for group settings per symbol
REDIS_GROUP_SYMBOL_SETTINGS_KEY_PREFIX = "group_symbol_settings:" # Stores spread, pip values, etc. per group and symbol
# New key prefix for general group settings
REDIS_GROUP_SETTINGS_KEY_PREFIX = "group_settings:" # Stores general group settings like sending_orders
# New key prefix for last known price
LAST_KNOWN_PRICE_KEY_PREFIX = "last_price:"

# Expiry times (adjust as needed)
USER_DATA_CACHE_EXPIRY_SECONDS = 7 * 24 * 60 * 60 # Example: User session length
# USER_DATA_CACHE_EXPIRY_SECONDS = 10
USER_PORTFOLIO_CACHE_EXPIRY_SECONDS = 5 * 60 # Example: Short expiry, updated frequently
GROUP_SYMBOL_SETTINGS_CACHE_EXPIRY_SECONDS = 30 * 24 * 60 * 60 # Example: Group settings change infrequently
GROUP_SETTINGS_CACHE_EXPIRY_SECONDS = 30 * 24 * 60 * 60 # Example: Group settings change infrequently

# Redis key prefixes for different cache types
USER_DATA_PREFIX = "user_data_cache:"
USER_PORTFOLIO_PREFIX = "user_portfolio_cache:"
GROUP_SYMBOL_SETTINGS_PREFIX = "group_symbol_settings:"
ADJUSTED_MARKET_PRICE_PREFIX = "adjusted_market_price:"
LAST_KNOWN_PRICE_PREFIX = "last_known_price:"

# Redis channel for account structure changes
REDIS_ACCOUNT_CHANGE_CHANNEL = 'account_structure_change'

# Cache expiration times (in seconds)
USER_DATA_EXPIRY = 86400  # 24 hours
USER_PORTFOLIO_EXPIRY = 3600  # 1 hour
GROUP_SYMBOL_SETTINGS_EXPIRY = 86400  # 24 hours
ADJUSTED_MARKET_PRICE_EXPIRY = 3600  # 1 hour
LAST_KNOWN_PRICE_EXPIRY = 3600  # 1 hour

# Maximum age for cache before requiring validation (in seconds)
CACHE_MAX_AGE = 300  # 5 minutes

# --- Last Known Price Cache ---
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super().default(o)

def decode_decimal(obj):
    """Recursively decode dictionary values, attempting to convert strings to Decimal."""
    if isinstance(obj, dict):
        return {k: decode_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [decode_decimal(elem) for elem in obj]
    elif isinstance(obj, str):
        try:
            return decimal.Decimal(obj)
        except decimal.InvalidOperation:
            return obj
    else:
        return obj


# --- User Data Cache (Modified) ---
async def set_user_data_cache(redis_client: Redis, user_id: int, user_data: Dict[str, Any]) -> bool:
    """
    Set user data in the cache with timestamp for validation.
    
    Args:
        redis_client: Redis client
        user_id: User ID
        user_data: User data to cache
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Add timestamp for cache validation
        user_data['_cache_timestamp'] = time.time()
        
        # Serialize and store in Redis
        key = f"{USER_DATA_PREFIX}{user_id}"
        await redis_client.set(
            key, 
            json.dumps(user_data, cls=DecimalEncoder),
            ex=USER_DATA_EXPIRY
        )
        return True
    except Exception as e:
        logger.error(f"Error setting user data cache for user {user_id}: {e}", exc_info=True)
        return False

async def get_user_data_cache(
    redis_client: Redis, 
    user_id: int, 
    db_session: Optional[AsyncSession] = None,
    user_type: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """
    Get user data from cache with validation and database fallback.
    
    Args:
        redis_client: Redis client
        user_id: User ID
        db_session: Database session for fallback
        user_type: User type (live or demo) for fallback
        
    Returns:
        Dict[str, Any]: User data or None if not found
    """
    try:
        key = f"{USER_DATA_PREFIX}{user_id}"
        cached_data = await redis_client.get(key)
        
        if cached_data:
            user_data = json.loads(cached_data, object_hook=decode_decimal)
            
            # Check if cache needs validation
            timestamp = user_data.get('_cache_timestamp', 0)
            current_time = time.time()
            
            if current_time - timestamp <= CACHE_MAX_AGE:
                # Cache is fresh, return it
                return user_data
            
            # Cache is stale but usable, refresh in background if db_session provided
            if db_session and user_type:
                # Import here to avoid circular imports
                from app.crud import user as crud_user
                
                # Schedule background refresh without awaiting
                if user_type.lower() == 'demo':
                    db_user = await crud_user.get_demo_user_by_id(db_session, user_id, user_type=user_type)
                else:
                    db_user = await crud_user.get_user_by_id(db_session, user_id, user_type=user_type)
                
                if db_user:
                    # Update cache with fresh data
                    fresh_user_data = {
                        "id": db_user.id,
                        "email": db_user.email,
                        "group_name": db_user.group_name,
                        "leverage": db_user.leverage,
                        "user_type": db_user.user_type,
                        "account_number": getattr(db_user, 'account_number', None),
                        "wallet_balance": db_user.wallet_balance,
                        "margin": db_user.margin,
                        "first_name": getattr(db_user, 'first_name', None),
                        "last_name": getattr(db_user, 'last_name', None),
                        "country": getattr(db_user, 'country', None),
                        "phone_number": getattr(db_user, 'phone_number', None),
                    }
                    await set_user_data_cache(redis_client, user_id, fresh_user_data)
                    return fresh_user_data
            
            # Return stale data if no refresh possible
            return user_data
            
        elif db_session and user_type:
            # Cache miss with DB fallback
            from app.crud import user as crud_user
            
            if user_type.lower() == 'demo':
                db_user = await crud_user.get_demo_user_by_id(db_session, user_id, user_type=user_type)
            else:
                db_user = await crud_user.get_user_by_id(db_session, user_id, user_type=user_type)
            
            if db_user:
                user_data = {
                    "id": db_user.id,
                    "email": db_user.email,
                    "group_name": db_user.group_name,
                    "leverage": db_user.leverage,
                    "user_type": db_user.user_type,
                    "account_number": getattr(db_user, 'account_number', None),
                    "wallet_balance": db_user.wallet_balance,
                    "margin": db_user.margin,
                    "first_name": getattr(db_user, 'first_name', None),
                    "last_name": getattr(db_user, 'last_name', None),
                    "country": getattr(db_user, 'country', None),
                    "phone_number": getattr(db_user, 'phone_number', None),
                }
                await set_user_data_cache(redis_client, user_id, user_data)
                return user_data
        
        return None
    except Exception as e:
        logger.error(f"Error getting user data cache for user {user_id}: {e}", exc_info=True)
        return None


# --- User Portfolio Cache (Keep as is) ---
async def set_user_portfolio_cache(redis_client: Redis, user_id: int, portfolio_data: Dict[str, Any]):
    """
    Stores dynamic user portfolio data (balance, positions) in Redis.
    This should be called whenever the user's balance or positions change.
    """
    if not redis_client:
        logger.warning(f"Redis client not available for setting user portfolio cache for user {user_id}.")
        return

    key = f"{REDIS_USER_PORTFOLIO_KEY_PREFIX}{user_id}"
    try:
        portfolio_serializable = json.dumps(portfolio_data, cls=DecimalEncoder)
        await redis_client.set(key, portfolio_serializable, ex=USER_PORTFOLIO_CACHE_EXPIRY_SECONDS)
        cache_logger.info(f"Writing portfolio cache for user_id={user_id}: {portfolio_data}")
    except Exception as e:
        cache_logger.error(f"Error setting user portfolio cache for user {user_id}: {e}", exc_info=True)


async def get_user_portfolio_cache(redis_client: Redis, user_id: int) -> Optional[Dict[str, Any]]:
    """
    Retrieves user portfolio data from Redis cache.
    """
    if not redis_client:
        logger.warning(f"Redis client not available for getting user portfolio cache for user {user_id}.")
        return None

    key = f"{REDIS_USER_PORTFOLIO_KEY_PREFIX}{user_id}"
    try:
        portfolio_json = await redis_client.get(key)
        if portfolio_json:
            portfolio_data = json.loads(portfolio_json, object_hook=decode_decimal)
            cache_logger.info(f"Read portfolio cache for user_id={user_id}: {portfolio_data}")
            return portfolio_data
        return None
    except Exception as e:
        cache_logger.error(f"Error getting user portfolio cache for user {user_id}: {e}", exc_info=True)
        return None

async def get_user_positions_from_cache(redis_client: Redis, user_id: int) -> List[Dict[str, Any]]:
    """
    Retrieves only the list of open positions from the user's cached portfolio data.
    Returns an empty list if data is not found or positions list is empty.
    """
    portfolio = await get_user_portfolio_cache(redis_client, user_id)
    if portfolio and 'positions' in portfolio and isinstance(portfolio['positions'], list):
        # The decode_decimal in get_user_portfolio_cache should handle Decimal conversion within positions
        return portfolio['positions']
    return []

# --- New Group Symbol Settings Cache ---

async def set_group_symbol_settings_cache(
    redis_client: Redis, 
    group_name: str, 
    symbol: str, 
    settings: Dict[str, Any]
) -> bool:
    """
    Set group symbol settings in the cache.
    
    Args:
        redis_client: Redis client
        group_name: Group name
        symbol: Symbol name
        settings: Symbol settings
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Add timestamp for cache validation
        settings['_cache_timestamp'] = time.time()
        
        # Serialize and store in Redis
        key = f"{GROUP_SYMBOL_SETTINGS_PREFIX}{group_name}:{symbol}"
        await redis_client.set(
            key, 
            json.dumps(settings, cls=DecimalEncoder),
            ex=GROUP_SYMBOL_SETTINGS_EXPIRY
        )
        return True
    except Exception as e:
        logger.error(f"Error setting group symbol settings cache for {group_name}:{symbol}: {e}", exc_info=True)
        return False

async def get_group_symbol_settings_cache(
    redis_client: Redis, 
    group_name: str, 
    symbol: str
) -> Optional[Dict[str, Any]]:
    """
    Get group symbol settings from cache.
    
    Args:
        redis_client: Redis client
        group_name: Group name
        symbol: Symbol name or "ALL" to get all symbols for the group
        
    Returns:
        Dict[str, Any]: Symbol settings or None if not found
    """
    try:
        if symbol == "ALL":
            # Get all symbols for this group
            pattern = f"{GROUP_SYMBOL_SETTINGS_PREFIX}{group_name}:*"
            keys = await redis_client.keys(pattern)
            
            if not keys:
                return None
                
            result = {}
            for key in keys:
                key_str = key.decode('utf-8')
                symbol_name = key_str.split(':')[-1]
                
                cached_data = await redis_client.get(key)
                if cached_data:
                    symbol_settings = json.loads(cached_data, object_hook=decode_decimal)
                    result[symbol_name] = symbol_settings
            
            return result
        else:
            # Get specific symbol
            key = f"{GROUP_SYMBOL_SETTINGS_PREFIX}{group_name}:{symbol}"
            cached_data = await redis_client.get(key)
            
            if cached_data:
                return json.loads(cached_data, object_hook=decode_decimal)
            
            return None
    except Exception as e:
        logger.error(f"Error getting group symbol settings cache for {group_name}:{symbol}: {e}", exc_info=True)
        return None

# You might also want a function to cache ALL settings for a group,
# or cache ALL group-symbol settings globally if the dataset is small enough.
# For now, fetching per symbol on demand from cache/DB is a good start.

# Add these functions to your app/core/cache.py file

# New key prefix for adjusted market prices per group and symbol
REDIS_ADJUSTED_MARKET_PRICE_KEY_PREFIX = "adjusted_market_price:"

# Increase cache expiry for adjusted market prices to 30 seconds
ADJUSTED_MARKET_PRICE_CACHE_EXPIRY_SECONDS = 30  # Cache for 30 seconds

async def set_adjusted_market_price_cache(
    redis_client: Redis,
    group_name: str,
    symbol: str,
    buy_price: decimal.Decimal,
    sell_price: decimal.Decimal,
    spread_value: decimal.Decimal
) -> bool:
    """
    Set adjusted market price in the cache.
    
    Args:
        redis_client: Redis client
        group_name: Group name
        symbol: Symbol name
        buy_price: Buy price
        sell_price: Sell price
        spread_value: Spread value
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Create price data with timestamp
        price_data = {
            'buy': str(buy_price),
            'sell': str(sell_price),
            'spread': str(spread_value),
            '_cache_timestamp': time.time()
        }
        
        # Serialize and store in Redis
        key = f"{ADJUSTED_MARKET_PRICE_PREFIX}{group_name}:{symbol}"
        await redis_client.set(
            key, 
            json.dumps(price_data),
            ex=ADJUSTED_MARKET_PRICE_EXPIRY
        )
        return True
    except Exception as e:
        logger.error(f"Error setting adjusted market price cache for {group_name}:{symbol}: {e}", exc_info=True)
        return False

async def get_adjusted_market_price_cache(
    redis_client: Redis,
    group_name: str,
    symbol: str
) -> Optional[Dict[str, Any]]:
    """
    Get adjusted market price from cache.
    
    Args:
        redis_client: Redis client
        group_name: Group name
        symbol: Symbol name
        
    Returns:
        Dict[str, Any]: Price data or None if not found
    """
    try:
        key = f"{ADJUSTED_MARKET_PRICE_PREFIX}{group_name}:{symbol}"
        cached_data = await redis_client.get(key)
        
        if cached_data:
            return json.loads(cached_data, object_hook=decode_decimal)
        
        return None
    except Exception as e:
        logger.error(f"Error getting adjusted market price cache for {group_name}:{symbol}: {e}", exc_info=True)
        return None

async def publish_account_structure_changed_event(
    redis_client: Redis,
    user_id: int,
    user_type: str = "live"
) -> bool:
    """
    Publish an event to the account structure change channel.
    
    Args:
        redis_client: Redis client
        user_id: User ID
        user_type: User type
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        event_data = {
            'user_id': user_id,
            'user_type': user_type,
            'timestamp': time.time()
        }
        
        await redis_client.publish(
            REDIS_ACCOUNT_CHANGE_CHANNEL,
            json.dumps(event_data)
        )
        return True
    except Exception as e:
        logger.error(f"Error publishing account structure change event for user {user_id}: {e}", exc_info=True)
        return False

async def invalidate_user_cache(
    redis_client: Redis,
    user_id: int
) -> bool:
    """
    Invalidate all cache for a user.
    
    Args:
        redis_client: Redis client
        user_id: User ID
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Delete user data cache
        user_data_key = f"{USER_DATA_PREFIX}{user_id}"
        await redis_client.delete(user_data_key)
        
        # Delete portfolio cache
        portfolio_key = f"{USER_PORTFOLIO_PREFIX}{user_id}"
        await redis_client.delete(portfolio_key)
        
        return True
    except Exception as e:
        logger.error(f"Error invalidating cache for user {user_id}: {e}", exc_info=True)
        return False

async def get_all_cached_users(redis_client: Redis) -> List[int]:
    """
    Get all user IDs that have cached data.
    
    Args:
        redis_client: Redis client
        
    Returns:
        List[int]: List of user IDs
    """
    try:
        # Get all user data keys
        pattern = f"{USER_DATA_PREFIX}*"
        keys = await redis_client.keys(pattern)
        
        user_ids = []
        for key in keys:
            key_str = key.decode('utf-8')
            user_id_str = key_str.replace(USER_DATA_PREFIX, "")
            try:
                user_id = int(user_id_str)
                user_ids.append(user_id)
            except ValueError:
                continue
                
        return user_ids
    except Exception as e:
        logger.error(f"Error getting all cached users: {e}", exc_info=True)
        return []

# --- Last Known Price Cache ---
async def set_last_known_price(
    redis_client: Redis,
    symbol: str,
    price_data: Dict[str, Any]
) -> bool:
    """
    Set last known price in the cache.
    
    Args:
        redis_client: Redis client
        symbol: Symbol name
        price_data: Price data from Firebase
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Add timestamp for cache validation
        price_data_with_timestamp = price_data.copy()
        price_data_with_timestamp['_cache_timestamp'] = time.time()
        
        # Serialize and store in Redis
        key = f"{LAST_KNOWN_PRICE_PREFIX}{symbol}"
        await redis_client.set(
            key, 
            json.dumps(price_data_with_timestamp),
            ex=LAST_KNOWN_PRICE_EXPIRY
        )
        return True
    except Exception as e:
        logger.error(f"Error setting last known price cache for {symbol}: {e}", exc_info=True)
        return False

async def get_last_known_price(
    redis_client: Redis,
    symbol: str
) -> Optional[Dict[str, Any]]:
    """
    Get last known price from cache.
    
    Args:
        redis_client: Redis client
        symbol: Symbol name
        
    Returns:
        Dict[str, Any]: Price data or None if not found
    """
    try:
        key = f"{LAST_KNOWN_PRICE_PREFIX}{symbol}"
        cached_data = await redis_client.get(key)
        
        if cached_data:
            return json.loads(cached_data, object_hook=decode_decimal)
        
        return None
    except Exception as e:
        logger.error(f"Error getting last known price cache for {symbol}: {e}", exc_info=True)
        return None

# Add these functions after the get_adjusted_market_price_cache function

async def get_live_adjusted_buy_price_for_pair(redis_client: Redis, symbol: str, user_group_name: str) -> Optional[decimal.Decimal]:
    """
    Fetches the live *adjusted* buy price for a given symbol, using group-specific cache.
    Falls back to raw Firebase in-memory market data if Redis cache is cold.

    Cache Key Format: adjusted_market_price:{group}:{symbol}
    Value: {"buy": "1.12345", "sell": "...", "spread_value": "..."}
    """
    cache_key = f"adjusted_market_price:{user_group_name}:{symbol.upper()}"
    try:
        cached_data_json = await redis_client.get(cache_key)
        if cached_data_json:
            price_data = json.loads(cached_data_json)
            buy_price_str = price_data.get("buy")
            if buy_price_str and isinstance(buy_price_str, (str, int, float)):
                return decimal.Decimal(str(buy_price_str))
            else:
                logger.warning(f"'buy' price not found or invalid in cache for {cache_key}: {price_data}")
        else:
            logger.warning(f"No cached adjusted buy price found for key: {cache_key}")
    except (json.JSONDecodeError, decimal.InvalidOperation) as e:
        logger.error(f"Error decoding cached data for {cache_key}: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected error accessing Redis for {cache_key}: {e}", exc_info=True)

    # --- Fallback: Try last known price ---
    try:
        fallback_data = await get_last_known_price(redis_client, symbol)
        # For BUY price, typically use the 'offer' or 'ask' price from market data ('b' in your Firebase structure)
        if fallback_data and 'b' in fallback_data:
            logger.warning(f"Fallback: Using raw last known 'b' price for {symbol}")
            return decimal.Decimal(str(fallback_data['b']))
        else:
            logger.warning(f"Fallback: No 'b' price found in last known price for symbol {symbol}")
    except Exception as fallback_error:
        logger.error(f"Fallback error fetching from last known price for {symbol}: {fallback_error}", exc_info=True)

    return None

async def get_live_adjusted_sell_price_for_pair(redis_client: Redis, symbol: str, user_group_name: str) -> Optional[decimal.Decimal]:
    """
    Fetches the live *adjusted* sell price for a given symbol, using group-specific cache.
    Falls back to raw Firebase in-memory market data if Redis cache is cold.

    Cache Key Format: adjusted_market_price:{group}:{symbol}
    Value: {"buy": "1.12345", "sell": "...", "spread_value": "..."}
    """
    cache_key = f"adjusted_market_price:{user_group_name}:{symbol.upper()}"
    try:
        cached_data_json = await redis_client.get(cache_key)
        if cached_data_json:
            price_data = json.loads(cached_data_json)
            sell_price_str = price_data.get("sell")
            if sell_price_str and isinstance(sell_price_str, (str, int, float)):
                return decimal.Decimal(str(sell_price_str))
            else:
                logger.warning(f"'sell' price not found or invalid in cache for {cache_key}: {price_data}")
        else:
            logger.warning(f"No cached adjusted sell price found for key: {cache_key}")
    except (json.JSONDecodeError, decimal.InvalidOperation) as e:
        logger.error(f"Error decoding cached data for {cache_key}: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected error accessing Redis for {cache_key}: {e}", exc_info=True)

    # --- Fallback: Try last known price ---
    try:
        fallback_data = await get_last_known_price(redis_client, symbol)
        # For SELL price, typically use the 'bid' price from market data ('o' in your Firebase structure)
        if fallback_data and 'o' in fallback_data:
            logger.warning(f"Fallback: Using raw last known 'o' price for {symbol}")
            return decimal.Decimal(str(fallback_data['o']))
        else:
            logger.warning(f"Fallback: No 'o' price found in last known price for symbol {symbol}")
    except Exception as fallback_error:
        logger.error(f"Fallback error fetching from last known price for {symbol}: {fallback_error}", exc_info=True)

    return None

async def set_group_settings_cache(redis_client: Redis, group_name: str, settings: Dict[str, Any]):
    """
    Stores general group settings in Redis.
    Settings include sending_orders, etc.
    """
    if not redis_client:
        logger.warning(f"Redis client not available for setting group settings cache for group '{group_name}'.")
        return

    key = f"{REDIS_GROUP_SETTINGS_KEY_PREFIX}{group_name.lower()}" # Use lower for consistency
    try:
        settings_serializable = json.dumps(settings, cls=DecimalEncoder)
        await redis_client.set(key, settings_serializable, ex=GROUP_SETTINGS_CACHE_EXPIRY_SECONDS)
        cache_logger.debug(f"Group settings cached for group '{group_name}'.")
    except Exception as e:
        cache_logger.error(f"Error setting group settings cache for group '{group_name}': {e}", exc_info=True)

async def get_group_settings_cache(redis_client: Redis, group_name: str) -> Optional[Dict[str, Any]]:
    """
    Retrieves general group settings from Redis cache.
    Returns None if no settings found for the specified group.
    
    Expected settings include:
    - sending_orders: str (e.g., 'barclays' or other values)
    - other group-level settings
    """
    if not redis_client:
        cache_logger.warning(f"Redis client not available for getting group settings cache for group '{group_name}'.")
        return None

    key = f"{REDIS_GROUP_SETTINGS_KEY_PREFIX}{group_name.lower()}" # Use lower for consistency
    try:
        settings_json = await redis_client.get(key)
        if settings_json:
            settings = json.loads(settings_json, object_hook=decode_decimal)
            cache_logger.debug(f"Group settings retrieved from cache for group '{group_name}'.")
            return settings
        cache_logger.debug(f"Group settings not found in cache for group '{group_name}'.")
        return None
    except Exception as e:
        cache_logger.error(f"Error getting group settings cache for group '{group_name}': {e}", exc_info=True)
        return None