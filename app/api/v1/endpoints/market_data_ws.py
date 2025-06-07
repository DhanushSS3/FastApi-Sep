# app/api/v1/endpoints/market_data_ws.py

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, status, Query, WebSocketException, Depends
import asyncio
import logging
from app.core.logging_config import websocket_logger
from sqlalchemy.orm import Session
from app.crud.crud_order import get_order_model
import json
from typing import Dict, Any, List, Optional, Set
import decimal
from starlette.websockets import WebSocketState
from decimal import Decimal


# Import necessary components for DB interaction and authentication
from sqlalchemy.ext.asyncio import AsyncSession
from app.database.session import get_db, AsyncSessionLocal # Import AsyncSessionLocal for db sessions in tasks
from app.crud import user as crud_user
from app.crud import group as crud_group
from app.crud import crud_order

# Import security functions for token validation
from app.core.security import decode_token

# Import the Redis client type
from redis.asyncio import Redis

# Import the caching helper functions
from app.core.cache import (
    set_user_data_cache, get_user_data_cache,
    set_user_portfolio_cache, get_user_portfolio_cache,
    set_adjusted_market_price_cache, get_adjusted_market_price_cache,
    set_group_symbol_settings_cache, get_group_symbol_settings_cache,
    set_last_known_price, get_last_known_price,
    DecimalEncoder, decode_decimal,
    LAST_KNOWN_PRICE_PREFIX
)

# Import the dependency to get the Redis client
from app.dependencies.redis_client import get_redis_client

# Import the shared state for the Redis publish queue (for Firebase data -> Redis)
from app.shared_state import redis_publish_queue

# Import the new portfolio calculation service
from app.services.portfolio_calculator import calculate_user_portfolio
from app.services.portfolio_background_worker import initialize_user_cache, notify_account_change

# Import the Symbol and ExternalSymbolInfo models
from app.database.models import Symbol, ExternalSymbolInfo, User, DemoUser # Import User/DemoUser for type hints
from sqlalchemy.future import select

# Configure logging for this module
logger = websocket_logger

# Redis channel for RAW market data updates from Firebase via redis_publisher_task
REDIS_MARKET_DATA_CHANNEL = 'market_data_updates'


router = APIRouter(
    tags=["market_data_ws"]
)


async def _calculate_and_cache_adjusted_prices(
    raw_market_data: Dict[str, Any],
    group_name: str,
    relevant_symbols: Set[str],
    group_settings: Dict[str, Any],
    redis_client: Redis
) -> Dict[str, Dict[str, float]]:
    """
    Calculates adjusted prices based on group settings and caches them.
    Returns a dictionary of adjusted prices for symbols in raw_market_data.
    """
    adjusted_prices_payload = {}
    
    # Debug logging
    logger.debug(f"Raw market data keys: {list(raw_market_data.keys())}")
    logger.debug(f"Relevant symbols: {list(relevant_symbols)}")
    logger.debug(f"Group settings keys: {list(group_settings.keys())}")
    
    # Get all unique currencies from the group settings
    all_currencies = set()
    for symbol in group_settings.keys():
        if len(symbol) == 6:  # Only process 6-character currency pairs
            all_currencies.add(symbol[:3])  # First currency
            all_currencies.add(symbol[3:])  # Second currency
    
    # Add USD conversion pairs to relevant symbols
    for currency in all_currencies:
        if currency != 'USD':
            relevant_symbols.add(f"{currency}USD")  # Direct conversion
            relevant_symbols.add(f"USD{currency}")  # Indirect conversion
    
    # Debug logging
    logger.debug(f"Expanded relevant symbols: {list(relevant_symbols)}")
    
    # If raw_market_data is empty, try to get data from last_known_price
    if not raw_market_data:
        logger.warning(f"Raw market data is empty, trying to use last known prices")
        for symbol in relevant_symbols:
            try:
                last_known = await get_last_known_price(redis_client, symbol)
                if last_known:
                    raw_market_data[symbol] = last_known
                    logger.debug(f"Using last known price for {symbol}: {last_known}")
            except Exception as e:
                logger.error(f"Error getting last known price for {symbol}: {e}")
    
    for symbol, prices in raw_market_data.items():
        symbol_upper = symbol.upper()
        if symbol_upper not in relevant_symbols or not isinstance(prices, dict):
            continue

        # --- Persist last known price for this symbol ---
        await set_last_known_price(redis_client, symbol_upper, prices)

        # Firebase 'b' is Ask, 'o' is Bid
        raw_ask_price = prices.get('b')  # Ask from Firebase
        raw_bid_price = prices.get('o')  # Bid from Firebase
        
        # Debug logging
        logger.debug(f"Processing symbol {symbol_upper}: raw_ask={raw_ask_price}, raw_bid={raw_bid_price}")
        
        symbol_group_settings = group_settings.get(symbol_upper)

        if raw_ask_price is not None and raw_bid_price is not None and symbol_group_settings:
            try:
                ask_decimal = Decimal(str(raw_ask_price))
                bid_decimal = Decimal(str(raw_bid_price))
                spread_setting = Decimal(str(symbol_group_settings.get('spread', 0)))
                spread_pip_setting = Decimal(str(symbol_group_settings.get('spread_pip', 0)))
                
                configured_spread_amount = spread_setting * spread_pip_setting
                half_spread = configured_spread_amount / Decimal(2)

                # Adjusted prices for user display and trading
                adjusted_buy_price = ask_decimal + half_spread  # User buys at adjusted ask
                adjusted_sell_price = bid_decimal - half_spread  # User sells at adjusted bid

                effective_spread_price_units = adjusted_buy_price - adjusted_sell_price
                effective_spread_in_pips = Decimal("0.0")
                if spread_pip_setting > Decimal("0.0"):
                    effective_spread_in_pips = effective_spread_price_units / spread_pip_setting

                # Cache the adjusted prices
                await set_adjusted_market_price_cache(
                    redis_client=redis_client,
                    group_name=group_name,
                    symbol=symbol_upper,
                    buy_price=adjusted_buy_price,
                    sell_price=adjusted_sell_price,
                    spread_value=configured_spread_amount
                )

                # Add to payload for immediate response
                adjusted_prices_payload[symbol_upper] = {
                    'buy': float(adjusted_buy_price),
                    'sell': float(adjusted_sell_price),
                    'spread': float(effective_spread_in_pips)
                }

                logger.debug(f"Adjusted prices for {symbol_upper}: Buy={adjusted_buy_price}, Sell={adjusted_sell_price}, Spread={effective_spread_in_pips}")

            except Exception as e:
                logger.error(f"Error adjusting price for {symbol_upper} in group {group_name}: {e}", exc_info=True)
                # Optionally send raw if calculation fails
                if raw_ask_price is not None and raw_bid_price is not None:
                    raw_spread = (Decimal(str(raw_ask_price)) - Decimal(str(raw_bid_price)))
                    raw_spread_pips = raw_spread / spread_pip_setting if spread_pip_setting > Decimal("0.0") else raw_spread
                    adjusted_prices_payload[symbol_upper] = {
                        'buy': float(raw_ask_price),
                        'sell': float(raw_bid_price),
                        'spread': float(raw_spread_pips)
                    }
        else:
            logger.warning(f"Cannot process {symbol_upper}: raw_ask={raw_ask_price}, raw_bid={raw_bid_price}, has_settings={symbol_group_settings is not None}")

    # Debug logging
    logger.debug(f"Final adjusted prices payload has {len(adjusted_prices_payload)} symbols")
    
    return adjusted_prices_payload


async def _get_full_portfolio_details(
    user_id: int,
    group_name: str, # User's group name
    redis_client: Redis,
) -> Optional[Dict[str, Any]]:
    """
    Fetches all necessary data from cache and calculates the full user portfolio.
    """
    user_data = await get_user_data_cache(redis_client, user_id)
    user_portfolio_cache = await get_user_portfolio_cache(redis_client, user_id) # Contains positions
    group_symbol_settings_all = await get_group_symbol_settings_cache(redis_client, group_name, "ALL")

    if not user_data:
        logger.warning(f"Missing user_data for user {user_id}, cannot calculate portfolio.")
        return None
        
    # If group settings are missing, use empty dict as fallback
    if not group_symbol_settings_all:
        logger.warning(f"Missing group_settings for group {group_name}. Using empty fallback.")
        group_symbol_settings_all = {}

    open_positions = user_portfolio_cache.get('positions', []) if user_portfolio_cache else []
    
    # Construct the market_prices dict for calculate_user_portfolio using cached adjusted prices
    market_prices_for_calc = {}
    relevant_symbols_for_group = set(group_symbol_settings_all.keys())
    for sym_upper in relevant_symbols_for_group:
        cached_adj_price = await get_adjusted_market_price_cache(redis_client, group_name, sym_upper)
        if cached_adj_price:
            # calculate_user_portfolio expects 'buy' and 'sell' keys
            market_prices_for_calc[sym_upper] = {
                'buy': Decimal(str(cached_adj_price.get('buy'))),
                'sell': Decimal(str(cached_adj_price.get('sell')))
            }

    portfolio_metrics = await calculate_user_portfolio(
        user_data=user_data, # This is a dict
        open_positions=open_positions, # List of dicts
        adjusted_market_prices=market_prices_for_calc, # Dict of symbol -> {'buy': Decimal, 'sell': Decimal}
        group_symbol_settings=group_symbol_settings_all, # Dict of symbol -> settings dict
        redis_client=redis_client
    )
    
    # Ensure all values in portfolio_metrics are JSON serializable
    account_data_payload = {
        "balance": portfolio_metrics.get("balance", "0.0"),
        "equity": portfolio_metrics.get("equity", "0.0"),
        "margin": user_data.get("margin", "0.0"), # User's OVERALL margin from cached user_data
        "free_margin": portfolio_metrics.get("free_margin", "0.0"),
        "profit_loss": portfolio_metrics.get("profit_loss", "0.0"),
        "margin_level": portfolio_metrics.get("margin_level", "0.0"),
        "positions": portfolio_metrics.get("positions", []) # This should be serializable list of dicts
    }
    return account_data_payload


async def per_connection_redis_listener(
    websocket: WebSocket,
    user_id: int,
    group_name: str,
    redis_client: Redis,
    db: AsyncSession
):
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(REDIS_MARKET_DATA_CHANNEL)
    logger.info(f"User {user_id}: Subscribed to {REDIS_MARKET_DATA_CHANNEL}")
    
    # Send initial market data update
    try:
        # Get all last known prices
        last_prices_keys = await redis_client.keys(f"{LAST_KNOWN_PRICE_PREFIX}*")
        
        if not last_prices_keys:
            # If no last known prices, publish test data
            logger.warning(f"No market data found for initial update, publishing test data")
            await publish_test_market_data(redis_client)
            last_prices_keys = await redis_client.keys(f"{LAST_KNOWN_PRICE_PREFIX}*")
        
        # Construct initial market data
        initial_market_data = {"type": "market_data_update"}
        
        for key in last_prices_keys:
            symbol = key.decode('utf-8').replace(LAST_KNOWN_PRICE_PREFIX, "")
            price_data_json = await redis_client.get(key)
            if price_data_json:
                try:
                    price_data = json.loads(price_data_json)
                    initial_market_data[symbol] = price_data
                except:
                    logger.error(f"Error parsing price data for {symbol}")
        
        # Process the initial market data
        if len(initial_market_data) > 1:  # More than just the "type" field
            logger.info(f"Sending initial market data update with {len(initial_market_data)-1} symbols")
            
            # Get group settings
            group_settings = await get_group_symbol_settings_cache(redis_client, group_name, "ALL")
            
            # If group settings are still None, try to update them
            if group_settings is None:
                logger.warning(f"Group settings not found for {group_name}, attempting to initialize...")
                await update_group_symbol_settings(group_name, db, redis_client)
                group_settings = await get_group_symbol_settings_cache(redis_client, group_name, "ALL")
                
            # If still None after update attempt, use an empty dict as fallback
            if group_settings is None:
                logger.error(f"Failed to get group settings for {group_name}, using empty fallback")
                group_settings = {}
                
            relevant_symbols = set(group_settings.keys())
            
            # Extract price data content
            price_data_content = {k: v for k, v in initial_market_data.items() if k != "type"}

            # Update and cache adjusted prices
            adjusted_prices = await _calculate_and_cache_adjusted_prices(
                raw_market_data=price_data_content,
                group_name=group_name,
                relevant_symbols=relevant_symbols,
                group_settings=group_settings,
                redis_client=redis_client
            )

            # Get open positions from portfolio cache
            portfolio_cache = await get_user_portfolio_cache(redis_client, user_id)
            positions = portfolio_cache.get("positions", []) if portfolio_cache else []

            # Fallback to DB if positions are empty
            if not positions:
                # Retrieve user_type from cached user_data to get the correct order model
                user_data_from_cache = await get_user_data_cache(redis_client, user_id)
                user_type_from_cache = user_data_from_cache.get("user_type", "live") if user_data_from_cache else "live"
                order_model = get_order_model(user_type_from_cache)  # Use user_type from cache
                open_positions_orm = await crud_order.get_all_open_orders_by_user_id(db, user_id, order_model)
                for pos in open_positions_orm:
                    positions.append({
                        "order_id": pos.order_id,
                        "order_company_name": pos.order_company_name,
                        "order_type": pos.order_type,
                        "order_quantity": str(pos.order_quantity),
                        "order_price": str(pos.order_price),
                        "margin": str(pos.margin),
                        "contract_value": str(pos.contract_value),
                        "stop_loss": str(pos.stop_loss) if pos.stop_loss is not None else None, # Add stop_loss
                        "take_profit": str(pos.take_profit) if pos.take_profit is not None else None, # Add take_profit
                        "commission": "0.0" # Ensure commission is present
                    })

            # Fetch pending orders
            pending_statuses = ["BUY_LIMIT", "SELL_LIMIT", "BUY_STOP", "SELL_STOP", "PENDING"]
            user_data_from_cache = await get_user_data_cache(redis_client, user_id)
            user_type = user_data_from_cache.get("user_type", "live") if user_data_from_cache else "live" # Default to "live"
            order_model = get_order_model(user_type) # Get correct model based on user_type

            pending_orders_orm = await crud_order.get_orders_by_user_id_and_statuses(db, user_id, pending_statuses, order_model)
            
            pending_orders_data = []
            for po in pending_orders_orm:
                po_dict = {attr: str(v) if isinstance(v := getattr(po, attr, None), Decimal) else v
                           for attr in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 'order_price', 'margin', 'contract_value', 'stop_loss', 'take_profit']}
                po_dict['commission'] = "0.0" # Ensure commission is present
                pending_orders_data.append(po_dict)

            # Get full portfolio details to get balance, margin etc.
            account_summary_data = await _get_full_portfolio_details(user_id, group_name, redis_client)

            if websocket.client_state == WebSocketState.CONNECTED:
                await websocket.send_text(json.dumps({
                    "type": "market_update",
                    "data": {
                        "market_prices": adjusted_prices,
                        "account_summary": {
                            "balance": account_summary_data.get("balance", "0.0") if account_summary_data else "0.0",
                            "margin": account_summary_data.get("margin", "0.0") if account_summary_data else "0.0",
                            "open_orders": positions, # Existing open positions
                            "pending_orders": pending_orders_data
                        }
                    }
                }, cls=DecimalEncoder))
                logger.info(f"User {user_id}: Sent initial market data update with {len(adjusted_prices)} symbols")
    except Exception as e:
        logger.error(f"User {user_id}: Error sending initial market data: {e}", exc_info=True)

    try:
        while websocket.client_state == WebSocketState.CONNECTED:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if message is None:
                await asyncio.sleep(0.01)
                continue

            if message['channel'] == REDIS_MARKET_DATA_CHANNEL:
                try:
                    raw_market_data_update = json.loads(message['data'], object_hook=decode_decimal)
                    if raw_market_data_update.get("type") == "market_data_update":
                        price_data_content = {k: v for k, v in raw_market_data_update.items() if k != "type"}

                        # Get group settings
                        group_settings = await get_group_symbol_settings_cache(redis_client, group_name, "ALL")
                        
                        # If group settings are still None, try to update them
                        if group_settings is None:
                            logger.warning(f"Group settings not found for {group_name}, attempting to initialize...")
                            await update_group_symbol_settings(group_name, db, redis_client)
                            group_settings = await get_group_symbol_settings_cache(redis_client, group_name, "ALL")
                            
                        # If still None after update attempt, use an empty dict as fallback
                        if group_settings is None:
                            logger.error(f"Failed to get group settings for {group_name}, using empty fallback")
                            group_settings = {}
                            
                        relevant_symbols = set(group_settings.keys())

                        # Update and cache adjusted prices
                        adjusted_prices = await _calculate_and_cache_adjusted_prices(
                            raw_market_data=price_data_content,
                            group_name=group_name,
                            relevant_symbols=relevant_symbols,
                            group_settings=group_settings,
                            redis_client=redis_client
                        )

                        # Get open positions from portfolio cache
                        portfolio_cache = await get_user_portfolio_cache(redis_client, user_id)
                        positions = portfolio_cache.get("positions", []) if portfolio_cache else []

                        # Fallback to DB if positions are empty
                        if not positions:
                            # Retrieve user_type from cached user_data to get the correct order model
                            user_data_from_cache = await get_user_data_cache(redis_client, user_id)
                            user_type_from_cache = user_data_from_cache.get("user_type", "live") if user_data_from_cache else "live"
                            order_model = get_order_model(user_type_from_cache)  # Use user_type from cache
                            open_positions_orm = await crud_order.get_all_open_orders_by_user_id(db, user_id, order_model)
                            for pos in open_positions_orm:
                                positions.append({
                                    "order_id": pos.order_id,
                                    "order_company_name": pos.order_company_name,
                                    "order_type": pos.order_type,
                                    "order_quantity": str(pos.order_quantity),
                                    "order_price": str(pos.order_price),
                                    "margin": str(pos.margin),
                                    "contract_value": str(pos.contract_value),
                                    "stop_loss": str(pos.stop_loss) if pos.stop_loss is not None else None, # Add stop_loss
                                    "take_profit": str(pos.take_profit) if pos.take_profit is not None else None, # Add take_profit
                                    "commission": "0.0" # Ensure commission is present
                                })

                        # Fetch pending orders
                        pending_statuses = ["BUY_LIMIT", "SELL_LIMIT", "BUY_STOP", "SELL_STOP", "PENDING"]
                        # user_type is not directly available here, assume "live" or derive from user_id if possible
                        # For now, will assume user_type can be derived from the user_id (which is db_user_id from authentication)
                        # We need the user_type to get the correct order model.
                        # Let's get the user_type from the user_data_cache which is set at connection time.
                        user_data_from_cache = await get_user_data_cache(redis_client, user_id)
                        user_type = user_data_from_cache.get("user_type", "live") if user_data_from_cache else "live" # Default to "live"
                        order_model = get_order_model(user_type) # Get correct model based on user_type

                        pending_orders_orm = await crud_order.get_orders_by_user_id_and_statuses(db, user_id, pending_statuses, order_model)
                        
                        pending_orders_data = []
                        for po in pending_orders_orm:
                            po_dict = {attr: str(v) if isinstance(v := getattr(po, attr, None), Decimal) else v
                                       for attr in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 'order_price', 'margin', 'contract_value', 'stop_loss', 'take_profit']}
                            po_dict['commission'] = "0.0" # Ensure commission is present
                            pending_orders_data.append(po_dict)

                        # Get full portfolio details to get balance, margin etc.
                        account_summary_data = await _get_full_portfolio_details(user_id, group_name, redis_client)

                        if websocket.client_state == WebSocketState.CONNECTED:
                            await websocket.send_text(json.dumps({
                                "type": "market_update",
                                "data": {
                                    "market_prices": adjusted_prices,
                                    "account_summary": {
                                        "balance": account_summary_data.get("balance", "0.0") if account_summary_data else "0.0",
                                        "margin": account_summary_data.get("margin", "0.0") if account_summary_data else "0.0",
                                        "open_orders": positions, # Existing open positions
                                        "pending_orders": pending_orders_data
                                    }
                                }
                            }, cls=DecimalEncoder))
                            logger.debug(f"User {user_id}: Sent positions + market prices update")

                except Exception as e:
                    logger.error(f"User {user_id}: Error in market data processing: {e}", exc_info=True)

    except WebSocketDisconnect:
        logger.info(f"User {user_id}: WebSocket disconnected.")
    except Exception as e:
        logger.error(f"User {user_id}: Unexpected error: {e}", exc_info=True)
    finally:
        await pubsub.unsubscribe(REDIS_MARKET_DATA_CHANNEL)
        await pubsub.close()
        logger.info(f"User {user_id}: Unsubscribed from Redis and cleaned up.")


@router.websocket("/ws/market-data")
async def websocket_endpoint(websocket: WebSocket, db: Session = Depends(get_db)):
    logger.info("--- MINIMAL TEST: ENTERED websocket_endpoint ---")
    for handler in logger.handlers:
        handler.flush()
    db_user_instance: Optional[User | DemoUser] = None

    # Extract token from query params
    token = websocket.query_params.get("token")
    if token is None:
        logger.warning(f"WebSocket connection attempt without token from {websocket.client.host}:{websocket.client.port}")
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Missing token")
        return

    # Clean up token - remove any trailing quotes or encoded characters
    token = token.strip('"').strip("'").replace('%22', '').replace('%27', '')

    # Initialize Redis client
    redis_client = await get_redis_client()

    try:
        from jose import JWTError, ExpiredSignatureError
        try:
            payload = decode_token(token)
            user_id = payload.get("sub")
            user_type = payload.get("user_type", "live")
            
            if not user_id:
                logger.warning(f"WebSocket auth failed: Invalid token payload - missing sub (user_id)")
                await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Invalid token: User ID missing")
                return

            # Strictly fetch from correct table based on user_type and user_id
            logger.info(f"WebSocket auth: token payload={payload}, user_id={user_id}, user_type={user_type}")
            if user_type == "demo":
                logger.info(f"WebSocket auth: About to call get_demo_user_by_id with user_id={user_id}, user_type={user_type}")
                db_user_instance = await crud_user.get_demo_user_by_id(db, user_id, user_type=user_type)
                logger.info(f"WebSocket auth: get_demo_user_by_id({user_id}, {user_type}) returned: {db_user_instance}")
            else:
                logger.info(f"WebSocket auth: About to call get_user_by_id with user_id={user_id}, user_type={user_type}")
                db_user_instance = await crud_user.get_user_by_id(db, user_id, user_type=user_type)
                logger.info(f"WebSocket auth: get_user_by_id({user_id}, {user_type}) returned: {db_user_instance}")

            if not db_user_instance:
                logger.warning(f"Authentication failed for user_id {user_id} (type {user_type}): User not found in correct table.")
                await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="User not found")
                return
            
            if not getattr(db_user_instance, 'isActive', True):
                logger.warning(f"Authentication failed for user ID {user_id}: User inactive.")
                await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="User inactive")
                return

        except ExpiredSignatureError:
            logger.warning(f"WebSocket auth failed: Token expired for {websocket.client.host}:{websocket.client.port}")
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Token expired")
            return
        except JWTError as jwt_err:
            logger.warning(f"WebSocket auth failed: JWT error for {websocket.client.host}:{websocket.client.port}: {str(jwt_err)}")
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Invalid token")
            return

        group_name = getattr(db_user_instance, 'group_name', 'default')
        
        # Ensure group settings are initialized
        await update_group_symbol_settings(group_name, db, redis_client)
        
        # Initialize user cache using the background worker's function
        cache_initialized = await initialize_user_cache(
            user_id=int(user_id),
            user_type=user_type,
            redis_client=redis_client,
            db_session=db
        )
        
        if not cache_initialized:
            logger.error(f"Failed to initialize cache for user {user_id}. Closing WebSocket connection.")
            await websocket.close(code=status.WS_1011_INTERNAL_ERROR, reason="Failed to initialize user data")
            return
            
        # Add user to the background worker's active users
        await notify_account_change(redis_client, int(user_id), user_type)
        
        # Ensure we have market data by publishing test data if needed
        last_prices = await redis_client.keys(f"{LAST_KNOWN_PRICE_PREFIX}*")
        if not last_prices:
            logger.info(f"No market data found, publishing test data")
            await publish_test_market_data(redis_client)

    except Exception as e:
        logger.error(f"Unexpected WS auth error for {websocket.client.host}:{websocket.client.port}: {e}", exc_info=True)
        if websocket.client_state != WebSocketState.DISCONNECTED:
            await websocket.close(code=status.WS_1011_INTERNAL_ERROR, reason="Authentication error")
        return

    # Accept the WebSocket connection
    await websocket.accept()
    logger.info(f"WebSocket connection accepted for user {user_id} (Group: {group_name}).")

    # Create and manage the per-connection Redis listener task
    listener_task = asyncio.create_task(
        per_connection_redis_listener(websocket, int(user_id), group_name, redis_client, db)
    )

    try:
        while True:
            await websocket.receive_text()

    except WebSocketDisconnect:
        logger.info(f"User {user_id}: WebSocket disconnected by client.")
    except Exception as e:
        logger.error(f"User {user_id}: Error in main WebSocket loop: {e}", exc_info=True)
    finally:
        logger.info(f"User {user_id}: Cleaning up WebSocket connection.")
        if not listener_task.done():
            listener_task.cancel()
            try:
                await listener_task
            except asyncio.CancelledError:
                logger.info(f"User {user_id}: Listener task successfully cancelled.")
            except Exception as task_e:
                logger.error(f"User {user_id}: Error during listener task cleanup: {task_e}", exc_info=True)
        
        if websocket.client_state != WebSocketState.DISCONNECTED:
            try:
                await websocket.close(code=status.WS_1000_NORMAL_CLOSURE)
            except Exception as close_e:
                logger.error(f"User {user_id}: Error explicitly closing WebSocket: {close_e}", exc_info=True)
        logger.info(f"User {user_id}: WebSocket connection fully closed.")


# --- Helper Function to Update Group Symbol Settings (used by websocket_endpoint) ---
async def update_group_symbol_settings(group_name: str, db: AsyncSession, redis_client: Redis):
    if not group_name:
        logger.warning("Cannot update group-symbol settings: group_name is missing.")
        return
    try:
        group_settings_list = await crud_group.get_groups(db, search=group_name)
        if not group_settings_list:
             logger.warning(f"No group settings found in DB for group '{group_name}'.")
             return
        for group_setting in group_settings_list:
            symbol_name = getattr(group_setting, 'symbol', None)
            if symbol_name:
                settings = {
                    "commision_type": getattr(group_setting, 'commision_type', None),
                    "commision_value_type": getattr(group_setting, 'commision_value_type', None),
                    "type": getattr(group_setting, 'type', None),
                    "pip_currency": getattr(group_setting, 'pip_currency', "USD"),
                    "show_points": getattr(group_setting, 'show_points', None),
                    "swap_buy": getattr(group_setting, 'swap_buy', decimal.Decimal(0.0)),
                    "swap_sell": getattr(group_setting, 'swap_sell', decimal.Decimal(0.0)),
                    "commision": getattr(group_setting, 'commision', decimal.Decimal(0.0)),
                    "margin": getattr(group_setting, 'margin', decimal.Decimal(0.0)),
                    "spread": getattr(group_setting, 'spread', decimal.Decimal(0.0)),
                    "deviation": getattr(group_setting, 'deviation', decimal.Decimal(0.0)),
                    "min_lot": getattr(group_setting, 'min_lot', decimal.Decimal(0.0)),
                    "max_lot": getattr(group_setting, 'max_lot', decimal.Decimal(0.0)),
                    "pips": getattr(group_setting, 'pips', decimal.Decimal(0.0)),
                    "spread_pip": getattr(group_setting, 'spread_pip', decimal.Decimal(0.0)),
                    "contract_size": getattr(group_setting, 'contract_size', decimal.Decimal("100000")),
                }
                # Fetch profit_currency from Symbol model
                symbol_obj_stmt = select(Symbol).filter_by(name=symbol_name.upper())
                symbol_obj_result = await db.execute(symbol_obj_stmt)
                symbol_obj = symbol_obj_result.scalars().first()
                if symbol_obj and symbol_obj.profit_currency:
                    settings["profit_currency"] = symbol_obj.profit_currency
                else: # Fallback
                    settings["profit_currency"] = getattr(group_setting, 'pip_currency', 'USD')
                # Fetch contract_size from ExternalSymbolInfo (overrides group if found)
                external_symbol_obj_stmt = select(ExternalSymbolInfo).filter_by(fix_symbol=symbol_name) # Case-sensitive match?
                external_symbol_obj_result = await db.execute(external_symbol_obj_stmt)
                external_symbol_obj = external_symbol_obj_result.scalars().first()
                if external_symbol_obj and external_symbol_obj.contract_size is not None:
                    settings["contract_size"] = external_symbol_obj.contract_size
                
                # Cache the settings
                await set_group_symbol_settings_cache(redis_client, group_name, symbol_name.upper(), settings)
                logger.debug(f"Cached settings for {symbol_name.upper()} in group {group_name}")
            else:
                 logger.warning(f"Group setting symbol is None for group '{group_name}'.")
        logger.debug(f"Cached/updated group-symbol settings for group '{group_name}'.")
    except Exception as e:
        logger.error(f"Error caching group-symbol settings for '{group_name}': {e}", exc_info=True)


# --- Redis Publisher Task (Publishes from Firebase queue to general market data channel) ---
async def redis_publisher_task(redis_client: Redis):
    logger.info("Redis publisher task started. Publishing to channel '%s'.", REDIS_MARKET_DATA_CHANNEL)
    if not redis_client:
        logger.critical("Redis client not provided for publisher task. Exiting.")
        return
    try:
        # Publish initial test data to ensure the channel is working
        await publish_test_market_data(redis_client)
        
        while True:
            raw_market_data_message = await redis_publish_queue.get()
            if raw_market_data_message is None: # Shutdown signal
                logger.info("Publisher task received shutdown signal. Exiting.")
                break
            try:
                # Filter out the _timestamp key before publishing, ensure "type" is added
                message_to_publish_data = {k: v for k, v in raw_market_data_message.items() if k != '_timestamp'}
                if message_to_publish_data: # Ensure there's data other than just timestamp
                     message_to_publish_data["type"] = "market_data_update" # Standardize type for raw updates
                     message_to_publish = json.dumps(message_to_publish_data, cls=DecimalEncoder)
                     logger.debug(f"Publishing market data with {len(message_to_publish_data)-1} symbols")
                else: # Skip if only timestamp was present
                     redis_publish_queue.task_done()
                     continue
            except Exception as e:
                logger.error(f"Publisher failed to serialize message: {e}. Skipping.", exc_info=True)
                redis_publish_queue.task_done()
                continue
            try:
                await redis_client.publish(REDIS_MARKET_DATA_CHANNEL, message_to_publish)
                logger.debug(f"Published market data update to {REDIS_MARKET_DATA_CHANNEL}")
            except Exception as e:
                logger.error(f"Publisher failed to publish to Redis: {e}. Msg: {message_to_publish[:100]}...", exc_info=True)
            redis_publish_queue.task_done()
            
            # If queue is empty, publish test data every 30 seconds to keep the channel active
            if redis_publish_queue.empty():
                await asyncio.sleep(30)
                await publish_test_market_data(redis_client)
    except asyncio.CancelledError:
        logger.info("Redis publisher task cancelled.")
    except Exception as e:
        logger.critical(f"FATAL ERROR: Redis publisher task failed: {e}", exc_info=True)
    finally:
        logger.info("Redis publisher task finished.")

async def publish_test_market_data(redis_client: Redis):
    """
    Publish test market data to ensure the channel is working.
    This is useful when there's no real market data coming in.
    """
    try:
        # Create test data for common forex pairs
        test_data = {
            "EURUSD": {"b": "1.0950", "o": "1.0940"},  # b is ask, o is bid
            "GBPUSD": {"b": "1.2650", "o": "1.2640"},
            "USDJPY": {"b": "110.50", "o": "110.40"},
            "AUDUSD": {"b": "0.6750", "o": "0.6740"},
            "USDCAD": {"b": "1.3550", "o": "1.3540"},
            "NZDUSD": {"b": "0.6150", "o": "0.6140"},
            "USDCHF": {"b": "0.9050", "o": "0.9040"},
            "EURJPY": {"b": "121.00", "o": "120.90"},
            "GBPJPY": {"b": "140.00", "o": "139.90"},
            "EURGBP": {"b": "0.8650", "o": "0.8640"},
            "type": "market_data_update"
        }
        
        message = json.dumps(test_data, cls=DecimalEncoder)
        await redis_client.publish(REDIS_MARKET_DATA_CHANNEL, message)
        logger.info(f"Published test market data with {len(test_data)-1} symbols")
        
        # Also store as last known prices
        for symbol, prices in test_data.items():
            if symbol != "type":
                await set_last_known_price(redis_client, symbol, prices)
                
        return True
    except Exception as e:
        logger.error(f"Failed to publish test market data: {e}", exc_info=True)
        return False