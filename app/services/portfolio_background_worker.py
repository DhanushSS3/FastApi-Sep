import asyncio
import json
import logging
from decimal import Decimal
from typing import Dict, Set, Any, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis

from app.database.session import AsyncSessionLocal
from app.crud import crud_order, user as crud_user
from app.crud.crud_order import get_order_model
from app.core.cache import (
    get_user_data_cache,
    set_user_data_cache,
    get_user_portfolio_cache,
    set_user_portfolio_cache,
    get_group_symbol_settings_cache,
    get_adjusted_market_price_cache,
    get_last_known_price,
    publish_account_structure_changed_event,
    DecimalEncoder,
    decode_decimal
)
from app.services.portfolio_calculator import calculate_user_portfolio

# Configure logging for this module
logger = logging.getLogger("portfolio-worker")
if not logger.handlers:
    file_handler = logging.FileHandler('logs/portfolio_worker.log')
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.setLevel(logging.INFO)

# Redis channel for market data updates
REDIS_MARKET_DATA_CHANNEL = 'market_data_updates'
# Redis channel for account structure changes (order placed, closed, etc.)
REDIS_ACCOUNT_CHANGE_CHANNEL = 'account_structure_change'

# Global state for the background worker
active_users: Set[int] = set()
user_types: Dict[int, str] = {}
stop_event = asyncio.Event()
worker_task = None

async def initialize_user_cache(
    user_id: int,
    user_type: str,
    redis_client: Redis,
    db_session = None
) -> bool:
    """
    Initialize or refresh the cache for a specific user.
    Returns True if successful, False otherwise.
    """
    try:
        # If no session provided, create one
        close_session = False
        if db_session is None:
            db_session = AsyncSessionLocal()
            close_session = True

        try:
            # 1. Get user data from DB and cache it
            if user_type.lower() == 'demo':
                db_user = await crud_user.get_demo_user_by_id(db_session, user_id, user_type=user_type)
            else:
                db_user = await crud_user.get_user_by_id(db_session, user_id, user_type=user_type)
            
            if not db_user:
                logger.warning(f"User {user_id} (type: {user_type}) not found in database. Cannot initialize cache.")
                return False
            
            # Cache user data
            user_data_to_cache = {
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
            await set_user_data_cache(redis_client, user_id, user_data_to_cache)
            
            # 2. Get open positions from DB
            order_model = crud_order.get_order_model(user_type)
            open_positions = await crud_order.get_all_open_orders_by_user_id(db_session, user_id, order_model)
            
            # Convert to dictionary format for portfolio calculation
            open_positions_dicts = [
                {attr: str(getattr(pos, attr)) if isinstance(getattr(pos, attr), Decimal) else getattr(pos, attr)
                 for attr in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 
                              'order_price', 'margin', 'contract_value', 'stop_loss', 
                              'take_profit', 'commission']}
                for pos in open_positions
            ]
            
            # 3. Get group symbol settings
            group_name = db_user.group_name
            
            # IMPORTANT: Update group symbol settings in cache first
            # Import the function here to avoid circular imports
            from app.api.v1.endpoints.market_data_ws import update_group_symbol_settings
            await update_group_symbol_settings(group_name, db_session, redis_client)
            
            # Now get the settings from cache
            group_symbol_settings = await get_group_symbol_settings_cache(redis_client, group_name, "ALL")
            
            # 4. Get market prices
            adjusted_market_prices = {}
            if group_symbol_settings:
                for symbol in group_symbol_settings.keys():
                    prices = await get_last_known_price(redis_client, symbol)
                    if prices:
                        adjusted_market_prices[symbol] = {
                            'buy': Decimal(str(prices.get('o', 0))),  # 'o' is bid in Firebase
                            'sell': Decimal(str(prices.get('b', 0)))  # 'b' is ask in Firebase
                        }
            
            # 5. Calculate portfolio
            portfolio = await calculate_user_portfolio(
                user_data_to_cache, 
                open_positions_dicts, 
                adjusted_market_prices, 
                group_symbol_settings or {}, 
                redis_client
            )
            
            # 6. Cache portfolio
            await set_user_portfolio_cache(redis_client, user_id, portfolio)
            
            logger.info(f"Successfully initialized cache for user {user_id} (type: {user_type})")
            return True
            
        finally:
            if close_session:
                await db_session.close()
                
    except Exception as e:
        logger.error(f"Error initializing cache for user {user_id}: {e}", exc_info=True)
        return False

async def notify_account_change(redis_client: Redis, user_id: int, user_type: str):
    """
    Notify the background worker that a user's account structure has changed.
    This should be called after order placement, closure, or other account changes.
    """
    # Add user to active users set
    active_users.add(user_id)
    user_types[user_id] = user_type
    
    # Publish event to trigger immediate update
    await publish_account_structure_changed_event(redis_client, user_id)
    
    logger.debug(f"Notified background worker about account change for user {user_id}")

async def check_margin_levels(
    user_id: int,
    user_type: str,
    portfolio: Dict[str, Any],
    redis_client: Redis,
    db_session
) -> None:
    """
    Check margin levels and perform autocut if necessary.
    """
    try:
        # Get margin level from portfolio
        margin_level_str = portfolio.get("margin_level", "0")
        try:
            margin_level = Decimal(margin_level_str)
        except:
            margin_level = Decimal("0")
        
        # Get user data for autocut settings
        user_data = await get_user_data_cache(redis_client, user_id)
        if not user_data:
            logger.warning(f"No user data found for margin level check - user_id={user_id}")
            return
            
        group_name = user_data.get("group_name")
        if not group_name:
            logger.warning(f"No group name found for user {user_id}")
            return
            
        # In a real implementation, you'd get these from group settings
        # For now, using hardcoded values
        margin_call_level = Decimal("100")  # 100%
        stop_out_level = Decimal("50")      # 50%
        
        if margin_level <= stop_out_level:
            logger.warning(f"STOP OUT! User {user_id} margin level {margin_level}% below stop out level {stop_out_level}%")
            # Implement autocut logic here
            # This would close positions starting from the most unprofitable
            positions = portfolio.get("positions", [])
            if positions:
                # Sort positions by profit_loss (ascending)
                positions_with_pnl = []
                for pos in positions:
                    try:
                        pnl = Decimal(pos.get("profit_loss", "0"))
                        positions_with_pnl.append((pos, pnl))
                    except:
                        positions_with_pnl.append((pos, Decimal("0")))
                
                positions_with_pnl.sort(key=lambda x: x[1])
                
                # Get the most unprofitable position
                worst_position = positions_with_pnl[0][0]
                order_id = worst_position.get("order_id")
                
                if order_id:
                    logger.warning(f"Auto-closing position {order_id} for user {user_id} due to stop out")
                    # In a real implementation, you'd call the close_order endpoint here
                    # For now, just log it
                    logger.info(f"Would auto-close position {order_id} for user {user_id}")
                    
        elif margin_level <= margin_call_level:
            logger.warning(f"MARGIN CALL! User {user_id} margin level {margin_level}% below margin call level {margin_call_level}%")
            # In a real implementation, you might send a notification to the user
            
    except Exception as e:
        logger.error(f"Error checking margin levels for user {user_id}: {e}", exc_info=True)

async def portfolio_worker_loop(redis_client: Redis):
    """
    Main worker loop that continuously updates user portfolios.
    """
    logger.info("Portfolio background worker started")
    
    while not stop_event.is_set():
        try:
            # Process all active users
            users_to_process = list(active_users)
            
            if users_to_process:
                logger.debug(f"Processing {len(users_to_process)} active users")
                
                async with AsyncSessionLocal() as db_session:
                    for user_id in users_to_process:
                        user_type = user_types.get(user_id, "live")  # Default to live if not specified
                        
                        # Get cached user data
                        user_data = await get_user_data_cache(redis_client, user_id)
                        if not user_data:
                            logger.debug(f"No cached data for user {user_id}, initializing...")
                            success = await initialize_user_cache(user_id, user_type, redis_client, db_session)
                            if not success:
                                logger.warning(f"Failed to initialize cache for user {user_id}, skipping")
                                continue
                            user_data = await get_user_data_cache(redis_client, user_id)
                            if not user_data:
                                continue
                        
                        # Get group symbol settings
                        group_name = user_data.get("group_name")
                        if not group_name:
                            logger.warning(f"No group name for user {user_id}, skipping")
                            continue
                            
                        group_symbol_settings = await get_group_symbol_settings_cache(redis_client, group_name, "ALL")
                        
                        # Get open positions from cache
                        portfolio_cache = await get_user_portfolio_cache(redis_client, user_id)
                        if not portfolio_cache:
                            logger.debug(f"No portfolio cache for user {user_id}, initializing...")
                            success = await initialize_user_cache(user_id, user_type, redis_client, db_session)
                            if not success:
                                continue
                            portfolio_cache = await get_user_portfolio_cache(redis_client, user_id)
                            if not portfolio_cache:
                                continue
                                
                        open_positions = portfolio_cache.get("positions", [])
                        
                        # Get market prices
                        adjusted_market_prices = {}
                        if group_symbol_settings:
                            for symbol in group_symbol_settings.keys():
                                prices = await get_last_known_price(redis_client, symbol)
                                if prices:
                                    adjusted_market_prices[symbol] = {
                                        'buy': Decimal(str(prices.get('o', 0))),  # 'o' is bid in Firebase
                                        'sell': Decimal(str(prices.get('b', 0)))  # 'b' is ask in Firebase
                                    }
                        
                        # Calculate updated portfolio
                        portfolio = await calculate_user_portfolio(
                            user_data, 
                            open_positions, 
                            adjusted_market_prices, 
                            group_symbol_settings or {}, 
                            redis_client
                        )
                        
                        # Check margin levels and perform autocut if necessary
                        await check_margin_levels(user_id, user_type, portfolio, redis_client, db_session)
                        
                        # Update portfolio cache
                        await set_user_portfolio_cache(redis_client, user_id, portfolio)
                        
                        # Notify WebSocket clients
                        await publish_account_structure_changed_event(redis_client, user_id)
            
            # Sleep for a short interval
            await asyncio.sleep(5)  # Update every 5 seconds
            
        except asyncio.CancelledError:
            logger.info("Portfolio worker task cancelled")
            break
        except Exception as e:
            logger.error(f"Error in portfolio worker loop: {e}", exc_info=True)
            await asyncio.sleep(5)  # Sleep before retrying
    
    logger.info("Portfolio background worker stopped")

async def start_portfolio_worker(redis_client: Redis):
    """
    Start the portfolio background worker.
    """
    global worker_task
    if worker_task is None or worker_task.done():
        stop_event.clear()
        worker_task = asyncio.create_task(portfolio_worker_loop(redis_client))
        logger.info("Portfolio background worker started")
    else:
        logger.warning("Portfolio background worker already running")

async def stop_portfolio_worker():
    """
    Stop the portfolio background worker.
    """
    global worker_task
    if worker_task and not worker_task.done():
        stop_event.set()
        try:
            await asyncio.wait_for(worker_task, timeout=10.0)
        except asyncio.TimeoutError:
            logger.warning("Portfolio worker did not stop gracefully, cancelling")
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass
        logger.info("Portfolio background worker stopped")
    else:
        logger.warning("Portfolio background worker not running")

# Function to call when an order is placed or closed
async def notify_account_change(redis_client: Redis, user_id: int, user_type: str = 'live'):
    """
    Notify the portfolio background worker that an account structure has changed.
    Call this function after placing or closing an order.
    """
    try:
        message = json.dumps({
            'user_id': user_id,
            'user_type': user_type,
            'timestamp': asyncio.get_event_loop().time()
        })
        await redis_client.publish(REDIS_ACCOUNT_CHANGE_CHANNEL, message)
        logger.debug(f"Published account change notification for user {user_id}")
    except Exception as e:
        logger.error(f"Error publishing account change notification: {e}", exc_info=True)

# Example autocut implementation (placeholder)
async def trigger_autocut(user_id: int, user_type: str, db: AsyncSession, redis_client: Redis):
    """
    Trigger autocut for a user when margin level falls below threshold.
    This is a placeholder - implement your actual autocut logic here.
    """
    logger.warning(f"AUTOCUT TRIGGERED for user {user_id} ({user_type})")
    # Implement your autocut logic here
    # For example, close all open positions for the user
    # This would involve calling your existing close_order function for each position
