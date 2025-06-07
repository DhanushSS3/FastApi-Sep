# app/services/margin_calculator.py

from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
import logging
from typing import Optional, Tuple, Dict, Any, List
import json

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from redis.asyncio import Redis

from app.database.models import User, Group, ExternalSymbolInfo
from app.core.cache import (
    get_user_data_cache,
    set_user_data_cache,
    get_group_symbol_settings_cache,
    set_group_symbol_settings_cache,
    get_group_settings_cache,
    DecimalEncoder,
    get_live_adjusted_buy_price_for_pair,
    get_live_adjusted_sell_price_for_pair,
    get_adjusted_market_price_cache,
    get_last_known_price
)
from app.firebase_stream import get_latest_market_data
from app.crud.crud_symbol import get_symbol_type
from app.services.portfolio_calculator import _convert_to_usd, _calculate_adjusted_prices_from_raw
from app.core.logging_config import orders_logger

logger = logging.getLogger(__name__)

# --- HELPER FUNCTION: Calculate Base Margin Per Lot (Used in Hedging) ---
# This helper calculates a per-lot value based on Group margin setting, price, and leverage.
# This is used in the hedging logic in order_processing.py for comparison.
async def calculate_base_margin_per_lot(
    redis_client: Redis,
    user_id: int,
    symbol: str,
    price: Decimal
) -> Optional[Decimal]:
    """
    Calculates a base margin value per standard lot for a given symbol and price,
    considering user's group settings and leverage.
    This value is used for comparison in hedging calculations.
    Returns the base margin value per lot or None if calculation fails.
    """
    # Retrieve user data from cache to get group_name and leverage
    user_data = await get_user_data_cache(redis_client, user_id)
    if not user_data or 'group_name' not in user_data or 'leverage' not in user_data:
        orders_logger.error(f"User data or group_name/leverage not found in cache for user {user_id}.")
        return None

    group_name = user_data['group_name']
    # Ensure user_leverage is Decimal
    user_leverage_raw = user_data.get('leverage', 1)
    user_leverage = Decimal(str(user_leverage_raw)) if user_leverage_raw is not None else Decimal(1)


    # Retrieve group-symbol settings from cache
    # Need settings for the specific symbol
    group_symbol_settings = await get_group_symbol_settings_cache(redis_client, group_name, symbol)
    # We need the 'margin' setting from the group for this calculation
    if not group_symbol_settings or 'margin' not in group_symbol_settings:
        orders_logger.error(f"Group symbol settings or margin setting not found in cache for group '{group_name}', symbol '{symbol}'.")
        return None

    # Ensure margin_setting is Decimal
    margin_setting_raw = group_symbol_settings.get('margin', 0)
    margin_setting = Decimal(str(margin_setting_raw)) if margin_setting_raw is not None else Decimal(0)


    if user_leverage <= 0:
         orders_logger.error(f"User leverage is zero or negative for user {user_id}.")
         return None

    # Calculation based on Group Base Margin Setting, Price, and Leverage
    # This formula seems to be the one needed for the per-lot comparison in hedging.
    try:
        # Ensure price is Decimal
        price_decimal = Decimal(str(price))
        base_margin_per_lot = (margin_setting * price_decimal) / user_leverage
        orders_logger.debug(f"Calculated base margin per lot (for hedging) for user {user_id}, symbol {symbol}, price {price}: {base_margin_per_lot}")
        return base_margin_per_lot
    except Exception as e:
        orders_logger.error(f"Error calculating base margin per lot (for hedging) for user {user_id}, symbol {symbol}: {e}", exc_info=True)
        return None


from app.core.cache import get_last_known_price

async def calculate_single_order_margin(
    redis_client: Redis,
    symbol: str,
    order_type: str,
    quantity: Decimal,
    user_leverage: Decimal,
    group_settings: Dict[str, Any],
    external_symbol_info: Dict[str, Any],
    raw_market_data: Dict[str, Any]
) -> Tuple[Optional[Decimal], Optional[Decimal], Optional[Decimal]]:
    """
    Calculate margin for a single order using raw prices.
    """
    try:
        # Get raw prices
        # Try to get prices from live data, else fallback to last known price in Redis
        symbol_data = raw_market_data.get(symbol, {})
        if not symbol_data:
            symbol_data = await get_last_known_price(redis_client, symbol)
            if not symbol_data:
                logger.error(f"No live or cached price found for {symbol}")
                return None, None, None
        raw_ask_price = Decimal(str(symbol_data.get('b', 0)))  # Ask price
        raw_bid_price = Decimal(str(symbol_data.get('o', 0)))  # Bid price

        if not raw_ask_price or not raw_bid_price:
            logger.error(f"Invalid raw prices for {symbol}: ask={raw_ask_price}, bid={raw_bid_price}")
            return None, None, None

        # Calculate contract value and margin using raw prices
        contract_size = Decimal(str(external_symbol_info.get('contract_size', 100000)))
        order_type_upper = order_type.upper()
        
        contract_value = quantity * contract_size
        adjusted_price = raw_ask_price
        adjusted_price = raw_bid_price

        # Calculate contract value based on order type
        if order_type_upper in ['BUY', 'BUY_LIMIT', 'BUY_STOP']:
            margin = ((contract_value * raw_ask_price) / user_leverage).quantize(Decimal('0.00000001'), rounding=ROUND_HALF_UP)
        else:  # SELL orders
            margin = ((contract_value * raw_ask_price) / user_leverage).quantize(Decimal('0.00000001'), rounding=ROUND_HALF_UP)
            

        # Calculate commission
        commission = Decimal('0.0')
        commission_type = int(group_settings.get('commision_type', 0))
        commission_value_type = int(group_settings.get('commision_value_type', 0))
        commission_rate = Decimal(str(group_settings.get('commision', 0)))

        if commission_type in [0, 1]:  # "Every Trade" or "In"
            if commission_value_type == 0:  # Per lot
                commission = quantity * commission_rate
            elif commission_value_type == 1:  # Percent of price
                commission = ((commission_rate * adjusted_price) / Decimal("100")) * quantity

        commission = commission.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        # Add commission to margin
        margin = margin + commission

        # Convert margin to USD if needed
        profit_currency = external_symbol_info.get('profit_currency', 'USD')
        if profit_currency != 'USD':
            try:
                # Try direct conversion first (e.g., EURUSD)
                direct_pair = f"{profit_currency}USD"
                direct_data = raw_market_data.get(direct_pair, {})
                if not direct_data:
                    direct_data = await get_last_known_price(redis_client, direct_pair)
                direct_rate = Decimal(str(direct_data.get('b', 0))) if direct_data else Decimal(0)
                if direct_rate > 0:
                    margin_usd = margin * direct_rate
                else:
                    # Try indirect conversion (e.g., USDEUR)
                    indirect_pair = f"USD{profit_currency}"
                    indirect_data = raw_market_data.get(indirect_pair, {})
                    if not indirect_data:
                        indirect_data = await get_last_known_price(redis_client, indirect_pair)
                    indirect_rate = Decimal(str(indirect_data.get('b', 0))) if indirect_data else Decimal(0)
                    if indirect_rate > 0:
                        margin_usd = margin / indirect_rate
                    else:
                        logger.error(f"Could not convert margin from {profit_currency} to USD")
                        return None, None, None
            except Exception as e:
                logger.error(f"Error converting margin to USD: {e}")
                return None, None, None
        else:
            margin_usd = margin

        return margin_usd, adjusted_price, contract_value

    except Exception as e:
        logger.error(f"Error calculating margin for {symbol}: {e}", exc_info=True)
        return None, None, None

async def calculate_total_symbol_margin_contribution(
    db: AsyncSession,
    redis_client: Redis,
    user_id: int,
    symbol: str,
    open_positions_for_symbol: list,
    order_model=None
) -> Decimal:
    """
    Calculate total margin contribution for a symbol considering hedged positions.
    """
    try:
        total_buy_quantity = Decimal('0.0')
        total_sell_quantity = Decimal('0.0')
        all_margins_per_lot: List[Decimal] = []

        # Get user data for leverage
        user_data = await get_user_data_cache(redis_client, user_id)
        if not user_data:
            logger.error(f"User data not found for user {user_id}")
            return Decimal('0.0')

        user_leverage = Decimal(str(user_data.get('leverage', '1.0')))
        if user_leverage <= 0:
            logger.error(f"Invalid leverage for user {user_id}: {user_leverage}")
            return Decimal('0.0')

        # Get group settings for margin calculation
        group_name = user_data.get('group_name')
        group_settings = await get_group_symbol_settings_cache(redis_client, group_name, symbol)
        if not group_settings:
            logger.error(f"Group settings not found for symbol {symbol}")
            return Decimal('0.0')

        # Get external symbol info
        external_symbol_info = await get_external_symbol_info(db, symbol)
        if not external_symbol_info:
            logger.error(f"External symbol info not found for {symbol}")
            return Decimal('0.0')

        # Get raw market data for price calculations
        raw_market_data = await get_latest_market_data()
        if not raw_market_data:
            logger.error("Failed to get market data")
            return Decimal('0.0')

        # Process each position
        for position in open_positions_for_symbol:
            position_quantity = Decimal(str(position.order_quantity))
            position_type = position.order_type.upper()
            position_margin = Decimal(str(position.margin))

            if position_quantity > 0:
                # Calculate margin per lot for this position
                margin_per_lot = position_margin / position_quantity
                all_margins_per_lot.append(margin_per_lot)

                # Add to total quantities
                if position_type in ['BUY', 'BUY_LIMIT', 'BUY_STOP']:
                    total_buy_quantity += position_quantity
                elif position_type in ['SELL', 'SELL_LIMIT', 'SELL_STOP']:
                    total_sell_quantity += position_quantity

        # Calculate net quantity (for hedged positions)
        net_quantity = max(total_buy_quantity, total_sell_quantity)
        
        # Get the highest margin per lot (for hedged positions)
        highest_margin_per_lot = max(all_margins_per_lot) if all_margins_per_lot else Decimal('0.0')

        # Calculate total margin contribution
        total_margin = (highest_margin_per_lot * net_quantity).quantize(Decimal('0.00000001'), rounding=ROUND_HALF_UP)

        # Add commission if applicable
        commission_type = int(group_settings.get('commision_type', 0))
        commission_value_type = int(group_settings.get('commision_value_type', 0))
        commission_rate = Decimal(str(group_settings.get('commision', '0.0')))

        if commission_type in [0, 1]:  # If commission is enabled
            if commission_value_type == 0:  # Fixed commission per lot
                commission = net_quantity * commission_rate
            elif commission_value_type == 1:  # Percentage of price
                # Get current price for commission calculation
                symbol_data = raw_market_data.get(symbol, {})
                if symbol_data:
                    current_price = Decimal(str(symbol_data.get('b', 0)))  # Use ask price
                    contract_size = Decimal(str(external_symbol_info.get('contract_size', 100000)))
                    commission = ((commission_rate * current_price * net_quantity * contract_size) / Decimal('100'))
                else:
                    commission = Decimal('0.0')
            else:
                commission = Decimal('0.0')
            
            commission = commission.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            total_margin += commission

        return total_margin

    except Exception as e:
        logger.error(f"Error calculating total symbol margin contribution: {e}", exc_info=True)
        return Decimal('0.0')