from typing import Optional, Any
from pydantic import BaseModel, Field, model_validator
from decimal import Decimal
from pydantic import validator

# --- Place Order ---
class OrderPlacementRequest(BaseModel):
    symbol: str # Corresponds to order_company_name from your description
    order_type: str # E.g., "MARKET", "LIMIT", "STOP", "BUY", "SELL", "BUY_LIMIT", "SELL_LIMIT"
    order_quantity: Decimal = Field(..., gt=0)
    order_price: Decimal # For LIMIT/STOP. For MARKET, can be current market price or 0 if server fetches.
    user_type: str # "live" or "demo" as passed by frontend
    status: Optional[str] = Field(None, description="Order status string (0-30 chars)")

    stop_loss: Optional[Decimal] = None
    take_profit: Optional[Decimal] = None
    user_id: Optional[int] = None # For service accounts placing orders for other users.

    class Config:
        json_encoders = {
            Decimal: lambda v: str(v),
        }


# --- Internal Model for Order Creation ---
class OrderCreateInternal(BaseModel):
    order_id: str
    order_status: str
    order_user_id: int
    order_company_name: str
    order_type: str
    order_price: Decimal
    order_quantity: Decimal
    contract_value: Optional[Decimal]
    margin: Optional[Decimal]
    status: Optional[str] = Field(None, description="Order status string (0-30 chars)")

    # Optional financials
    stop_loss: Optional[Decimal] = None
    take_profit: Optional[Decimal] = None
    close_id: Optional[str] = None # Added for tracking closed orders


# --- Order Response Schema ---
class OrderResponse(BaseModel):
    order_id: str
    order_user_id: int  # Use this field for user id
    order_company_name: str  # Use this for symbol/company
    order_type: str
    order_quantity: Decimal
    order_price: Decimal
    status: str
    stop_loss: Optional[Decimal] = None
    take_profit: Optional[Decimal] = None
    order_status: str
    contract_value: Optional[Decimal] = None  # Allow None for pending orders
    margin: Optional[Decimal] = None  # Allow None for pending orders
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    # open_time removed; use created_at
    net_profit: Optional[Decimal] = None
    close_price: Optional[Decimal] = None
    commission: Optional[Decimal] = None
    swap: Optional[Decimal] = None
    cancel_message: Optional[str] = None
    close_message: Optional[str] = None
    created_at: Optional[Any] = None # datetime will be serialized by FastAPI/Pydantic
    updated_at: Optional[Any] = None # datetime will be serialized by FastAPI/Pydantic
    stoploss_id: Optional[str] = None
    takeprofit_id: Optional[str] = None
    close_order_id: Optional[str] = None # Added for tracking closed orders


# --- Close Order Request Schema ---
class CloseOrderRequest(BaseModel):
    order_id: str
    close_price: Decimal
    user_id: Optional[int] = None # For service accounts closing orders for other users.
    # Frontend might pass these for context, but backend should verify/use its own
    order_type: Optional[str] = None
    order_company_name: Optional[str] = None
    order_status: Optional[str] = None
    status: Optional[str] = Field(None, description="Order status string (0-30 chars)")

    class Config:
        json_encoders = {
            Decimal: lambda v: str(v),
        }


# --- Update Stop Loss / Take Profit Request Schema ---
class UpdateStopLossTakeProfitRequest(BaseModel):
    order_id: str
    stop_loss: Optional[Decimal] = None
    take_profit: Optional[Decimal] = None
    user_id: Optional[int] = None # For service accounts updating orders for other users.
    modify_id: Optional[str] = None # Unique ID for this modification
    stoploss_id: Optional[str] = None
    takeprofit_id: Optional[str] = None
    status: Optional[str] = Field(None, description="Order status string (0-30 chars)")

    @model_validator(mode="after")
    def validate_tp_sl(self) -> 'UpdateStopLossTakeProfitRequest':
        if not self.stop_loss and not self.take_profit:
            raise ValueError("Either stop_loss or take_profit must be provided.")
        if self.stop_loss is not None and not self.stoploss_id:
            raise ValueError("stoploss_id is required when stop_loss is provided.")
        if self.take_profit is not None and not self.takeprofit_id:
            raise ValueError("takeprofit_id is required when take_profit is provided.")
        return self

    class Config:
        from_attributes = True


class PendingOrderPlacementRequest(OrderPlacementRequest):
    """
    Schema for placing pending orders (BUY_LIMIT, SELL_LIMIT, BUY_STOP, SELL_STOP).
    """
    order_status: str = "PENDING" # Default status for pending orders

    @validator('order_type')
    def validate_pending_order_type(cls, v):
        valid_pending_types = {"BUY_LIMIT", "SELL_LIMIT", "BUY_STOP", "SELL_STOP"}
        if v.upper() not in valid_pending_types:
            raise ValueError(f"Invalid order type for pending order. Must be one of: {', '.join(valid_pending_types)}")
        return v.upper()

    class Config:
        # Inherits json_encoders from OrderPlacementRequest, but can be overridden if needed
        pass



# --- Order PATCH Update Schema ---
class OrderUpdateRequest(BaseModel):
    order_status: Optional[str] = None
    status: Optional[str] = Field(None, description="Order status string (0-30 chars)")
    order_price: Optional[Decimal] = None
    order_quantity: Optional[Decimal] = None
    margin: Optional[Decimal] = None
    close_price: Optional[Decimal] = None
    net_profit: Optional[Decimal] = None
    stop_loss: Optional[Decimal] = None
    take_profit: Optional[Decimal] = None
    contract_value: Optional[Decimal] = None
    commission: Optional[Decimal] = None
    swap: Optional[Decimal] = None
    cancel_message: Optional[str] = None
    close_message: Optional[str] = None

    # Tracking Fields
    stoploss_id: Optional[str] = None
    takeprofit_id: Optional[str] = None
    close_id: Optional[str] = None # Added for OrderActionHistory tracking