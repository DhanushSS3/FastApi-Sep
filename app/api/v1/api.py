# app/api/v1/api.py

from fastapi import APIRouter

# Import individual routers from endpoints
from app.api.v1.endpoints import users, groups, orders, market_data_ws, admin
from app.api.v1.endpoints import money_requests
from app.api.v1.endpoints import wallets

# Create the main API router for version 1
api_router = APIRouter()

# Include all routers
api_router.include_router(users.router, tags=["users"])
api_router.include_router(groups.router, tags=["groups"])
api_router.include_router(orders.router, tags=["orders"])
api_router.include_router(market_data_ws.router)
api_router.include_router(money_requests.router, tags=["Money Requests"])
api_router.include_router(wallets.router, tags=["Money Requests"])
api_router.include_router(admin.router)  # Include admin router