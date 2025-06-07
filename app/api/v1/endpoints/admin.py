from fastapi import APIRouter, Depends, HTTPException, status, Body
from sqlalchemy.ext.asyncio import AsyncSession
from app.database.session import get_db
from app.dependencies.redis_client import get_redis_client
from app.api.v1.endpoints.market_data_ws import update_group_symbol_settings, publish_test_market_data
from app.core.security import get_current_active_superuser, get_password_hash, verify_password, create_access_token
from typing import Dict, Any, List
from pydantic import BaseModel, EmailStr
from app.database.models import User
from sqlalchemy.future import select
from decimal import Decimal
import uuid
import os
from datetime import timedelta

router = APIRouter(
    prefix="/admin",
    tags=["admin"],
    responses={404: {"description": "Not found"}},
)

# Model for creating admin user
class AdminCreate(BaseModel):
    email: EmailStr
    password: str
    name: str = "Admin User"
    setup_key: str  # Secret key to authorize admin creation

# Model for admin login
class AdminLogin(BaseModel):
    email: EmailStr
    password: str

@router.post("/rebuild-group-cache/{group_name}", status_code=status.HTTP_200_OK, dependencies=[Depends(get_current_active_superuser)])
async def rebuild_group_cache(
    group_name: str,
    db: AsyncSession = Depends(get_db),
    redis_client = Depends(get_redis_client)
) -> Dict[str, Any]:
    """
    Rebuild the cache for a specific group.
    This is useful when group settings have been updated in the database.
    """
    try:
        await update_group_symbol_settings(group_name, db, redis_client)
        return {"status": "success", "message": f"Cache rebuilt for group {group_name}"}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to rebuild cache: {str(e)}"
        )

@router.post("/rebuild-all-caches", status_code=status.HTTP_200_OK, dependencies=[Depends(get_current_active_superuser)])
async def rebuild_all_caches(
    db: AsyncSession = Depends(get_db),
    redis_client = Depends(get_redis_client)
) -> Dict[str, Any]:
    """
    Rebuild all caches including group settings and user portfolios.
    This is useful after a Redis flush or server restart.
    """
    try:
        # Get all groups
        from app.crud import group as crud_group
        all_groups = await crud_group.get_unique_groups(db)
        
        # Rebuild cache for each group
        for group in all_groups:
            await update_group_symbol_settings(group, db, redis_client)
            
        # Rebuild user portfolios
        from app.services.portfolio_background_worker import initialize_user_cache
        from app.crud import user as crud_user
        
        # Get all users
        all_users = await crud_user.get_all_users(db)
        all_demo_users = await crud_user.get_all_demo_users(db)
        
        # Initialize cache for each user
        for user in all_users:
            await initialize_user_cache(user.id, "live", redis_client, db)
            
        for demo_user in all_demo_users:
            await initialize_user_cache(demo_user.id, "demo", redis_client, db)
            
        return {
            "status": "success", 
            "message": f"All caches rebuilt: {len(all_groups)} groups, {len(all_users)} live users, {len(all_demo_users)} demo users"
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to rebuild caches: {str(e)}"
        )

@router.post("/setup-first-admin", status_code=status.HTTP_201_CREATED)
async def setup_first_admin(
    admin_data: AdminCreate,
    db: AsyncSession = Depends(get_db)
) -> Dict[str, Any]:
    """
    Create the first admin user in the system.
    This endpoint can only be used if no admin users exist yet.
    Requires a setup key that matches the ADMIN_SETUP_KEY environment variable.
    """
    # Check setup key
    setup_key = os.environ.get("ADMIN_SETUP_KEY", "default_setup_key_change_me")
    if admin_data.setup_key != setup_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid setup key"
        )
    
    # Check if any admin users already exist
    result = await db.execute(select(User).filter(User.user_type == "admin"))
    existing_admin = result.scalars().first()
    
    if existing_admin:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Admin user already exists. This endpoint can only be used for initial setup."
        )
    
    # Generate a unique account number
    account_number = str(uuid.uuid4())[:5].upper()
    
    # Create new admin user
    admin_user = User(
        name=admin_data.name,
        email=admin_data.email,
        phone_number="admin",  # Placeholder
        hashed_password=get_password_hash(admin_data.password),
        user_type="admin",
        wallet_balance=Decimal("0.0"),
        leverage=Decimal("1.0"),
        margin=Decimal("0.0"),
        account_number=account_number,
        group_name="Admin",
        status=1,  # Active status
        isActive=1,  # Active
    )
    
    db.add(admin_user)
    await db.commit()
    await db.refresh(admin_user)
    
    return {
        "status": "success",
        "message": "Admin user created successfully",
        "user_id": admin_user.id,
        "email": admin_user.email,
        "account_number": admin_user.account_number
    }

@router.post("/login", status_code=status.HTTP_200_OK)
async def admin_login(
    login_data: AdminLogin,
    db: AsyncSession = Depends(get_db)
) -> Dict[str, Any]:
    """
    Login endpoint for admin users.
    Returns an access token if credentials are valid.
    """
    # Find admin user by email
    result = await db.execute(select(User).filter(User.email == login_data.email, User.user_type == "admin"))
    admin_user = result.scalars().first()
    
    if not admin_user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials"
        )
    
    # Verify password
    if not verify_password(login_data.password, admin_user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials"
        )
    
    # Create access token
    access_token_expires = timedelta(minutes=60*24)  # 24 hours
    access_token = create_access_token(
        data={
            "sub": str(admin_user.id),
            "user_type": "admin",
            "account_number": admin_user.account_number
        },
        expires_delta=access_token_expires
    )
    
    return {
        "status": "success",
        "access_token": access_token,
        "token_type": "bearer",
        "user_id": admin_user.id,
        "email": admin_user.email,
        "name": admin_user.name
    }

@router.post("/publish-test-market-data", status_code=status.HTTP_200_OK)
async def publish_test_market_data_endpoint(
    db: AsyncSession = Depends(get_db),
    redis_client = Depends(get_redis_client),
    current_user: User = Depends(get_current_active_superuser)
) -> Dict[str, Any]:
    """
    Manually publish test market data to the market data channel.
    This is useful for testing when there's no real market data coming in.
    """
    try:
        success = await publish_test_market_data(redis_client)
        if success:
            return {"status": "success", "message": "Test market data published successfully"}
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to publish test market data"
            )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to publish test market data: {str(e)}"
        ) 