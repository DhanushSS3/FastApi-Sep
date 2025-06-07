from fastapi import APIRouter, Depends, HTTPException, status, File, UploadFile, Form, Query
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError
import datetime
import shutil
import os
import uuid
from typing import Optional, List, Dict, Any
import logging
from decimal import Decimal

from sqlalchemy.future import select
from jose import JWTError

from app.database.session import get_db
from app.database.models import User, UserOrder, DemoUser # Import DemoUser
from app.schemas.user import (
    UserCreate,
    UserResponse,
    UserUpdate,
    SendOTPRequest,
    VerifyOTPRequest,
    RequestPasswordReset,
    ResetPasswordConfirm,
    StatusResponse,
    UserLogin,
    Token,
    TokenRefresh
)
from app.schemas.demo_user import ( # Import DemoUser schemas
    DemoUserCreate,
    DemoUserResponse,
    DemoUserUpdate,
    DemoUserLogin,
    DemoSendOTPRequest,
    DemoVerifyOTPRequest,
    DemoRequestPasswordReset,
    DemoResetPasswordConfirm
)
from app.crud import user as crud_user
from app.crud import otp as crud_otp
from app.crud.user import generate_unique_account_number, generate_unique_demo_account_number # Import demo account number generator
from app.services import email as email_service
from app.core.security import (
    get_password_hash,
    verify_password,
    create_access_token,
    create_refresh_token,
    decode_token,
    get_current_user,
    get_current_admin_user,
    store_refresh_token,
    get_refresh_token_data,
    delete_refresh_token
)
from app.core.config import get_settings

from redis.asyncio import Redis
from app.dependencies.redis_client import get_redis_client

from app.schemas.user import SignupVerifyOTPRequest, SignupSendOTPRequest
from app.schemas.money_request import MoneyRequestResponse, MoneyRequestCreate
from app.crud import money_request as crud_money_request
from app.schemas.wallet import WalletTransactionRequest, WalletBalanceResponse # Import WalletBalanceResponse

logger = logging.getLogger(__name__)

router = APIRouter(
    tags=["users"]
)

settings = get_settings()

UPLOAD_DIRECTORY = "./uploads/proofs"
os.makedirs(UPLOAD_DIRECTORY, exist_ok=True)

async def save_upload_file(upload_file: Optional[UploadFile]) -> Optional[dict]:
    """
    Saves an uploaded file to the UPLOAD_DIRECTORY with a unique filename.
    Returns a dict with both the static path and the real file path.
    """
    if not upload_file:
        return None

    file_extension = os.path.splitext(upload_file.filename)[1]
    unique_filename = f"{uuid.uuid4()}{file_extension}"
    full_file_path = os.path.join(UPLOAD_DIRECTORY, unique_filename)

    os.makedirs(os.path.dirname(full_file_path), exist_ok=True)

    try:
        contents = await upload_file.read()
        with open(full_file_path, "wb") as f:
            f.write(contents)
    except Exception as e:
        logger.error(f"Error saving uploaded file {upload_file.filename}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to save uploaded file.")
    finally:
        await upload_file.close()

    static_path = os.path.join("/static/proofs", unique_filename)
    logger.info(f"Saved uploaded file {upload_file.filename} to {full_file_path}, storing path {static_path}")
    return {"static_path": static_path, "real_path": full_file_path}


# --- User Endpoints ---

@router.post(
    "/register",
    response_model=UserResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Register a new user with proofs",
    description="Creates a new user account with the provided details and uploads identity/address proofs."
)
async def register_user_with_proofs(
    name: str = Form(...),
    phone_number: str = Form(..., max_length=20),
    email: str = Form(...),
    password: str = Form(..., min_length=8),
    country: str = Form(...),
    city: str = Form(...),
    state: str = Form(...),
    pincode: int = Form(...),
    group: str = Form(...),
    bank_account_number: str = Form(...),
    bank_ifsc_code: str = Form(...),
    bank_holder_name: str = Form(...),
    bank_branch_name: str = Form(...),
    security_question: str = Form(...),
    security_answer: str = Form(...),
    address_proof: str = Form(...),
    address_proof_image: UploadFile = File(...),
    id_proof: str = Form(...),
    id_proof_image: UploadFile = File(...),
    is_self_trading: int = Form(...),
    # Optional fields
    fund_manager: str = Form(None),
    user_type: str = Form(None),  # Keep accepting user_type but we'll override it
    isActive: int = Form(None),
    db: AsyncSession = Depends(get_db)
):
    # Always set user_type to "live" regardless of what's provided from frontend
    fixed_user_type = "live"
    
    existing_user = await crud_user.get_user_by_email_phone_type(db, email=email, phone_number=phone_number, user_type=fixed_user_type)
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="User with this email and phone number already exists for this user type."
        )

    id_proof_image_result = await save_upload_file(id_proof_image)
    address_proof_image_result = await save_upload_file(address_proof_image)

    id_proof_image_path = id_proof_image_result["static_path"] if id_proof_image_result else None
    id_proof_image_real_path = id_proof_image_result["real_path"] if id_proof_image_result else None
    address_proof_image_path = address_proof_image_result["static_path"] if address_proof_image_result else None
    address_proof_image_real_path = address_proof_image_result["real_path"] if address_proof_image_result else None

    user_data = {
        "name": name,
        "email": email,
        "phone_number": phone_number,
        "country": country,
        "city": city,
        "state": state,
        "pincode": pincode,
        "group_name": group,  # store as group_name in DB
        "bank_account_number": bank_account_number,
        "bank_ifsc_code": bank_ifsc_code,
        "bank_holder_name": bank_holder_name,
        "bank_branch_name": bank_branch_name,
        "security_question": security_question,
        "security_answer": security_answer,
        "address_proof": address_proof,
        "id_proof": id_proof,
        "user_type": fixed_user_type,  # Always use "live" regardless of input
        "fund_manager": fund_manager,
        "is_self_trading": is_self_trading,
        "wallet_balance": Decimal("0.0"),
        "margin": Decimal("0.0"),
        "leverage": Decimal("100.0"),
        "status": 1,
        "isActive": isActive if isActive is not None else 1,
        "account_number": await generate_unique_account_number(db),
    }

    hashed_password = get_password_hash(password)

    try:
        new_user = await crud_user.create_user(
            db=db,
            user_data=user_data,
            hashed_password=hashed_password,
            id_proof_path=id_proof,
            id_proof_image_path=id_proof_image_path,
            address_proof_path=address_proof,
            address_proof_image_path=address_proof_image_path
        )
        return new_user

    except IntegrityError:
        await db.rollback()
        if id_proof_image_real_path and os.path.exists(id_proof_image_real_path):
            os.remove(id_proof_image_real_path)
        if address_proof_image_real_path and os.path.exists(address_proof_image_real_path):
            os.remove(address_proof_image_real_path)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="User with this email or phone number already exists."
        )

    except Exception as e:
        await db.rollback()
        if id_proof_image_real_path and os.path.exists(id_proof_image_real_path):
            os.remove(id_proof_image_real_path)
        if address_proof_image_real_path and os.path.exists(address_proof_image_real_path):
            os.remove(address_proof_image_real_path)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred during registration."
        )

@router.post("/login", response_model=Token, summary="User Login (JSON)")
async def login_with_user_type(
    credentials: UserLogin,
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client)
):
    # Always require email and user_type to distinguish demo/live
    user_type = credentials.user_type if credentials.user_type else "live"
    user = await crud_user.get_user_by_email_and_type(db, email=credentials.email, user_type=user_type)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email, user type, or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not verify_password(credentials.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if getattr(user, 'isActive', 0) != 1:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User account is not active or verified."
        )

    access_token_expires = datetime.timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    refresh_token_expires = datetime.timedelta(minutes=settings.REFRESH_TOKEN_EXPIRE_MINUTES)

    access_token = create_access_token(
        data={"sub": str(user.id), "user_type": user_type, "account_number": user.account_number},
        expires_delta=access_token_expires
    )
    refresh_token = create_refresh_token(
        data={"sub": str(user.id), "user_type": user_type, "account_number": user.account_number},
        expires_delta=refresh_token_expires
    )

    await store_refresh_token(client=redis_client, user_id=user.id, refresh_token=refresh_token)

    return Token(access_token=access_token, refresh_token=refresh_token, token_type="bearer")


from fastapi import Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

bearer_scheme = HTTPBearer(auto_error=True)

@router.post("/refresh-token", response_model=Token, summary="Refresh Access Token")
async def refresh_access_token(
    credentials: HTTPAuthorizationCredentials = Security(bearer_scheme),
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client)
):
    """
    Refreshes an access token using a valid refresh token provided as a Bearer token in the Authorization header.
    """
    refresh_token = credentials.credentials
    try:
        payload = decode_token(refresh_token)
        user_id_from_payload_str: Optional[str] = payload.get("sub")
        if user_id_from_payload_str is None:
            logger.warning("Refresh token payload missing 'sub' claim.")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid refresh token payload",
                headers={"WWW-Authenticate": "Bearer"},
            )

        logger.info(f"Refresh token decoded. User ID from token payload: {user_id_from_payload_str}")

        redis_token_data = await get_refresh_token_data(client=redis_client, refresh_token=token_refresh.refresh_token)

        if redis_token_data:
             logger.debug(f"Comparing Redis user_id (type: {type(redis_token_data.get('user_id'))}, value: {redis_token_data.get('user_id')}) with decoded user_id (type: {type(user_id_from_payload_str)}, value: {user_id_from_payload_str})")
        else:
             logger.debug("redis_token_data is None. Cannot perform user_id comparison.")

        if not redis_token_data or (redis_token_data and str(redis_token_data.get("user_id")) != user_id_from_payload_str):
             logger.warning(f"Refresh token validation failed for user ID {user_id_from_payload_str}. Data found: {bool(redis_token_data)}, User ID match: {str(redis_token_data.get('user_id')) == user_id_from_payload_str if redis_token_data else False}")
             raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired refresh token",
                headers={"WWW-Authenticate": "Bearer"},
            )

        logger.info(f"Refresh token validated successfully for user ID: {user_id_from_payload_str}")

        user = await crud_user.get_user_by_id(db, user_id=int(user_id_from_payload_str))

        if user is None or getattr(user, 'isActive', 0) != 1:
             logger.warning(f"Refresh token valid, but user ID {user_id_from_payload_str} not found or inactive.")
             try:
                 await delete_refresh_token(client=redis_client, refresh_token=token_refresh.refresh_token)
                 logger.info(f"Invalidated refresh token for invalid user ID {user_id_from_payload_str}.")
             except Exception as delete_e:
                 logger.error(f"Error deleting invalid refresh token for user ID {user_id_from_payload_str}: {delete_e}", exc_info=True)
             raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User not found or inactive",
                headers={"WWW-Authenticate": "Bearer"},
            )

        access_token_expires = datetime.timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
        refresh_token_expires = datetime.timedelta(minutes=settings.REFRESH_TOKEN_EXPIRE_MINUTES)
        # Optionally, include user_type if present in the original token
        user_type = payload.get("user_type", getattr(user, "user_type", "live"))
        account_number = payload.get("account_number", getattr(user, "account_number", None))
        new_access_token = create_access_token(
            data={"sub": str(user.id), "user_type": user_type, "account_number": account_number},
            expires_delta=access_token_expires
        )
        new_refresh_token = create_refresh_token(
            data={"sub": str(user.id), "user_type": user_type, "account_number": account_number},
            expires_delta=refresh_token_expires
        )
        await store_refresh_token(client=redis_client, user_id=user.id, refresh_token=new_refresh_token)
        logger.info(f"New access and refresh tokens generated for user ID {user.id} using refresh token.")
        return Token(access_token=new_access_token, refresh_token=new_refresh_token, token_type="bearer")

    except JWTError:
        logger.warning("JWTError during refresh token validation.", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except Exception as e:
        logger.error(f"Unexpected error during refresh token process for token: {token_refresh.refresh_token[:20]}... : {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while refreshing token."
        )


@router.post("/logout", response_model=StatusResponse, summary="User Logout")
async def logout_user(
    credentials: HTTPAuthorizationCredentials = Security(bearer_scheme),
    current_user: User = Depends(get_current_user),
    redis_client: Redis = Depends(get_redis_client)
):
    """
    Logs out a user by invalidating their refresh token in Redis. The refresh token must be supplied as a Bearer token in the Authorization header.
    """
    refresh_token = credentials.credentials
    try:
        await delete_refresh_token(client=redis_client, refresh_token=refresh_token)
        logger.info(f"Logout successful for user ID {current_user.id} by invalidating refresh token.")
        return StatusResponse(message="Logout successful.")
    except Exception as e:
        logger.error(f"Error during logout for user ID {current_user.id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred during logout."
        )

@router.get(
    "/users",
    response_model=List[UserResponse],
    summary="Get all users (Admin Only)",
    description="Retrieves a list of all registered users (requires admin authentication)."
)
async def read_users(
    skip: int = Query(0, description="Number of users to skip"),
    limit: int = Query(100, description="Maximum number of users to return"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user)
):
    users = await crud_user.get_all_users(db, skip=skip, limit=limit)
    return users

@router.get(
    "/users/demo",
    response_model=List[DemoUserResponse], # Changed response model to DemoUserResponse
    summary="Get all demo users (Admin Only)",
    description="Retrieves a list of all registered demo users (requires admin authentication)."
)
async def read_demo_users(
    skip: int = Query(0, description="Number of demo users to skip"),
    limit: int = Query(100, description="Maximum number of demo users to return"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user)
):
    demo_users = await crud_user.get_all_demo_users(db, skip=skip, limit=limit) # Changed to get_all_demo_users
    return demo_users

@router.get(
    "/users/live",
    response_model=List[UserResponse],
    summary="Get all live users (Admin Only)",
    description="Retrieves a list of all registered live users (requires admin authentication)."
)
async def read_live_users(
    skip: int = Query(0, description="Number of live users to skip"),
    limit: int = Query(100, description="Maximum number of live users to return"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user)
):
    users = await crud_user.get_live_users(db, skip=skip, limit=limit)
    return users


from typing import Union
from app.schemas.user import UserResponse
from app.schemas.demo_user import DemoUserResponse
from app.core.security import get_user_from_service_or_user_token
from fastapi import Request

@router.get("/me", response_model=Union[UserResponse, DemoUserResponse], summary="Get current user details (live or demo)")
async def read_users_me(
    request: Request,
    current_user: User | DemoUser = Depends(get_user_from_service_or_user_token)
):
    """
    Retrieves the details of the currently authenticated user (live or demo), based on the JWT token's user_type.
    Returns the correct schema for the user type.
    """
    if hasattr(current_user, 'user_type') and getattr(current_user, 'user_type', None) == 'demo':
        return DemoUserResponse(**current_user.__dict__)
    else:
        return UserResponse(**current_user.__dict__)


@router.patch(
    "/users/{user_id}",
    response_model=UserResponse,
    summary="Update a user by ID (Admin Only)",
    description="Updates the details of a specific user by ID (requires admin authentication)."
)
async def update_user_by_id(
    user_id: int,
    user_update: UserUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user)
):
    update_data_dict = user_update.model_dump(exclude_unset=True)
    sensitive_fields = ["wallet_balance", "leverage", "margin", "group_name", "status", "isActive", "security_answer"] # Added security_answer
    needs_locking = any(field in update_data_dict for field in sensitive_fields)

    if needs_locking:
        db_user = await crud_user.get_user_by_id_with_lock(db, user_id=user_id)
        if db_user is None:
             raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )
        logger.info(f"Acquired lock for user ID {user_id} during update.")
    else:
        db_user = await crud_user.get_user_by_id(db, user_id=user_id)
        if db_user is None:
             raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )

    try:
        updated_user = await crud_user.update_user(db=db, db_user=db_user, user_update=user_update)
        logger.info(f"User ID {user_id} updated successfully by admin {current_user.id}.")
        return updated_user

    except Exception as e:
        await db.rollback()
        logger.error(f"Error updating user ID {user_id} by admin {current_user.id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while updating the user."
        )

@router.delete(
    "/users/{user_id}",
    response_model=StatusResponse,
    summary="Delete a user by ID (Admin Only)",
    description="Deletes a specific user by ID (requires admin authentication)."
)
async def delete_user_by_id(
    user_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user)
):
    db_user = await crud_user.get_user_by_id(db, user_id=user_id)
    if db_user is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    try:
        await crud_user.delete_user(db=db, db_user=db_user)
        logger.info(f"User ID {user_id} deleted successfully by admin {current_user.id}.")
        return StatusResponse(message=f"User with ID {user_id} deleted successfully.")

    except Exception as e:
        await db.rollback()
        logger.error(f"Error deleting user ID {user_id} by admin {current_user.id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while deleting the user."
        )


@router.post(
    "/signup/send-otp",
    response_model=StatusResponse,
    summary="Send OTP for new user email verification by account type",
)
async def signup_send_otp(
    request_data: SignupSendOTPRequest,
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client)
):
    user = await crud_user.get_user_by_email_and_type(db, email=request_data.email, user_type=request_data.user_type)

    if user and getattr(user, 'isActive', 0) == 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email is already registered and active for this account type."
        )

    otp_code = crud_otp.generate_otp_code()
    email_subject = f"Verify Your Email for {request_data.user_type.capitalize()} Account"
    email_body = f"Your One-Time Password (OTP) for email verification for your {request_data.user_type} account is: {otp_code}\n\nThis OTP is valid for {settings.OTP_EXPIRATION_MINUTES} minutes."

    if user and getattr(user, 'isActive', 0) == 0:
        await crud_otp.create_otp(db, user_id=user.id, force_otp_code=otp_code)
        logger.info(f"Sent OTP to existing inactive user {request_data.email} (type: {request_data.user_type}) for activation.")
    else:
        redis_key = f"signup_otp:{request_data.email}:{request_data.user_type}"
        await redis_client.set(redis_key, otp_code, ex=int(settings.OTP_EXPIRATION_MINUTES * 60))
        logger.info(f"Stored OTP in Redis for new email {request_data.email} (type: {request_data.user_type}).")

    try:
        await email_service.send_email(
            to_email=request_data.email,
            subject=email_subject,
            body=email_body
        )
        logger.info(f"Signup OTP sent successfully to {request_data.email} for account type {request_data.user_type}.")
    except Exception as e:
        logger.error(f"Error sending signup OTP to {request_data.email} for type {request_data.user_type}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to send OTP. Please try again later."
        )
    return StatusResponse(message="OTP sent successfully to your email.")


@router.post(
    "/signup/verify-otp",
    response_model=StatusResponse,
    summary="Verify OTP for new user email by account type or activate existing",
)
async def signup_verify_otp(
    request_data: SignupVerifyOTPRequest,
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client)
):
    redis_key_signup_otp = f"signup_otp:{request_data.email}:{request_data.user_type}"
    stored_otp_in_redis = await redis_client.get(redis_key_signup_otp)

    if stored_otp_in_redis:
        if stored_otp_in_redis == request_data.otp_code:
            await redis_client.delete(redis_key_signup_otp)
            redis_key_preverified = f"preverified_email:{request_data.email}:{request_data.user_type}"
            await redis_client.set(redis_key_preverified, "1", ex=15 * 60)
            logger.info(f"OTP for new email {request_data.email} (type: {request_data.user_type}) verified via Redis.")
            return StatusResponse(message="Email verified successfully. Please complete your registration.")
        else:
            logger.warning(f"Invalid Redis OTP for {request_data.email} (type: {request_data.user_type}).")
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid or expired OTP.")
    else:
        user = await crud_user.get_user_by_email_and_type(db, email=request_data.email, user_type=request_data.user_type)
        if user and getattr(user, 'isActive', 0) == 0:
            otp_record = await crud_otp.get_valid_otp(db, user_id=user.id, otp_code=request_data.otp_code) # Pass user_id
            if not otp_record:
                logger.warning(f"Invalid DB OTP for inactive user {request_data.email} (type: {request_data.user_type}).")
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid or expired OTP.")
            user.status = 1
            user.isActive = 1
            try:
                await db.commit()
                await crud_otp.delete_otp(db, otp_id=otp_record.id)
                logger.info(f"Existing inactive user {request_data.email} (type: {request_data.user_type}, ID: {user.id}) activated.")
                return StatusResponse(message="Account activated successfully. You can now login.")
            except Exception as e:
                await db.rollback()
                logger.error(f"Error activating user ID {user.id} (type: {request_data.user_type}): {e}", exc_info=True)
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An error occurred during account activation.")
        else:
            logger.warning(f"No OTP in Redis and no matching inactive user for {request_data.email} (type: {request_data.user_type}).")
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid OTP, or email not eligible for this verification process.")


@router.post("/request-password-reset", response_model=StatusResponse)
async def request_password_reset(
    payload: RequestPasswordReset,
    db: AsyncSession = Depends(get_db),
):
    user = await crud_user.get_user_by_email_and_type(db, payload.email, payload.user_type)
    if not user:
        return StatusResponse(message="If a user exists, an OTP has been sent.")

    otp = await crud_otp.create_otp(db, user_id=user.id) # Pass user_id
    await email_service.send_email(
        user.email,
        "Password Reset OTP",
        f"Your OTP is: {otp.otp_code}"
    )
    return StatusResponse(message="Password reset OTP sent successfully.")

from uuid import uuid4
from app.schemas.user import PasswordResetVerifyResponse, PasswordResetConfirmRequest

@router.post("/verify-password-reset-otp", response_model=PasswordResetVerifyResponse)
async def verify_password_reset_otp(
    payload: VerifyOTPRequest,
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis_client)
):
    user_type = payload.user_type if payload.user_type else "live"
    user = await crud_user.get_user_by_email_and_type(db, payload.email, user_type)
    if not user:
        raise HTTPException(status_code=400, detail="Invalid credentials.")

    otp = await crud_otp.get_valid_otp(db, user_id=user.id, otp_code=payload.otp_code)
    if not otp:
        raise HTTPException(status_code=400, detail="Invalid or expired OTP.")

    reset_token = str(uuid4())
    redis_key = f"reset_token:{user.email}:{user_type}"
    await redis.setex(redis_key, settings.OTP_EXPIRATION_MINUTES * 60, reset_token)
    await redis.setex(crud_otp.get_otp_flag_key(user.email, user_type), settings.OTP_EXPIRATION_MINUTES * 60, "1")
    return PasswordResetVerifyResponse(verified=True, message="OTP verified successfully.", reset_token=reset_token)


@router.post("/reset-password-confirm", response_model=StatusResponse)
async def confirm_password_reset(
    payload: PasswordResetConfirmRequest,
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis_client)
):
    user_type = payload.user_type if payload.user_type else "live"
    user = await crud_user.get_user_by_email_and_type(db, payload.email, user_type)
    if not user:
        raise HTTPException(status_code=400, detail="Invalid email or user type.")

    redis_key = f"reset_token:{user.email}:{user_type}"
    stored_token = await redis.get(redis_key)
    if not stored_token or stored_token.decode() != payload.reset_token:
        raise HTTPException(status_code=403, detail="Invalid or expired reset token.")

    user.hashed_password = get_password_hash(payload.new_password)
    await db.commit()
    await crud_otp.delete_all_user_otps(db, user.id)
    await redis.delete(redis_key)

    return StatusResponse(message="Password reset successful.")


@router.post("/wallet/deposit", response_model=MoneyRequestResponse)
async def request_deposit_funds(
    request: WalletTransactionRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    if request.amount <= 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Amount to deposit must be positive."
        )

    money_request_data = MoneyRequestCreate(
        amount=request.amount,
        type="deposit"
    )

    try:
        new_money_request = await crud_money_request.create_money_request(
            db=db,
            request_data=money_request_data,
            user_id=current_user.id
        )
        return new_money_request

    except Exception as e:
        logger.error(f"Error creating deposit request: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while creating the deposit request."
        )

@router.post("/wallet/withdraw", response_model=MoneyRequestResponse)
async def request_withdraw_funds(
    request: WalletTransactionRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    if request.amount <= 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Amount to withdraw must be positive."
        )

    money_request_data = MoneyRequestCreate(
        amount=request.amount,
        type="withdraw"
    )

    try:
        new_money_request = await crud_money_request.create_money_request(
            db=db,
            request_data=money_request_data,
            user_id=current_user.id
        )
        return new_money_request

    except Exception as e:
        logger.error(f"Error creating withdrawal request: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while creating the withdrawal request."
        )

# --- Demo User Endpoints ---

@router.post(
    "/demo/register",
    response_model=DemoUserResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Register a new demo user",
    description="Creates a new demo user account with the provided details."
)
async def register_demo_user(
    name: str = Form(...),
    email: str = Form(...),
    phone_number: str = Form(..., max_length=20),
    password: str = Form(..., min_length=8),
    security_answer: Optional[str] = Form(None), # Added security_answer
    city: Optional[str] = Form(None), # Changed to Optional to match schema more closely if None is intended as default
    state: Optional[str] = Form(None), # Changed to Optional
    pincode: Optional[int] = Form(None),
    security_question: Optional[str] = Form(None),
    group_name: Optional[str] = Form(None),
    db: AsyncSession = Depends(get_db)
):
    # Always set user_type to "demo" regardless of what might be provided from frontend
    fixed_user_type = "demo"

    existing_user = await crud_user.get_user_by_email_phone_type(db, email=email, phone_number=phone_number, user_type=fixed_user_type)
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Demo user with this email and phone number already exists."
        )

    demo_user_data = {
        "name": name,
        "email": email,
        "phone_number": phone_number,
        "city": city,
        "state": state,
        "pincode": pincode,
        "user_type": fixed_user_type, # Always use "demo" regardless of input
        "security_question": security_question,
        "group_name": group_name,
        "wallet_balance": Decimal("100000.0"), # Starting balance for demo
        "leverage": Decimal("100.0"), # Default leverage for demo
        "margin": Decimal("0.0"),
        "status": 1, # Active by default for demo
        "isActive": 1, # Active by default for demo
        "account_number": await generate_unique_demo_account_number(db),
    }

    hashed_password = get_password_hash(password)

    try:
        new_demo_user = await crud_user.create_demo_user(
            db=db,
            demo_user_data=demo_user_data,
            hashed_password=hashed_password,
            security_answer=security_answer, # Pass security_answer
        )
        return new_demo_user

    except IntegrityError:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Demo user with this email or phone number already exists."
        )
    except Exception as e:
        await db.rollback()
        logger.error(f"Error during demo user registration: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred during demo user registration."
        )

@router.post("/demo/login", response_model=Token, summary="Demo User Login (JSON)")
async def login_demo_user(
    credentials: DemoUserLogin,
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client)
):
    demo_user = await crud_user.get_demo_user_by_email(db, email=credentials.email)
    if not demo_user:
        demo_user = await crud_user.get_demo_user_by_phone_number(db, phone_number=credentials.email)

    if not demo_user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password for demo user",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not verify_password(credentials.password, demo_user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect password for demo user",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if getattr(demo_user, 'isActive', 0) != 1:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Demo user account is not active."
        )

    access_token_expires = datetime.timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    refresh_token_expires = datetime.timedelta(minutes=settings.REFRESH_TOKEN_EXPIRE_MINUTES)

    access_token = create_access_token(
        data={"sub": str(demo_user.id), "user_type": "demo", "account_number": demo_user.account_number},
        expires_delta=access_token_expires
    )
    refresh_token = create_refresh_token(
        data={"sub": str(demo_user.id), "user_type": "demo", "account_number": demo_user.account_number},
        expires_delta=refresh_token_expires
    )

    await store_refresh_token(client=redis_client, user_id=demo_user.id, refresh_token=refresh_token, user_type="demo") # Store user_type
    return Token(access_token=access_token, refresh_token=refresh_token, token_type="bearer")


@router.post(
    "/demo/signup/send-otp",
    response_model=StatusResponse,
    summary="Send OTP for new demo user email verification",
)
async def demo_signup_send_otp(
    request_data: DemoSendOTPRequest,
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client)
):
    demo_user = await crud_user.get_demo_user_by_email(db, email=request_data.email)

    if demo_user and getattr(demo_user, 'isActive', 0) == 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email is already registered and active for a demo account."
        )

    otp_code = crud_otp.generate_otp_code()
    email_subject = "Verify Your Email for Demo Account"
    email_body = f"Your One-Time Password (OTP) for email verification for your demo account is: {otp_code}\n\nThis OTP is valid for {settings.OTP_EXPIRATION_MINUTES} minutes."

    if demo_user and getattr(demo_user, 'isActive', 0) == 0:
        await crud_otp.create_otp(db, demo_user_id=demo_user.id, force_otp_code=otp_code) # Pass demo_user_id
        logger.info(f"Sent OTP to existing inactive demo user {request_data.email} for activation.")
    else:
        redis_key = f"signup_otp:{request_data.email}:demo" # Specific key for demo
        await redis_client.set(redis_key, otp_code, ex=int(settings.OTP_EXPIRATION_MINUTES * 60))
        logger.info(f"Stored OTP in Redis for new demo email {request_data.email}.")

    try:
        await email_service.send_email(
            to_email=request_data.email,
            subject=email_subject,
            body=email_body
        )
        logger.info(f"Signup OTP sent successfully to {request_data.email} for demo account.")
    except Exception as e:
        logger.error(f"Error sending signup OTP to {request_data.email} for demo: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to send OTP. Please try again later."
        )
    return StatusResponse(message="OTP sent successfully to your email.")


@router.post(
    "/demo/signup/verify-otp",
    response_model=StatusResponse,
    summary="Verify OTP for new demo user email or activate existing",
)
async def demo_signup_verify_otp(
    request_data: DemoVerifyOTPRequest,
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client)
):
    redis_key_signup_otp = f"signup_otp:{request_data.email}:demo"
    stored_otp_in_redis = await redis_client.get(redis_key_signup_otp)

    if stored_otp_in_redis:
        if stored_otp_in_redis == request_data.otp_code:
            await redis_client.delete(redis_key_signup_otp)
            redis_key_preverified = f"preverified_email:{request_data.email}:demo"
            await redis_client.set(redis_key_preverified, "1", ex=15 * 60)
            logger.info(f"OTP for new demo email {request_data.email} verified via Redis.")
            return StatusResponse(message="Email verified successfully. Please complete your registration.")
        else:
            logger.warning(f"Invalid Redis OTP for demo {request_data.email}.")
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid or expired OTP.")
    else:
        demo_user = await crud_user.get_demo_user_by_email(db, email=request_data.email)
        if demo_user and getattr(demo_user, 'isActive', 0) == 0:
            otp_record = await crud_otp.get_valid_otp(db, demo_user_id=demo_user.id, otp_code=request_data.otp_code) # Pass demo_user_id
            if not otp_record:
                logger.warning(f"Invalid DB OTP for inactive demo user {request_data.email}.")
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid or expired OTP.")
            demo_user.status = 1
            demo_user.isActive = 1
            try:
                await db.commit()
                await crud_otp.delete_otp(db, otp_id=otp_record.id)
                logger.info(f"Existing inactive demo user {request_data.email} (ID: {demo_user.id}) activated.")
                return StatusResponse(message="Demo account activated successfully. You can now login.")
            except Exception as e:
                await db.rollback()
                logger.error(f"Error activating demo user ID {demo_user.id}: {e}", exc_info=True)
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An error occurred during demo account activation.")
        else:
            logger.warning(f"No OTP in Redis and no matching inactive demo user for {request_data.email}.")
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid OTP, or email not eligible for this verification process.")


@router.post("/demo/request-password-reset", response_model=StatusResponse)
async def demo_request_password_reset(
    payload: DemoRequestPasswordReset,
    db: AsyncSession = Depends(get_db),
):
    demo_user = await crud_user.get_demo_user_by_email(db, payload.email)
    if not demo_user:
        return StatusResponse(message="If a demo user exists, an OTP has been sent.")

    otp = await crud_otp.create_otp(db, demo_user_id=demo_user.id) # Pass demo_user_id
    await email_service.send_email(
        demo_user.email,
        "Demo Account Password Reset OTP",
        f"Your OTP is: {otp.otp_code}"
    )
    return StatusResponse(message="Demo account password reset OTP sent successfully.")

from uuid import uuid4
from app.schemas.user import PasswordResetVerifyResponse, PasswordResetConfirmRequest

@router.post("/demo/verify-password-reset-otp", response_model=PasswordResetVerifyResponse)
async def demo_verify_password_reset_otp(
    payload: DemoVerifyOTPRequest,
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis_client)
):
    user_type = payload.user_type if payload.user_type else "demo"
    demo_user = await crud_user.get_demo_user_by_email(db, payload.email)
    if not demo_user:
        raise HTTPException(status_code=400, detail="Invalid credentials for demo user.")

    otp = await crud_otp.get_valid_otp(db, demo_user_id=demo_user.id, otp_code=payload.otp_code)
    if not otp:
        raise HTTPException(status_code=400, detail="Invalid or expired OTP.")

    reset_token = str(uuid4())
    redis_key = f"reset_token:{demo_user.email}:{user_type}"
    await redis.setex(redis_key, settings.OTP_EXPIRATION_MINUTES * 60, reset_token)
    await redis.setex(crud_otp.get_otp_flag_key(demo_user.email, user_type), settings.OTP_EXPIRATION_MINUTES * 60, "1")
    return PasswordResetVerifyResponse(verified=True, message="Demo OTP verified successfully.", reset_token=reset_token)


@router.post("/demo/reset-password-confirm", response_model=StatusResponse)
async def demo_confirm_password_reset(
    payload: DemoResetPasswordConfirm,
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis_client)
):
    user_type = payload.user_type if payload.user_type else "demo"
    demo_user = await crud_user.get_demo_user_by_email(db, payload.email)
    if not demo_user:
        raise HTTPException(status_code=400, detail="Invalid email for demo user.")

    redis_key = f"reset_token:{demo_user.email}:{user_type}"
    stored_token = await redis.get(redis_key)
    if not stored_token or stored_token.decode() != payload.reset_token:
        raise HTTPException(status_code=403, detail="Invalid or expired reset token.")

    demo_user.hashed_password = get_password_hash(payload.new_password)
    await db.commit()
    await crud_otp.delete_all_demo_user_otps(db, demo_user.id)
    await redis.delete(redis_key)

    return StatusResponse(message="Demo password reset successful.")


@router.post("/demo/wallet/deposit", response_model=MoneyRequestResponse)
async def request_deposit_funds_demo(
    request: WalletTransactionRequest,
    current_user: DemoUser = Depends(get_current_user), # Assuming get_current_user can return DemoUser based on token
    db: AsyncSession = Depends(get_db)
):
    if request.amount <= 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Amount to deposit must be positive."
        )

    money_request_data = MoneyRequestCreate(
        amount=request.amount,
        type="deposit"
    )

    try:
        # For demo users, money requests are handled directly as wallet updates
        # Assuming we don't need a separate MoneyRequest table for demo users,
        # and instead directly update their wallet balance.
        # If you DO need a MoneyRequest for demo users, you'd need to add a
        # demo_user_id foreign key to the MoneyRequest model.
        updated_demo_user = await crud_user.update_demo_user_wallet_balance(
            db=db,
            demo_user_id=current_user.id,
            amount=request.amount,
            transaction_type="deposit",
            description=request.description or "Demo funds added by user request"
        )
        return MoneyRequestResponse(
            id=0, # Placeholder ID as no MoneyRequest object is created
            user_id=current_user.id,
            amount=request.amount,
            type="deposit",
            status=1, # Approved immediately
            created_at=datetime.datetime.utcnow(),
            updated_at=datetime.datetime.utcnow()
        )
    except ValueError as ve:
        logger.warning(f"Demo wallet deposit failed for user {current_user.id}: {ve}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(ve)
        )
    except Exception as e:
        logger.error(f"Error creating demo deposit request: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while creating the demo deposit request."
        )

@router.post("/demo/wallet/withdraw", response_model=MoneyRequestResponse)
async def request_withdraw_funds_demo(
    request: WalletTransactionRequest,
    current_user: DemoUser = Depends(get_current_user), # Assuming get_current_user can return DemoUser based on token
    db: AsyncSession = Depends(get_db)
):
    if request.amount <= 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Amount to withdraw must be positive."
        )

    try:
        updated_demo_user = await crud_user.update_demo_user_wallet_balance(
            db=db,
            demo_user_id=current_user.id,
            amount=-request.amount, # Negative amount for withdrawal
            transaction_type="withdrawal",
            description=request.description or "Demo funds withdrawn by user request"
        )
        return MoneyRequestResponse(
            id=0, # Placeholder ID
            user_id=current_user.id,
            amount=request.amount,
            type="withdraw",
            status=1, # Approved immediately
            created_at=datetime.datetime.utcnow(),
            updated_at=datetime.datetime.utcnow()
        )
    except ValueError as ve:
        logger.warning(f"Demo wallet withdrawal failed for user {current_user.id}: {ve}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(ve)
        )
    except Exception as e:
        logger.error(f"Error creating demo withdrawal request: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while creating the demo withdrawal request."
        )