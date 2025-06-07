from typing import Optional
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, EmailStr, Field

# Base schema for common attributes
class DemoUserBase(BaseModel):
    name: str
    email: EmailStr
    phone_number: str
    user_type: Optional[str] = "demo" # Default to "demo" for DemoUser
    wallet_balance: Decimal = Field(default=Decimal("0.00"), decimal_places=8)
    leverage: Decimal = Field(default=Decimal("1.0"), decimal_places=2)
    margin: Decimal = Field(default=Decimal("0.00"), decimal_places=8)
    account_number: Optional[str] = None
    group_name: Optional[str] = None
    status: int = 0 # 0: inactive/pending, 1: active
    security_question: Optional[str] = None
    security_answer: Optional[str] = None # Added security_answer
    city: Optional[str] = None
    state: Optional[str] = None
    pincode: Optional[int] = None
    isActive: int = 0 # 0: not active, 1: active
    referred_by_id: Optional[int] = None
    reffered_code: Optional[str] = None

    class Config:
        from_attributes = True # For Pydantic V2+, use from_attributes instead of orm_mode = True

# Schema for creating a new demo user
class DemoUserCreate(DemoUserBase):
    hashed_password: str # Password should be hashed before passing to this schema

# Schema for updating an existing demo user
class DemoUserUpdate(DemoUserBase):
    name: Optional[str] = None
    email: Optional[EmailStr] = None
    phone_number: Optional[str] = None
    hashed_password: Optional[str] = None # Allow updating password, but hash before passing
    user_type: Optional[str] = None
    wallet_balance: Optional[Decimal] = None
    leverage: Optional[Decimal] = None
    margin: Optional[Decimal] = None
    account_number: Optional[str] = None
    group_name: Optional[str] = None
    status: Optional[int] = None
    security_question: Optional[str] = None
    security_answer: Optional[str] = None # Allow updating security_answer
    city: Optional[str] = None
    state: Optional[str] = None
    pincode: Optional[int] = None
    isActive: Optional[int] = None
    referred_by_id: Optional[int] = None
    reffered_code: Optional[str] = None

# Schema for demo user data as stored in the database (includes IDs and timestamps)
class DemoUserInDBBase(DemoUserBase):
    id: int
    created_at: datetime
    updated_at: datetime

# Full schema for reading demo user data (what you'd typically return from an API)
# Renamed from DemoUser to DemoUserResponse to match imports in users.py
class DemoUserResponse(DemoUserInDBBase):
    pass

# Schema for Demo User Login
class DemoUserLogin(BaseModel):
    email: str # Email address for login
    password: str

# Schema for sending OTP for Demo User signup/verification
class DemoSendOTPRequest(BaseModel):
    email: EmailStr
    user_type: Optional[str] = Field("demo", description="User type, defaults to 'demo'.")

# Schema for verifying OTP for Demo User signup/verification
class DemoVerifyOTPRequest(BaseModel):
    email: EmailStr
    otp_code: str = Field(..., min_length=6, max_length=10)
    user_type: Optional[str] = Field("demo", description="User type, defaults to 'demo'.")

# Schema for requesting password reset for Demo User
class DemoRequestPasswordReset(BaseModel):
    email: EmailStr
    user_type: Optional[str] = Field("demo", description="User type, defaults to 'demo'.")

# Schema for confirming password reset for Demo User
class DemoResetPasswordConfirm(BaseModel):
    email: EmailStr
    new_password: str = Field(..., min_length=8)
    confirm_password: str = Field(..., min_length=8)
    user_type: Optional[str] = Field("demo", description="User type, defaults to 'demo'.")
    reset_token: str = Field(..., description="Reset token obtained after OTP verification.")

    class Config:
        # Add a custom validator if needed to ensure new_password == confirm_password
        pass

class PasswordResetVerifyResponse(BaseModel):
    verified: bool = Field(..., description="Whether the OTP was successfully verified.")
    message: str = Field(..., description="Response message.")
    reset_token: Optional[str] = Field(None, description="Reset token to be used for confirming password reset.")

class PasswordResetConfirmRequest(BaseModel):
    email: EmailStr = Field(..., description="Email address associated with the OTP.")
    user_type: Optional[str] = Field("demo", description="User type, defaults to 'demo'.")
    new_password: str = Field(..., min_length=8, description="The new password for the user account.")
    reset_token: str = Field(..., description="Reset token obtained after OTP verification.")

