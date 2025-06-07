# app/scripts/create_admin.py

import asyncio
import sys
import os
import logging
from decimal import Decimal
import uuid

# Add parent directory to path so we can import from app
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from app.database.session import AsyncSessionLocal
from app.database.models import User
from app.core.security import get_password_hash
from sqlalchemy.future import select

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def create_admin_user(email: str, password: str, name: str = "Admin User"):
    """
    Create an admin user in the database.
    """
    async with AsyncSessionLocal() as db:
        # Check if admin with this email already exists
        result = await db.execute(
            select(User).filter(User.email == email, User.user_type == "admin")
        )
        existing_admin = result.scalars().first()
        
        if existing_admin:
            logger.info(f"Admin user with email {email} already exists.")
            return
        
        # Generate a unique account number
        account_number = str(uuid.uuid4())[:5].upper()
        
        # Create new admin user
        admin_user = User(
            name=name,
            email=email,
            phone_number="admin",  # Placeholder, update if needed
            hashed_password=get_password_hash(password),
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
        
        logger.info(f"Admin user created successfully with ID: {admin_user.id}")
        logger.info(f"Email: {email}")
        logger.info(f"Account Number: {account_number}")
        
        return admin_user

async def main():
    """
    Main function to create an admin user.
    """
    if len(sys.argv) < 3:
        print("Usage: python create_admin.py <email> <password> [name]")
        return
    
    email = sys.argv[1]
    password = sys.argv[2]
    name = sys.argv[3] if len(sys.argv) > 3 else "Admin User"
    
    await create_admin_user(email, password, name)

if __name__ == "__main__":
    asyncio.run(main()) 