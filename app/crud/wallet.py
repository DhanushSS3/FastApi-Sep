# app/crud/wallet.py

import datetime
import uuid
from decimal import Decimal
# Changed from sqlalchemy.orm import Session to sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError
from app.database.models import Wallet  # Import the Wallet model from models.py
# Import the new WalletCreate schema
from app.schemas.wallet import WalletCreate

import logging # Import logging

from sqlalchemy import select

from typing import List

logger = logging.getLogger(__name__) # Get logger for this module


# Changed function signature to accept WalletCreate schema
async def create_wallet_record(
    db: AsyncSession,
    wallet_data: WalletCreate
) -> Wallet | None:
    """
    Creates a new wallet transaction record using data from a WalletCreate schema.

    Args:
        db: The asynchronous database session.
        wallet_data: A WalletCreate Pydantic schema object containing transaction details.

    Returns:
        The newly created Wallet object, or None on error.
    """
    try:
        # Generate a unique transaction ID
        transaction_id = str(uuid.uuid4())
        logger.debug(f"Generated transaction ID: {transaction_id}")

        # Create the wallet transaction object
        wallet_record = Wallet(
            user_id=wallet_data.user_id,
            order_quantity=wallet_data.order_quantity,
            symbol=wallet_data.symbol,
            transaction_type=wallet_data.transaction_type,
            is_approved=wallet_data.is_approved,
            order_type=wallet_data.order_type,
            transaction_amount=wallet_data.transaction_amount,
            transaction_id=transaction_id,
            description=wallet_data.description,
        )

        # Add and flush to prepare for refresh
        db.add(wallet_record)
        await db.flush()                 # ✅ Safe flush instead of commit
        await db.refresh(wallet_record)  # ✅ Still inside open transaction

        logger.info(
            f"Wallet record created successfully for user {wallet_data.user_id} "
            f"with transaction ID {transaction_id}."
        )
        return wallet_record

    except SQLAlchemyError as e:
        logger.error(
            f"Database error creating wallet record for user {wallet_data.user_id}: {e}",
            exc_info=True
        )
        await db.rollback()
        return None

    except Exception as e:
        logger.error(
            f"Unexpected error creating wallet record for user {wallet_data.user_id}: {e}",
            exc_info=True
        )
        await db.rollback()
        return None

# Example function to get wallet records by user ID (Async)
async def get_wallet_records_by_user_id(
    db: AsyncSession, user_id: int, skip: int = 0, limit: int = 100
) -> List[Wallet]:
    """
    Retrieves wallet transaction records for a specific user with pagination.
    """
    result = await db.execute(
        select(Wallet)
        .filter(Wallet.user_id == user_id)
        .offset(skip)
        .limit(limit)
        .order_by(Wallet.created_at.desc()) # Order by creation time
    )
    return result.scalars().all()

# Example function to update wallet record approval status and transaction time (Async)
async def update_wallet_record_approval(
    db: AsyncSession, transaction_id: str, is_approved: int
) -> Wallet | None:
    """
    Updates the approval status of a wallet transaction record and sets transaction_time if approved.
    """
    result = await db.execute(
        select(Wallet).filter(Wallet.transaction_id == transaction_id)
    )
    wallet_record = result.scalars().first()

    if wallet_record:
        wallet_record.is_approved = is_approved
        # Set transaction_time only if it's being approved (is_approved == 1)
        if is_approved == 1 and wallet_record.transaction_time is None:
            wallet_record.transaction_time = datetime.datetime.now()

        await db.commit()
        await db.refresh(wallet_record)
        logger.info(f"Wallet record {transaction_id} approval status updated to {is_approved}.")
        return wallet_record
    else:
        logger.warning(f"Wallet record with transaction ID {transaction_id} not found for update.")
        return None

# The __main__ block for testing needs to be updated for async as well
# import asyncio
# if __name__ == "__main__":
#     # This section is for testing the function.  It will only run
#     # when this file is executed directly (e.g., python crud/wallet.py).
#     # It assumes you have an async database session setup.  You'll need to
#     # adapt this to your actual async database setup.

#     # Example Usage (replace with your actual async database setup and test data)
#     # from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
#     # from sqlalchemy.orm import sessionmaker
#     # from app.database.models import Base # Import Base
#     # from app.core.config import get_settings # Import get_settings

#     # settings = get_settings()
#     # DATABASE_URL = settings.ASYNC_DATABASE_URL
#     # engine = create_async_engine(DATABASE_URL, echo=True)
#     # AsyncSessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

#     # async def get_test_db():
#     #     async with AsyncSessionLocal() as session:
#     #         yield session

#     # async def test_create_wallet():
#     #     # Ensure tables exist for testing
#     #     async with engine.begin() as conn:
#     #         await conn.run_sync(Base.metadata.create_all)

#     #     # Get a test session
#     #     db_gen = get_test_db()
#     #     db = await anext(db_gen) # Use anext for async generator

#     #     try:
#     #         # Example user ID (replace with a valid user ID from your database)
#     #         test_user_id = 1 # Replace with actual user ID

#     #         # Example wallet data using the schema
#     #         test_wallet_data = WalletCreate(
#     #             user_id=test_user_id,
#     #             transaction_type="deposit",
#     #             transaction_amount=Decimal("500.00"),
#     #             description="Initial deposit via test script" # Include description
#     #             # Other optional fields will be None
#     #         )

#     #         # Create a wallet record
#     #         new_wallet_record = await create_wallet_record(
#     #             db=db,
#     #             wallet_data=test_wallet_data,
#     #         )

#     #         if new_wallet_record:
#     #             print("Wallet record created successfully:")
#     #             # Print details using the model attributes
#     #             print(f"ID: {new_wallet_record.id}")
#     #             print(f"Transaction ID: {new_wallet_record.transaction_id}")
#     #             print(f"User ID: {new_wallet_record.user_id}")
#     #             print(f"Transaction Type: {new_wallet_record.transaction_type}")
#     #             print(f"Transaction Amount: {new_wallet_record.transaction_amount}")
#     #             print(f"Description: {new_wallet_record.description}") # Print description
#     #             print(f"Created At: {new_wallet_record.created_at}")
#     #         else:
#     #             print("Failed to create wallet record.")

#     #     except Exception as e:
#     #         print(f"Error in test_create_wallet: {e}")
#     #     finally:
#     #         # Close the database session (handled by async with in get_test_db)
#     #         pass # No explicit close needed here if using async with

#     # # Run the async test function
#     # asyncio.run(test_create_wallet())

