# app/main.py

# Import necessary components from fastapi
from fastapi import FastAPI, Depends, HTTPException, status, Query
from fastapi.staticfiles import StaticFiles
from sqlalchemy.ext.asyncio import AsyncSession
import asyncio
import os
import json
from typing import Optional, Any
from dotenv import load_dotenv

import logging

# --- APScheduler Imports ---
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# --- Custom Service and DB Session for Scheduler ---
from app.services.swap_service import apply_daily_swap_charges_for_all_open_orders
from app.database.session import AsyncSessionLocal

# --- CORS Middleware Import ---
from fastapi.middleware.cors import CORSMiddleware

# Configure basic logging early
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.getLogger('app.services.portfolio_calculator').setLevel(logging.DEBUG)
logging.getLogger('app.services.portfolio_background_worker').setLevel(logging.DEBUG)
logging.getLogger('app.services.swap_service').setLevel(logging.DEBUG)

# Configure file logging for specific modules to logs/orders.log
log_file_path = os.path.join(os.path.dirname(__file__), '..', 'logs', 'orders.log')
os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Orders endpoint logger to file
orders_ep_logger = logging.getLogger('app.api.v1.endpoints.orders')
orders_ep_logger.setLevel(logging.DEBUG)
orders_fh = logging.FileHandler(log_file_path)
orders_fh.setFormatter(file_formatter)
orders_ep_logger.addHandler(orders_fh)
orders_ep_logger.propagate = False # Prevent console output from basicConfig

# Order processing service logger to file
order_proc_logger = logging.getLogger('app.services.order_processing')
order_proc_logger.setLevel(logging.DEBUG)
order_proc_fh = logging.FileHandler(log_file_path) # Use the same file handler or a new one if separate formatting is needed
order_proc_fh.setFormatter(file_formatter)
order_proc_logger.addHandler(order_proc_fh)
order_proc_logger.propagate = False # Prevent console output from basicConfig

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Import Firebase Admin SDK components
import firebase_admin
from firebase_admin import credentials, db as firebase_db

# Import configuration settings
from app.core.config import get_settings

# Import database session dependency and table creation function
from app.database.session import get_db, create_all_tables

# Import API router
from app.api.v1.api import api_router

# Import background tasks
from app.firebase_stream import process_firebase_events
from app.api.v1.endpoints.market_data_ws import redis_publisher_task # Keep publisher
from app.services.portfolio_background_worker import start_portfolio_worker, stop_portfolio_worker

# Import Redis dependency and global instance
from app.dependencies.redis_client import get_redis_client, global_redis_client_instance
from app.core.security import close_redis_connection, create_service_account_token

# Import shared state (for the queue)
from app.shared_state import redis_publish_queue

# Import orders logger
from app.core.logging_config import orders_logger

settings = get_settings()
app = FastAPI(
    title=settings.PROJECT_NAME,
    openapi_url=f"{settings.API_V1_STR}/openapi.json"
)

# --- CORS Settings (Allow All Origins) ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins
    allow_credentials=False, # Must be False if allow_origins is ["*"]
    allow_methods=["*"],  # Allows all HTTP methods
    allow_headers=["*"],  # Allows all headers
)
# --- End CORS Settings ---

scheduler: Optional[AsyncIOScheduler] = None

load_dotenv() 

# Now, you can safely print and access them
SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = os.getenv("ALGORITHM")

print(f"--- Application Startup ---")
print(f"Loaded SECRET_KEY (from code): '{SECRET_KEY}'")
print(f"Loaded ALGORITHM (from code): '{ALGORITHM}'")
print(f"---------------------------")

# Log application startup
orders_logger.info("Application starting up - Orders logging initialized")

# --- Scheduled Job Functions ---
async def daily_swap_charge_job():
    logger.info("APScheduler: Executing daily_swap_charge_job...")
    async with AsyncSessionLocal() as db:
        if global_redis_client_instance:
            try:
                await apply_daily_swap_charges_for_all_open_orders(db, global_redis_client_instance)
                logger.info("APScheduler: Daily swap charge job completed successfully.")
            except Exception as e:
                logger.error(f"APScheduler: Error during daily_swap_charge_job: {e}", exc_info=True)
        else:
            logger.error("APScheduler: Cannot execute daily_swap_charge_job - Global Redis client not available.")

# --- New JWT Rotation Job ---
async def rotate_service_account_jwt():
    try:
        token = create_service_account_token("python_bridge", expires_minutes=30)
        jwt_ref = firebase_db.reference("trade_data/service_auth_token")
        jwt_ref.set({"token": token})
        logger.info("Service account JWT pushed to Firebase.")
        logger.info(f"Generated service account JWT: {token}")
    except Exception as e:
        logger.error(f"Error generating or pushing service JWT to Firebase: {e}", exc_info=True)

@app.on_event("startup")
async def startup_event():
    global scheduler, global_redis_client_instance
    logger.info("Application startup event triggered.")

    firebase_app_instance = None
    try:
        cred_path = settings.FIREBASE_SERVICE_ACCOUNT_KEY_PATH
        if not os.path.exists(cred_path):
            logger.critical(f"Firebase service account key file not found at: {cred_path}")
        else:
            cred = credentials.Certificate(cred_path)
            if not firebase_admin._apps:
                firebase_app_instance = firebase_admin.initialize_app(cred, {
                    'databaseURL': settings.FIREBASE_DATABASE_URL
                })
                logger.info("Firebase Admin SDK initialized successfully.")
            else:
                firebase_app_instance = firebase_admin.get_app()
                logger.info("Firebase Admin SDK already initialized.")
    except Exception as e:
        logger.critical(f"Failed to initialize Firebase Admin SDK: {e}", exc_info=True)

    try:
        global_redis_client_instance = await get_redis_client()
        if global_redis_client_instance:
            logger.info("Redis client initialized successfully.")
        else:
            logger.critical("Redis client failed to initialize.")
    except Exception as e:
        logger.critical(f"Failed to connect to Redis during startup: {e}", exc_info=True)
        global_redis_client_instance = None

    try:
        await create_all_tables()
        logger.info("Database tables ensured/created.")
    except Exception as e:
        logger.critical(f"Failed to create database tables: {e}", exc_info=True)

    # Initialize and Start APScheduler
    if global_redis_client_instance:
        scheduler = AsyncIOScheduler(timezone="UTC")
        scheduler.add_job(
            daily_swap_charge_job,
            trigger=CronTrigger(hour=0, minute=0, second=0, timezone="UTC"),
            id="daily_swap_task",
            name="Daily Swap Charges",
            replace_existing=True
        )
        scheduler.add_job(
            rotate_service_account_jwt,
            trigger=CronTrigger(minute="*/30", timezone="UTC"),
            id="rotate_service_jwt",
            name="Rotate service account JWT for python_bridge",
            replace_existing=True
        )
        try:
            scheduler.start()
            logger.info("APScheduler started with jobs for daily swap and JWT rotation.")
            await rotate_service_account_jwt() # Run once on startup
        except Exception as e:
            logger.error(f"Failed to start APScheduler: {e}", exc_info=True)
            scheduler = None
    else:
        logger.warning("Scheduler not started: Redis client unavailable.")

    if global_redis_client_instance and firebase_app_instance:
        # Start Firebase stream processing task
        firebase_task = asyncio.create_task(process_firebase_events(firebase_db, path=settings.FIREBASE_DATA_PATH))
        firebase_task.set_name("firebase_listener")
        background_tasks.add(firebase_task)
        firebase_task.add_done_callback(background_tasks.discard)
        logger.info("Firebase stream processing task scheduled.")

        # Start Redis publisher task
        publisher_task = asyncio.create_task(redis_publisher_task(global_redis_client_instance))
        publisher_task.set_name("redis_publisher_task")
        background_tasks.add(publisher_task)
        publisher_task.add_done_callback(background_tasks.discard)
        logger.info("Redis publisher task scheduled.")
        
        # Start portfolio background worker
        try:
            await start_portfolio_worker(global_redis_client_instance)
            logger.info("Portfolio background worker started successfully.")
        except Exception as e:
            logger.error(f"Failed to start portfolio background worker: {e}", exc_info=True)
    else:
        missing_services = []
        if not global_redis_client_instance:
            missing_services.append("Redis client")
        if not firebase_app_instance:
            missing_services.append("Firebase app instance")
        logger.warning(f"Other background tasks not started due to missing: {', '.join(missing_services)}.")

    logger.info("Application startup event finished.")

    if global_redis_client_instance:
        try:
            pong = await global_redis_client_instance.ping()
            logger.info(f"Redis ping response: {pong}")
        except Exception as e:
            logger.critical(f"Redis ping failed post-init: {e}", exc_info=True)

background_tasks = set()

@app.on_event("shutdown")
async def shutdown_event():
    global scheduler, global_redis_client_instance
    logger.info("Application shutdown event triggered.")

    # Stop portfolio background worker
    try:
        await stop_portfolio_worker()
        logger.info("Portfolio background worker stopped.")
    except Exception as e:
        logger.error(f"Error stopping portfolio background worker: {e}", exc_info=True)

    if scheduler and scheduler.running:
        try:
            scheduler.shutdown()
            logger.info("APScheduler shutdown complete.")
        except Exception as e:
            logger.error(f"Error shutting down scheduler: {e}", exc_info=True)
    else:
        logger.info("APScheduler not running, no shutdown needed.")

    # Cancel all background tasks
    for task in background_tasks:
        if not task.done():
            logger.info(f"Cancelling background task: {task.get_name()}")
            task.cancel()

    # Close Redis connection
    if global_redis_client_instance:
        try:
            await close_redis_connection(global_redis_client_instance)
            logger.info("Redis connection closed.")
        except Exception as e:
            logger.error(f"Error closing Redis connection: {e}", exc_info=True)

    # Clean up Firebase resources
    from app.firebase_stream import cleanup_firebase
    try:
        cleanup_firebase()
        logger.info("Firebase resources cleaned up.")
    except Exception as e:
        logger.error(f"Error cleaning up Firebase resources: {e}", exc_info=True)

    logger.info("Application shutdown complete.")

# Include API router
app.include_router(api_router, prefix=settings.API_V1_STR)

# Mount static files (if needed)
# app.mount("/static", StaticFiles(directory="static"), name="static")

# Root endpoint
@app.get("/")
async def read_root():
    return {"message": "Welcome to the Trading Platform API"}

# Add an admin endpoint to rebuild the Redis cache
@app.post("/api/v1/admin/rebuild-cache")
async def rebuild_cache(db: AsyncSession = Depends(get_db)):
    """
    Admin endpoint to rebuild the Redis cache from the database.
    This is useful after a Redis flush or server restart.
    """
    from app.services.portfolio_background_worker import initialize_user_cache
    from app.crud import user as crud_user
    
    try:
        # Get all users from the database
        live_users = await crud_user.get_all_users(db)
        demo_users = await crud_user.get_all_demo_users(db)
        
        # Initialize cache for each user
        initialized_count = 0
        failed_count = 0
        
        for user in live_users:
            success = await initialize_user_cache(
                user_id=user.id,
                user_type="live",
                redis_client=global_redis_client_instance,
                db_session=db
            )
            if success:
                initialized_count += 1
            else:
                failed_count += 1
                
        for user in demo_users:
            success = await initialize_user_cache(
                user_id=user.id,
                user_type="demo",
                redis_client=global_redis_client_instance,
                db_session=db
            )
            if success:
                initialized_count += 1
            else:
                failed_count += 1
                
        return {
            "status": "success",
            "message": f"Cache rebuild complete. Initialized {initialized_count} users, failed {failed_count} users."
        }
    except Exception as e:
        logger.error(f"Error rebuilding cache: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error rebuilding cache: {str(e)}"
        )