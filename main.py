import asyncio
from app.dependencies.redis_client import get_redis_client, global_redis_client_instance
from app.services.portfolio_background_worker import portfolio_background_worker

# Start the portfolio background worker
@app.on_event("startup")
async def start_portfolio_background_worker():
    """
    Start the background worker that continuously calculates portfolio metrics for active users.
    This runs independently of WebSocket connections and is essential for features like autocut.
    """
    # Make sure Redis client is initialized
    if global_redis_client_instance is None:
        from app.core.security import connect_to_redis
        redis_client = await connect_to_redis()
    else:
        redis_client = global_redis_client_instance
        
    if redis_client:
        # Start the background worker as a task
        asyncio.create_task(portfolio_background_worker(redis_client))
        logger.info("Portfolio background worker started")
    else:
        logger.error("Failed to start portfolio background worker: Redis client not available") 