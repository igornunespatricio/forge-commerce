#!/usr/bin/env python3
"""
Simple startup script for the E-Commerce Data Generator API.

Provides an easy way to start the API server with common configurations.
"""

import os
import sys
import argparse
import logging
import uvicorn

# Add the current directory to the Python path to enable imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app import run_server
from config import config


def setup_logging():
    """Setup logging configuration."""
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL),
        format=config.LOG_FORMAT,
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('api.log')
        ]
    )


def main():
    """Main entry point for the API server."""
    parser = argparse.ArgumentParser(description="E-Commerce Data Generator API Server")
    parser.add_argument("--host", default=config.HOST, help="Host to bind to (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=config.PORT, help="Port to bind to (default: 8000)")
    parser.add_argument("--reload", action="store_true", default=config.RELOAD, help="Enable auto-reload (development)")
    parser.add_argument("--no-reload", action="store_false", dest="reload", help="Disable auto-reload (production)")
    parser.add_argument("--log-level", default=config.LOG_LEVEL, help="Log level (default: INFO)")
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging()
    
    # Log startup information
    logger = logging.getLogger(__name__)
    logger.info(f"Starting E-Commerce Data Generator API")
    logger.info(f"Host: {args.host}")
    logger.info(f"Port: {args.port}")
    logger.info(f"Reload: {args.reload}")
    logger.info(f"Log Level: {args.log_level}")
    logger.info(f"Environment: {os.getenv('ENVIRONMENT', 'development')}")
    
    print(f"🚀 Starting E-Commerce Data Generator API")
    print(f"📍 Server: {args.host}:{args.port}")
    print(f"🔄 Auto-reload: {args.reload}")
    print(f"📝 Log level: {args.log_level}")
    print(f"📚 API docs: http://{args.host}:{args.port}/docs")
    print(f"💚 Health check: http://{args.host}:{args.port}/api/health")
    print()
    print("Press Ctrl+C to stop the server")
    print()
    
    # Start the server
    try:
        uvicorn.run(
            "app:app",
            host=args.host,
            port=args.port,
            reload=args.reload,
            log_level=args.log_level.lower()
        )
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
        print("\n👋 Server stopped")
    except Exception as e:
        logger.error(f"Server error: {e}", exc_info=True)
        print(f"❌ Server error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()