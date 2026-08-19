import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the FastAPI application
from app.main import app

# Vercel entry point - must be named 'app' or 'handler'
app = app
handler = app
application = app

# For Vercel serverless function compatibility
def handler(event, context):
    """AWS Lambda-style handler for Vercel compatibility."""
    from mangum import Mangum
    asgi_handler = Mangum(app)
    return asgi_handler(event, context)