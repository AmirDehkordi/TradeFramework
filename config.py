import logging

# Basic logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[logging.StreamHandler()]
)

IB_HOST = '127.0.0.1'
IB_PORT = 4002
IB_CLIENT_ID = 45
