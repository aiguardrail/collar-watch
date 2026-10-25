import os
from dotenv import load_dotenv

load_dotenv()

ROBINHOOD_WSS = os.getenv("ROBINHOOD_WSS", "wss://feed.mainnet.chain.robinhood.com")
ROBINHOOD_RPC = os.getenv("ROBINHOOD_RPC", "https://rpc.mainnet.chain.robinhood.com")
POOL_MANAGER = os.getenv("POOL_MANAGER", "").strip().lower()
COLLAR_MCP_URL = os.getenv("COLLAR_MCP_URL", "https://api.collarguardrail.com/mcp-http/mcp")
COLLAR_API_KEY = os.getenv("COLLAR_API_KEY", "")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
