"""Message type names (no dependencies, so the CDK app can import them without boto3)."""

# external operations (user intent, ExternalOpsTopic)
ADD_STOCK = "ADD_STOCK"
ADJUST_MANAGED_CASH = "ADJUST_MANAGED_CASH"
ADJUST_MANAGED_SHARES = "ADJUST_MANAGED_SHARES"
# system events (SystemOpsTopic)
STOCK_ADDED = "STOCK_ADDED"
NEW_NEWS = "NEW_NEWS"
NEWS_STORED = "NEWS_STORED"
AUTO_TRADE_DECISION = "AUTO_TRADE_DECISION"
MARK_STOCK_DELETED = "MARK_STOCK_DELETED"
CLEANUP_STOCK = "CLEANUP_STOCK"
