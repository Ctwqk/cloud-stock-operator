# Resume Project Description

**Cloud-Based Intelligent Stock Trading and Monitoring System**

• **Architecture Design**: Built serverless architecture using AWS CDK (Python) with DynamoDB, S3, SNS/SQS, EventBridge, and API Gateway; implemented DAG-based workflow orchestration for data pipeline dependencies from news ingestion to trade execution.

• **Core Functionality**: Developed news fetching and sentiment analysis modules, integrating FinBERT via MCP (Model Context Protocol) for real-time sentiment scoring; implemented threshold-based automated trading system that generates and executes buy/sell signals.

• **Data Processing**: Designed portfolio management features including cash management, position tracking, and net worth visualization; utilized Lambda functions with DAG patterns for scheduled data processing and cleanup.

• **System Integration**: Built local trading worker communicating via SQS queues; developed RESTful management API with MCP-enabled model integration for watchlist management and system control operations.

