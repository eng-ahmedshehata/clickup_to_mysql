# ClickUp to MySQL Sync

A high-performance, enterprise-grade Python script that synchronizes ClickUp workspace data to a MySQL database with advanced features including intelligent rate limiting, circuit breaker patterns, enhanced progress tracking, and comprehensive error handling.

![Python](https://img.shields.io/badge/python-v3.8+-blue.svg)
![MySQL](https://img.shields.io/badge/mysql-v5.7+-orange.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)
![Status](https://img.shields.io/badge/status-production--ready-brightgreen.svg)

##  Features

### Core Functionality
- **Complete Data Synchronization**: Syncs spaces, folders, lists, tasks, subtasks, relations, sprints, and task status history
- **Incremental & Full Sync**: Support for both full data refresh (REFETCH) and incremental updates (ACCUMULATE)
- **Real-time Progress Tracking**: Beautiful visual progress bars with ETA calculations and performance metrics
- **Bulk Operations**: Optimized bulk inserts with intelligent fallback to individual operations

### Performance & Reliability
- **Adaptive Rate Limiting**: Intelligent API rate limiting with health scoring and predictive adjustment
- **Circuit Breaker Pattern**: Automatic API failure detection and recovery with exponential backoff
- **Database Optimization**: Comprehensive indexing strategy for optimal query performance
- **Connection Pooling**: Advanced database connection management with auto-recovery
- **Memory Management**: Built-in memory monitoring and garbage collection

### Enterprise Features
- **Configuration Validation**: Comprehensive startup validation with connectivity testing
- **Structured Logging**: Detailed logging with performance metrics and error tracking
- **Error Recovery**: Robust error handling with retry logic and graceful degradation
- **Data Integrity**: Advanced data validation and sanitization
- **Metrics Collection**: Comprehensive performance and operational metrics

##  Prerequisites

- Python 3.8 or higher
- MySQL 5.7 or higher
- ClickUp API access token
- Required Python packages (see Installation)

##  Installation

1. **Clone the repository**
   `ash
   git clone https://github.com/eng-ahmedshehata/clickup_to_mysql.git
   cd clickup_to_mysql
   `

2. **Create a virtual environment**
   `ash
   python -m venv venv
   # On Windows:
   venv\Scripts\activate
   # On macOS/Linux:
   source venv/bin/activate
   `

3. **Install dependencies**
   `ash
   pip install -r requirements.txt
   `

##  Configuration

### Environment Variables

Create a .env file in the project root with the following variables:

`env
# ClickUp API Configuration
CLICKUP_API_KEY=your_clickup_api_key_here
TEAM_ID=your_team_id
SPACE_ID=your_space_id

# Database Configuration
DB_CONN_STRING=mysql+pymysql://username:password@localhost:3306/database_name

# Optional Configuration
DRY_RUN=false  # Set to true for testing without database changes
`

### Getting ClickUp Credentials

1. **API Key**: 
   - Go to ClickUp Settings  Apps  API
   - Generate a new API token
   
2. **Team ID**: 
   - Available in your ClickUp workspace URL or via API
   
3. **Space ID**: 
   - Found in the ClickUp space settings or URL

##  Usage

### Basic Usage

`ash
python clickup_to_mysql_single_space.py
`

The script will prompt you to choose:
- R - REFETCH: Complete data refresh (clears existing data)
- A - ACCUMULATE: Incremental sync (only new/updated data)

### Advanced Usage

`ash
# Dry run mode (test without making changes)
DRY_RUN=true python clickup_to_mysql_single_space.py

# With custom configuration
CLICKUP_API_KEY=your_key SPACE_ID=123456 python clickup_to_mysql_single_space.py
`

##  Database Schema

The script creates the following tables:

### Core Tables
- **spaces**: ClickUp space information
- **folders**: Folder organization
- **lists**: Task lists within folders
- **tasks**: Main task data with comprehensive fields
- **subtasks**: Task subtasks and dependencies

### Relationship Tables
- **relations**: Task relationships and dependencies
- **sprints**: Sprint assignments and tracking
- **task_time_in_status**: Historical status tracking with time analysis

##  Performance Features

### Intelligent Rate Limiting
- Base delay: 0.5 seconds, maximum: 12 seconds
- Health-based adjustment with API scoring
- Predictive backoff based on response times
- Circuit breaker activation for severe rate limiting

### Database Optimization
- Connection pooling (20 base connections, 40 overflow)
- Bulk insert operations with fallback strategies
- Comprehensive indexing on foreign keys and common queries
- Auto-commit mode for optimal performance

### Progress Tracking
`
 Processing Lists Progress Report 
 [14:30:15] Progress: [] 80.0%
 Items: 88/110 | Speed: 2.1/min
 Elapsed: 42m 15s | ETA: 10m 30s
 Success Rate: 99.2% | Errors: 0
 API Health: 100% | Delay: 0.5s
 Memory: 245MB

`

##  Troubleshooting

### Common Issues

**API Authentication Errors**
`
 Invalid CLICKUP_API_KEY - authentication failed
`
- Verify your API key is correct and has proper permissions
- Check that the token hasn't expired

**Database Connection Issues**
`
 Database connectivity test failed
`
- Verify database credentials and connection string
- Ensure database server is accessible
- Check firewall and network settings

**Rate Limiting**
`
 Rate limit detected - API health: 60%
`
- The script automatically handles rate limiting
- Consider reducing parallel workers if persistent

##  Contributing

1. Fork the repository
2. Create a feature branch (git checkout -b feature/amazing-feature)
3. Commit your changes (git commit -m 'Add amazing feature')
4. Push to the branch (git push origin feature/amazing-feature)
5. Open a Pull Request

##  License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

##  Support

- **Issues**: [GitHub Issues](https://github.com/eng-ahmedshehata/clickup_to_mysql/issues)
- **Documentation**: [Wiki](https://github.com/eng-ahmedshehata/clickup_to_mysql/wiki)

##  Star History

If this project helped you, please consider giving it a star! 

---

**Made with  for the ClickUp community**

*This tool is not officially affiliated with ClickUp. ClickUp is a trademark of ClickUp Inc.*
