# QA Portal Server - AI Coding Agent Instructions

## Project Overview
This is a Flask-based Oracle database monitoring and visualization portal that provides real-time statistics (RTS) analysis, chart generation, and database performance monitoring. The application connects to both "target" (monitored) and "repo" (repository) Oracle databases.

## Architecture Patterns

### Database Configuration System
- **Two-tier config**: `db_setup.json` → `db_config.json` via `/rts/db/setup` API
- **Shared connection management**: All DB operations use `app/shared_db.py` for connection pooling
- **Target vs Repo pattern**: "target" = monitored database, "repo" = statistics repository
- Critical: Always check `is_target_db_configured()` and `is_repo_db_configured()` before DB operations

### Blueprint Architecture
- **Mass blueprint registration**: All blueprints listed in `app/routes/__init__.py` → auto-registered in `app/__init__.py`
- **URL prefix pattern**: `/rts/*` for RTS APIs, specific prefixes per module (e.g., `/rts/repo/logging`)
- **Swagger integration**: All routes use `@swag_from()` with detailed API documentation

### Service Layer Pattern
- **Minimal services**: Currently only `ssh_utils.py` - business logic is primarily in routes
- **Direct DB access**: Routes directly call shared_db functions rather than service abstractions
- **SSH operations**: Remote file operations via `SSHUtils` class for Oracle configuration deployment

## Database Integration Patterns

### Connection Management
```python
# Standard pattern for all routes
from app.shared_db import get_connection, is_target_db_configured

def _connect_target():
    if not is_target_db_configured():
        raise Exception("Target DB 설정이 없습니다. 먼저 /rts/db/setup API를 호출하세요.")
    return get_connection('target')
```

### Oracle-Specific Patterns
- **Service name connections**: Use `oracledb.makedsn()` with `service_name` parameter
- **V$ queries**: Direct system view queries for real-time statistics (`v$sysstat`, `v$session_wait`)
- **Partition handling**: Time-based partitioning with `YYYYMMDD` format (e.g., `_250905` suffix)

## Key Development Workflows

### Starting the Application
```bash
python app.py
# Swagger UI automatically available at: http://localhost:5000/apidocs
```

### Database Setup Sequence
1. Create `db_setup.json` with target/repo DB credentials
2. Call `POST /rts/db/setup` to activate configuration
3. Configuration persists in `db_config.json` for subsequent runs

### Testing Patterns
- **API testing**: Use `test_api.py` with requests library for endpoint validation
- **Browser automation**: `browser_test.py` and `test_chrome_browser.py` for web UI testing
- **Connection testing**: `test_connection.py` for database connectivity validation

## Chart and Visualization System

### Chart Generation Flow
- **Data collection**: Oracle queries → JSON format → Plotly charts
- **Export patterns**: HTML + PNG + JSON data exports to `chart_exports/` directory
- **Real-time viewer**: `realtime_chart_viewer.html` with auto-refresh capabilities
- **Chart route**: `/rts/chart/*` endpoints handle data visualization

### File Naming Convention
```
chart_{metric}_{timestamp}.{html|png|json}
# Example: chart_session_logical_reads_DB_time_20250905_141904.html
```

## Critical Integration Points

### SSH File Operations
- **Remote config deployment**: Routes use `SSHUtils` to deploy `sec_logging.json` to Oracle servers
- **Path handling**: Support both Windows (`C:\path`) and Unix (`/path`) formats
- **Local fallback**: `local_only=True` option bypasses SSH for local-only operations

### Data Collection Monitoring
- **Missing data detection**: Specialized routes for identifying gaps in time-series data
- **Partition-aware queries**: All queries consider Oracle partition naming conventions
- **Hourly reporting**: Automated monitoring with file-based report generation in `data_collection_reports/`

## Error Handling Patterns
- **DB connection errors**: Always use `record_connection_error()` for tracking
- **Configuration validation**: Check both target and repo DB configuration before operations  
- **Korean language support**: Error messages and logs use Korean text with UTF-8 encoding

## File Organization Anti-Patterns
- **Route proliferation**: Many similar route files (avoid creating `*_backup.py`, `*_clean.py` variants)
- **Configuration drift**: Multiple JSON config formats exist - always use `db_config.json` as source of truth
- **Test file sprawl**: Consolidate similar test functionality rather than creating numerous `test_*.py` files

## Development Guidelines

### Adding New Routes
1. Create blueprint with appropriate URL prefix in `app/routes/`
2. Add import and blueprint to `app/routes/__init__.py` blueprints list
3. Use `@swag_from()` for Swagger documentation
4. Follow `_connect_target()` / `_connect_repo()` helper pattern

### Database Operations
- Always use `app/shared_db.py` functions - never create direct oracledb connections
- Check configuration status before attempting connections
- Use connection pooling via `get_connection()` / `release_connection()`

### Chart/Visualization Features
- Ensure matplotlib backend set to 'Agg' for headless operation
- Export data in multiple formats (HTML, PNG, JSON) for flexibility
- Use Plotly for interactive charts, matplotlib for static exports