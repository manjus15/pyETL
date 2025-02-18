# pyETL - Data Pipeline Framework

A robust ETL (Extract, Transform, Load) framework for processing and analyzing data from various sources.

## Features

- Modular ETL pipeline architecture
- Configurable data extraction from multiple sources
- Data transformation with validation and optimization
- Flexible data loading capabilities
- Environment-based configuration management
- Comprehensive logging and error handling
- Data quality checks and profiling
- Resource monitoring

## Installation

1. Clone the repository:
```bash
git clone <your-repo-url>
cd pyETL
```

2. Create and activate virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your configuration
```

## Configuration

The project uses a hierarchical configuration system:

1. Environment variables (highest priority)
2. `.env` file
3. Default values in `config/config.py`

### Database Configuration

Configure database connection in `.env`:
```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_database
DB_USER=your_user
DB_PASSWORD=your_password
DB_SCHEMA=public
```

### Environment Configuration

- `ENV`: Set to 'dev', 'test', or 'prod'
- `LOG_LEVEL`: Logging level (INFO, DEBUG, etc.)
- `BATCH_SIZE`: Number of records to process in batch
- `MAX_WORKERS`: Maximum number of concurrent workers

## Usage

Run the ETL pipeline:
```bash
python main.py
```

### Development

1. Install development dependencies:
```bash
pip install -r requirements-dev.txt
```

2. Run tests:
```bash
pytest
```

3. Run linting:
```bash
flake8 .
black .
```

## Project Structure

```
pyETL/
├── config/             # Configuration files
├── src/               # Source code
│   ├── extract/       # Data extraction modules
│   ├── transform/     # Data transformation modules
│   ├── load/          # Data loading modules
│   └── database/      # Database connection handling
├── tests/             # Test files
├── utils/             # Utility functions
├── .env               # Environment variables
├── requirements.txt   # Project dependencies
└── README.md         # Project documentation
```

## Testing

Run tests with coverage:
```bash
pytest --cov=src tests/
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

[Your License] - See LICENSE file for details 