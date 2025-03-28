# PDF Downloader Tests

This repository contains a PDF Downloader application along with comprehensive test suites to verify its functionality.

## Project Overview

The PDF Downloader is a Python application that:

1. Downloads PDF files from URLs listed in an Excel file
2. Tracks download status in metadata 
3. Creates detailed reports of download results
4. Uploads downloaded PDFs to Google Drive

## Test Suite Structure

The project includes two types of tests:

1. **Unit Tests** (`PDF_Downloader_Tests.py`): Tests individual components with mocked dependencies
2. **Integration Tests** (`PDF_Downloader_IntegrationTests.py`): Tests the application with real files and actual network calls

## Prerequisites

Before running the tests, ensure you have the following installed:

- Python 3.6+
- Required Python packages:
  - pandas
  - requests
  - pydrive2
  - coverage
  - unittest (included in Python standard library)

Install dependencies using:

```bash
pip install pandas requests pydrive2 coverage
```

## Directory Structure

The project should be structured as follows:

```
SPAC-Uge-5-PDF-Downloader-Tests/
│
├── PDF_Downloader.py       # Main application
├── client_secrets.json     # Google API credentials (for Google Drive tests)
│
├── Data/                   # Data directory
│   ├── Downloads/          # Downloaded PDFs go here
│   ├── Output/             # Output reports go here
│   └── ...                 
│
└── tests/                  # Test directory
    ├── PDF_Downloader_Tests.py           # Unit tests
    ├── PDF_Downloader_IntegrationTests.py # Integration tests
    └── ...
```

## Running Unit Tests

The unit tests use mocking to avoid external dependencies, making them fast and reliable.

### Basic Usage

```bash
python -m tests.PDF_Downloader_Tests
```

This will:
- Run all unit tests
- Generate a code coverage report in the terminal
- Create an HTML coverage report in the `tests/coverage_html` directory

### Options

- Run specific test: `python -m unittest tests.PDF_Downloader_Tests.TestPDFDownloader.test_init`
- Run with more details: `python -m unittest -v tests.PDF_Downloader_Tests`

## Running Integration Tests

Integration tests work with real files and make actual network requests. They take longer to run but verify the application works in real-world scenarios.

### Basic Usage

```bash
python -m tests.PDF_Downloader_IntegrationTests
```

### Notes for Integration Tests

- These tests create a test environment in `tests/integration_test_data`
- They download actual PDF files from reliable public URLs
- Tests may take longer depending on network speed
- Some tests require a working internet connection

## Google Drive Tests

Some tests involve Google Drive functionality. To run these successfully:

1. Create a project in Google Cloud Console
2. Enable the Google Drive API
3. Create OAuth credentials
4. Download the credentials as `client_secrets.json` and place it in the project root directory

For unit tests, Google Drive operations are mocked, so these steps are only necessary if you want to test actual uploads to Google Drive.

## Understanding Test Results

Test results will show:

- Number of tests run
- Number of failures or errors
- For unit tests, code coverage statistics showing which lines of code were executed

Example output:
```
Ran 25 tests in 12.345s

OK

Coverage Report:
Name                    Stmts   Miss  Cover
-------------------------------------------
PDF_Downloader.py         384     42    89%
-------------------------------------------
TOTAL                     384     42    89%
```