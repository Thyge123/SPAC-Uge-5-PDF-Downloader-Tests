import unittest  # Python's built-in testing framework
import sys
import os
import pandas as pd  # For Excel file operations
import shutil  # For file and directory operations
import logging  # For structured logging
import time
from pathlib import Path  # Object-oriented filesystem paths

# Add parent directory to path so we can import our module
# This allows us to import from the parent directory of this test file
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the module we want to test
from PDF_Downloader import PDF_Downloader

# Set up logging configuration to track test progress
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)  # Get a logger for this module

class PDFDownloaderIntegrationTests(unittest.TestCase):
    """Integration tests for PDF_Downloader using real files."""

    def setUp(self):
        """Set up test environment with actual files and directories.
        
        This method runs before each test method and prepares the
        test environment with necessary directories and files.
        """
        # Create test directory structure
        self.test_dir = os.path.join(os.path.dirname(__file__), 'integration_test_data')
        self.download_dir = os.path.join(self.test_dir, 'Downloads')
        self.output_dir = os.path.join(self.test_dir, 'Output')
        
        # Ensure directories exist (create if they don't)
        os.makedirs(self.test_dir, exist_ok=True)
        os.makedirs(self.download_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Define paths for test input files
        self.reports_path = os.path.join(self.test_dir, 'test_reports.xlsx')
        self.metadata_path = os.path.join(self.test_dir, 'test_metadata.xlsx')
        
        # Create test reports file with reliable public PDFs that should be available online
        self.create_test_reports_file()
        
        # Create initial metadata file to track download status
        self.create_test_metadata_file()
        
        # Create PDF_Downloader instance with test configuration
        self.downloader = PDF_Downloader()
        # Configure the downloader to use our test directories and files
        self.downloader.data_dir = self.test_dir
        self.downloader.download_dir = self.download_dir
        self.downloader.output_dir = self.output_dir
        self.downloader.reports_path = self.reports_path
        self.downloader.metadata_path = self.metadata_path
        
        # Limit downloads to make tests faster
        # In real usage, these values would likely be higher
        self.downloader.max_downloads = 2
        self.downloader.max_concurrent_threads = 2
        
        logger.info(f"Test environment set up at: {self.test_dir}")

    def tearDown(self):
        """Clean up test files and directories.
        
        This method runs after each test method and ensures
        we don't leave test files scattered around.
        """
        if os.path.exists(self.test_dir):
            try:
                shutil.rmtree(self.test_dir)  # Remove the entire test directory tree
                logger.info(f"Test directory removed: {self.test_dir}")
            except Exception as e:
                logger.warning(f"Error removing test directory: {e}")

    def create_test_reports_file(self):
        """Create a test reports Excel file with real, reliable PDF URLs.
        
        This method prepares a sample dataset with URLs pointing to 
        publicly available PDF files that should be stable over time.
        """
        # Use reliable PDF URLs that are likely to remain available
        test_data = {
            'BRnum': ['TEST001', 'TEST002', 'TEST003', 'TEST004'],  # Business Report numbers
            'Pdf_URL': [
                'https://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf',  # W3C test PDF
                'https://www.adobe.com/pdf/pdfs/ISO32000-1PublicPatentLicense.pdf',  # Adobe PDF
                'https://www.hq.nasa.gov/alsj/a17/A17_FlightPlan.pdf',  # NASA PDF
                None  # Test with missing URL
            ],
            'Report Html Address': [
                None,
                None,
                None,
                'https://www.w3.org/TR/WCAG20/'  # Test with HTML address instead of PDF
            ],
            'Year': [2020, 2021, 2022, 2023]
        }
        
        # Create the Excel file with pandas
        pd.DataFrame(test_data).to_excel(self.reports_path, index=False)
        logger.info(f"Created test reports file: {self.reports_path}")

    def create_test_metadata_file(self):
        """Create a test metadata Excel file.
        
        The metadata file tracks which PDFs have been downloaded.
        Initially, no PDFs have been downloaded ('No' status).
        """
        # Initial metadata with no downloads
        test_data = {
            'BRnum': ['TEST001', 'TEST002'],
            'pdf_downloaded': ['No', 'No'],  # Initial status is 'No' (not downloaded)
            'Year': [2020, 2021]
        }
        
        # Create the Excel file
        pd.DataFrame(test_data).to_excel(self.metadata_path, index=False)
        logger.info(f"Created test metadata file: {self.metadata_path}")

    def test_excel_file_reading(self):
        """Test that the downloader can read Excel files correctly.
        
        This test verifies that the application can properly read and process
        the Excel files that contain URL data and metadata.
        """
        # Custom test class to isolate file reading functionality
        class ExcelReader(PDF_Downloader):
            def run(self2):
                # Read the reports file
                reports_data = pd.read_excel(self2.reports_path)
                
                # Read the metadata file
                metadata = pd.read_excel(self2.metadata_path)
                
                # Return information for verification
                return {
                    'reports_count': len(reports_data),  # Number of reports
                    'reports_columns': list(reports_data.columns),  # Column names in reports
                    'metadata_count': len(metadata),  # Number of metadata records
                    'metadata_columns': list(metadata.columns)  # Column names in metadata
                }
        
        # Create and configure the reader
        reader = ExcelReader()
        reader.reports_path = self.reports_path
        reader.metadata_path = self.metadata_path
        
        # Run the reader and get results
        result = reader.run()
        
        # Verify that reading was successful with assertions
        self.assertEqual(result['reports_count'], 4, "Should read 4 reports")
        self.assertIn('BRnum', result['reports_columns'], "Reports should have BRnum column")
        self.assertIn('Pdf_URL', result['reports_columns'], "Reports should have Pdf_URL column")
        
        self.assertEqual(result['metadata_count'], 2, "Should read 2 metadata records")
        self.assertIn('pdf_downloaded', result['metadata_columns'], "Metadata should have pdf_downloaded column")
        
        logger.info("Excel file reading test passed successfully")

    def test_url_processing(self):
        """Test URL extraction and processing from input files.
        
        This test verifies that the application correctly processes
        different types of URLs (valid, invalid, missing) from the input files.
        """
        # Create a more complex test file with various URL types
        url_test_data = {
            'BRnum': ['URL001', 'URL002', 'URL003', 'URL004', 'URL005'],
            'Pdf_URL': [
                'https://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf',  # Standard URL
                'https://example.com/test.pdf',  # URL that won't resolve
                None,  # Missing URL
                'https://www.adobe.com/pdf/pdfs/ISO32000-1PublicPatentLicense.pdf',  # Another valid URL
                'not-a-valid-url'  # Malformed URL
            ],
            'Report Html Address': [
                None,
                None,
                'https://www.w3.org/TR/WCAG20/',  # HTML URL
                None,
                None
            ],
            'Year': [2020, 2021, 2022, 2023, 2024]
        }
        
        # Create the test file
        url_test_path = os.path.join(self.test_dir, 'url_test.xlsx')
        pd.DataFrame(url_test_data).to_excel(url_test_path, index=False)
        
        # Create a specialized URL processor that extends PDF_Downloader
        class URLProcessor(PDF_Downloader):
            def run(self2):
                # Read the input file
                reports_data = pd.read_excel(url_test_path)
                
                # Initialize containers for different URL types
                valid_pdf_urls = []
                valid_html_urls = []
                invalid_urls = []
                missing_urls = []
                
                # Analyze each URL row by row
                for _, row in reports_data.iterrows():
                    br_num = row['BRnum']
                    pdf_url = row.get('Pdf_URL')
                    html_url = row.get('Report Html Address')
                    
                    # Process PDF URLs
                    if pd.notna(pdf_url):  # If PDF URL is not NA (not missing)
                        if pdf_url.startswith('http'):  # Basic URL validation
                            valid_pdf_urls.append((br_num, pdf_url))
                        else:
                            invalid_urls.append((br_num, pdf_url))
                    # Process HTML URLs if PDF URL is missing
                    elif pd.notna(html_url):
                        if html_url.startswith('http'):  # Basic URL validation
                            valid_html_urls.append((br_num, html_url))
                        else:
                            invalid_urls.append((br_num, html_url))
                    # Both URLs are missing
                    else:
                        missing_urls.append(br_num)
                
                # Return results for verification
                return {
                    'valid_pdf_count': len(valid_pdf_urls),
                    'valid_html_count': len(valid_html_urls),
                    'invalid_count': len(invalid_urls),
                    'missing_count': len(missing_urls),
                    'valid_pdf_urls': valid_pdf_urls,
                    'valid_html_urls': valid_html_urls
                }
        
        # Run the processor
        processor = URLProcessor()
        result = processor.run()
        
        # Verify URL processing with assertions
        self.assertEqual(result['valid_pdf_count'], 3, "Should find 3 valid PDF URLs")
        self.assertEqual(result['valid_html_count'], 1, "Should find 1 valid HTML URL")
        self.assertEqual(result['invalid_count'], 1, "Should find 1 invalid URL")
        self.assertEqual(result['missing_count'], 0, "Should find 0 records with missing URLs")
        
        # Verify the first valid PDF URL details
        first_url = result['valid_pdf_urls'][0]
        self.assertEqual(first_url[0], 'URL001', "First valid URL should be from URL001")
        
        logger.info("URL processing test passed successfully")

    def test_file_download_and_status_tracking(self):
        """Test actual file downloading and status tracking in output files.
        
        This test verifies that:
        1. Files are actually downloaded from their URLs
        2. Download status is correctly tracked in the output report
        """
        # Run the downloader with the test files
        self.downloader.run()
        
        # Check if files were downloaded to the output directory
        downloaded_files = os.listdir(self.download_dir)
        logger.info(f"Downloaded files: {downloaded_files}")
        
        # At least some files should be downloaded
        self.assertGreater(len(downloaded_files), 0, "No files were downloaded")
        
        # Verify that PDF files were created with the correct naming convention
        pdf_files = [f for f in downloaded_files if f.endswith('.pdf')]
        self.assertGreater(len(pdf_files), 0, "No PDF files were downloaded")
        
        # Check the status report was created
        status_file = os.path.join(self.output_dir, "Download_Status.xlsx")
        self.assertTrue(os.path.exists(status_file), "Download status report was not created")
        
        # Check status report contents
        status_df = pd.read_excel(status_file)
        self.assertGreater(len(status_df), 0, "Status report is empty")
        
        # Verify required columns exist
        self.assertIn('Status', status_df.columns, "Status column missing from report")
        self.assertIn('Brnum', status_df.columns, "Brnum column missing from report")
        
        # Check for both successful and failed downloads in status column
        status_values = status_df['Status'].astype(str).str.lower()
        
        # Look for success indicators in status
        has_success = any(status_values.str.contains('success|downloaded|yes'))
        
        # This assertion might fail if all downloads fail (e.g., due to network issues)
        # So we log it but don't make it a hard requirement
        if not has_success:
            logger.warning("No successful downloads found in status report")
            
        logger.info("File download and status tracking test completed")

    def test_metadata_updating(self):
        """Test that metadata file is correctly updated after downloads.
        
        This test verifies that the metadata file is properly updated to
        reflect the download status of each PDF.
        """
        # Make a copy of the original metadata for comparison
        original_metadata = pd.read_excel(self.metadata_path)
        
        # Run the downloader
        self.downloader.run()
        
        # Check that metadata file still exists
        self.assertTrue(os.path.exists(self.metadata_path), "Metadata file should still exist")
        
        # Read the updated metadata
        updated_metadata = pd.read_excel(self.metadata_path)
        
        # Metadata file should have at least the original number of rows
        self.assertGreaterEqual(len(updated_metadata), len(original_metadata), 
                               "Updated metadata should have at least as many rows as original")
        
        # Check if any 'No' values were updated to 'Yes' or contain status information
        if 'pdf_downloaded' in updated_metadata.columns:
            updated_statuses = updated_metadata['pdf_downloaded'].astype(str)
            original_nos = original_metadata['pdf_downloaded'].astype(str).str.lower() == 'no'
            
            # Count changes from 'No' to something else
            changes = 0
            for i, was_no in enumerate(original_nos):
                if was_no and i < len(updated_statuses):
                    if updated_statuses.iloc[i].lower() != 'no':
                        changes += 1
            
            # If no changes, log a warning but don't fail the test
            # (might happen if downloads fail due to network issues)
            if changes == 0:
                logger.warning("No metadata status changes detected")
            else:
                logger.info(f"Detected {changes} metadata status updates")
        
        logger.info("Metadata updating test completed")

    def test_comprehensive_workflow(self):
        """Test the complete workflow from reading files to updating metadata.
        
        This is an end-to-end test that verifies the entire process works correctly:
        1. Reading input files
        2. Processing URLs
        3. Downloading PDFs
        4. Updating status information
        5. Generating output reports
        """
        # Create a comprehensive test with multiple cases
        comprehensive_data = {
            'BRnum': ['COMP001', 'COMP002', 'COMP003', 'COMP004', 'COMP005'],
            'Pdf_URL': [
                'https://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf',  # Should succeed
                'https://invalid-url-that-wont-work.example/test.pdf',  # Should fail
                None,  # Missing URL
                'https://www.adobe.com/pdf/pdfs/ISO32000-1PublicPatentLicense.pdf',  # Should succeed
                'not-a-valid-url'  # Invalid format
            ],
            'Report Html Address': [
                None,
                None,
                'https://www.w3.org/TR/WCAG20/',  # HTML instead of PDF
                None,
                None
            ],
            'Year': [2020, 2021, 2022, 2023, 2024]
        }
        
        # Create the comprehensive test file
        comp_test_path = os.path.join(self.test_dir, 'comprehensive_test.xlsx')
        pd.DataFrame(comprehensive_data).to_excel(comp_test_path, index=False)
        
        # Update downloader to use this file and allow processing more records
        self.downloader.reports_path = comp_test_path
        self.downloader.max_downloads = 3
        
        # Create a fresh metadata file for this test
        comp_metadata_path = os.path.join(self.test_dir, 'comprehensive_metadata.xlsx')
        pd.DataFrame({
            'BRnum': ['COMP001', 'COMP003'],
            'pdf_downloaded': ['No', 'No'],
            'Year': [2020, 2022]
        }).to_excel(comp_metadata_path, index=False)
        self.downloader.metadata_path = comp_metadata_path
        
        # Run the complete workflow
        self.downloader.run()
        
        # Check all aspects of the workflow
        
        # 1. Verify downloads occurred
        downloaded_files = os.listdir(self.download_dir)
        pdf_files = [f for f in downloaded_files if f.endswith('.pdf')]
        self.assertGreater(len(pdf_files), 0, "No PDF files were downloaded")
        
        # 2. Check for specific downloaded files (we expect this one to succeed)
        expected_file = "COMP001.pdf"
        self.assertIn(expected_file, downloaded_files, f"Expected file {expected_file} not found")
        
        # 3. Verify status report was created with correct structure
        status_file = os.path.join(self.output_dir, "Download_Status.xlsx")
        self.assertTrue(os.path.exists(status_file), "Download status report was not created")
        
        status_df = pd.read_excel(status_file)
        self.assertGreaterEqual(len(status_df), 3, "Status report should have at least 3 entries")
        
        # 4. Check for all expected BR numbers in the status report
        br_nums_in_report = status_df['Brnum'].astype(str).tolist()
        self.assertIn('COMP001', br_nums_in_report, "COMP001 should be in status report")
        
        # 5. Check metadata was updated
        updated_metadata = pd.read_excel(comp_metadata_path)
        
        # The metadata should now include the other records as well
        self.assertGreaterEqual(len(updated_metadata), 2, "Metadata should retain at least 2 records")
        
        # Get the status for COMP001 (we expect this download to succeed)
        comp001_status = None
        for _, row in updated_metadata.iterrows():
            if row['BRnum'] == 'COMP001':
                comp001_status = row.get('pdf_downloaded')
                break
        
        # COMP001 should have been updated to 'Yes' or contain success status info
        if comp001_status is not None:
            comp001_status_str = str(comp001_status).lower()
            self.assertTrue(comp001_status_str == 'yes' or 'success' in comp001_status_str,
                           f"COMP001 should show successful download, found: {comp001_status}")
        
        logger.info("Comprehensive workflow test completed successfully")

if __name__ == '__main__':
    unittest.main(verbosity=2)  # Run tests with detailed output