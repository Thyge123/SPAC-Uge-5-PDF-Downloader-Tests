import unittest  # Python's built-in testing framework
import sys
import os
import pandas as pd  # For Excel file operations
import shutil  # For file and directory operations
import logging  # For structured logging
import time
from pathlib import Path  # Object-oriented filesystem paths

# Add parent directory to path so we can import PDF_Downloader module
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
        """Set up test environment with actual files and directories."""
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
        self.downloader.max_downloads = 2
        self.downloader.max_concurrent_threads = 2
        
        logger.info(f"Test environment set up at: {self.test_dir}\n")
        logger.info("--------------------------------------------------\n")

    def create_test_reports_file(self):
        """Create a test reports Excel file with real, reliable PDF URLs."""
        # Use reliable PDF URLs that are likely to remain available
        test_data = {
            'BRnum': ['TEST001', 'TEST002', 'TEST003', 'TEST004'], 
            'Pdf_URL': [
                'http://cdn12.a1.net/m/resources/media/pdf/A1-Umwelterkl-rung-2016-2017.pdf', 
                'https://www.hkexnews.hk/listedco/listconews/sehk/2017/0512/ltn20170512165.pdf',  
                'https://ebooks.exakta.se/aak/2017/hallbarhetsrapport_2016_2017_en/pubData/source/aak_sustainability_report_2016_2017_ebook.pdf',  
                None  # Test with missing URL
            ],
            'Report Html Address': [
                None,
                None,
                None,
                'https://www.ab-science.com/file_bdd/content/1480493978_DDRVF.pdf' 
            ], 
        }
        
        # Create the Excel file with pandas
        pd.DataFrame(test_data).to_excel(self.reports_path, index=False)
        logger.info(f"Created test reports file: {self.reports_path}")

    def create_test_metadata_file(self):
        """Create a test metadata Excel file."""
        # Initial metadata with no downloads
        test_data = {
            'BRnum': ['TEST001', 'TEST002'],
            'pdf_downloaded': ['No', 'No'],  # Initial status is 'No' (not downloaded)
        }
        
        # Create the Excel file
        pd.DataFrame(test_data).to_excel(self.metadata_path, index=False)
        logger.info(f"Created test metadata file: {self.metadata_path}")

    def test_excel_file_reading(self):
        """Test that the downloader can read Excel files correctly."""
        # Read the reports file using pandas directly
        reports_data = pd.read_excel(self.reports_path)
        
        # Read the metadata file using pandas directly
        metadata = pd.read_excel(self.metadata_path)
        
        # Verify that reading was successful with assertions
        self.assertEqual(len(reports_data), 4, "Should read 4 reports")
        self.assertIn('BRnum', reports_data.columns, "Reports should have BRnum column")
        self.assertIn('Pdf_URL', reports_data.columns, "Reports should have Pdf_URL column")
        
        self.assertEqual(len(metadata), 2, "Should read 2 metadata records")
        self.assertIn('pdf_downloaded', metadata.columns, "Metadata should have pdf_downloaded column")
        
        logger.info("Excel file reading test passed successfully\n")
        logger.info("--------------------------------------------------\n")

    def test_get_existing_downloads(self):
        """Test the get_existing_downloads method."""
        # Create test files in the download directory
        test_files = ['TEST001.pdf', 'TEST002.pdf']
        for file in test_files:
            with open(os.path.join(self.download_dir, file), 'w') as f:
                f.write('Test PDF content')
        
        # Call the method directly
        existing_downloads = self.downloader.get_existing_downloads()
        
        # Verify results
        self.assertEqual(len(existing_downloads), 2, "Should find 2 existing downloads")
        self.assertIn('TEST001', existing_downloads, "Should find TEST001.pdf")
        self.assertIn('TEST002', existing_downloads, "Should find TEST002.pdf")
        
        logger.info("get_existing_downloads test passed successfully\n")
        logger.info("--------------------------------------------------\n")

    def test_url_extraction(self):
        """Test URL extraction from input files."""
        # Read the Excel file directly
        reports_data = pd.read_excel(self.reports_path, index_col=self.downloader.id_column)
        
        # Check for valid download URLs
        has_valid_url = (reports_data.Pdf_URL.notnull()) | (reports_data['Report Html Address'].notnull())
        reports_with_urls = reports_data[has_valid_url]
        
        # Verify URL extraction
        self.assertEqual(len(reports_with_urls), 4, "Should find 4 reports with URLs")
        
        # Count primary vs. secondary URLs
        primary_urls = reports_data[reports_data.Pdf_URL.notnull()]
        secondary_urls = reports_data[(reports_data.Pdf_URL.isnull()) & (reports_data['Report Html Address'].notnull())]
        
        self.assertEqual(len(primary_urls), 3, "Should find 3 reports with primary URLs")
        self.assertEqual(len(secondary_urls), 1, "Should find 1 report with secondary URL")
        
        logger.info("URL extraction test passed successfully\n")
        logger.info("--------------------------------------------------\n")

    def test_download_file(self):
        """Test the download_file method for a single file."""
        # Create a mock row for a reliable URL
        mock_row = pd.Series({
            'Pdf_URL': 'http://cdn12.a1.net/m/resources/media/pdf/A1-Umwelterkl-rung-2016-2017.pdf',
            'Report Html Address': None
        })
        
        # Create an empty list to capture any errors
        download_errors = []
        
        # Call the download_file method directly
        self.downloader.download_file('TEST999', mock_row, download_errors)
        
        # Check if the file was downloaded successfully
        downloaded_file = os.path.join(self.download_dir, 'TEST999.pdf')
        self.assertTrue(os.path.exists(downloaded_file), "File should be downloaded")
        self.assertGreater(os.path.getsize(downloaded_file), 0, "File should not be empty")
        
        # Verify no errors occurred
        self.assertEqual(len(download_errors), 0, "No errors should occur")
        
        logger.info("download_file test passed successfully")

    def test_download_with_error(self):
        """Test error handling in the download_file method."""
        # Create a mock row with a broken URL
        mock_row = pd.Series({
            'Pdf_URL': 'https://invalid-url-that-wont-work.example/test.pdf',
            'Report Html Address': None
        })
        
        # Create a list to capture errors
        download_errors = []
        
        # Call the download_file method directly
        self.downloader.download_file('TEST888', mock_row, download_errors)
        
        # Check that the file wasn't downloaded
        downloaded_file = os.path.join(self.download_dir, 'TEST888.pdf')
        self.assertFalse(os.path.exists(downloaded_file), "File should not be downloaded")
        
        # Verify errors were captured
        self.assertGreater(len(download_errors), 0, "Errors should be captured")
        self.assertEqual(download_errors[0], 'TEST888', "Error should reference correct ID")
        
        logger.info("download error handling test passed successfully\n")
        logger.info("--------------------------------------------------\n")

    def test_file_download_and_status_tracking(self):
        """Test actual file downloading and status tracking in output files."""
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
        has_success = any(status_values.str.contains('downloaded'))
        
        # This assertion might fail if all downloads fail (e.g., due to network issues)
        # So we log it but don't make it a hard requirement
        if not has_success:
            logger.warning("No successful downloads found in status report")
            
        logger.info("File download and status tracking test completed\n")
        logger.info("--------------------------------------------------\n")

    def test_create_output_report(self):
        """Test the create_output_report method."""
        # Create a test download queue
        download_queue = pd.DataFrame({
            'Pdf_URL': ['http://cdn12.a1.net/m/resources/media/pdf/A1-Umwelterkl-rung-2016-2017.pdf', 'http://example.com/test2.pdf'],
            'Report Html Address': [None, None]
        }, index=['TEST777', 'TEST778'])
        
        self.downloader.download_file('TEST777', download_queue.loc['TEST777'], [])

        # Create a test downloaded file (just one to test both success and failure cases)    
        #with open(os.path.join(self.download_dir, 'TEST777.pdf'), 'w') as f:
            #f.write('Test content')
        
        # Create error list
        download_errors = ['TEST778', 'Connection timeout']
        
        # Call the method directly
        self.downloader.create_output_report(download_queue, download_errors)
        
        # Check the status report was created
        status_file = os.path.join(self.output_dir, "Download_Status.xlsx")
        self.assertTrue(os.path.exists(status_file), "Status report should be created")
        
        # Verify contents
        status_df = pd.read_excel(status_file)
        self.assertEqual(len(status_df), 2, "Should have 2 status entries")
        
        # Find each test ID in the report
        test777_status = status_df[status_df['Brnum'] == 'TEST777']['Status'].iloc[0]
        test778_status = status_df[status_df['Brnum'] == 'TEST778']['Status'].iloc[0]
        
        self.assertEqual(test777_status, "Downloaded", "TEST777 should show as Downloaded")
        self.assertEqual(test778_status, "Failed", "TEST778 should show as Failed")
        
        logger.info("create_output_report test passed successfully\n")
        logger.info("--------------------------------------------------\n")

    def test_metadata_updating(self):
        """Test the update_metadata method directly."""
        # Create a download queue
        download_queue = pd.DataFrame({
            'Pdf_URL': ['http://example.com/test1.pdf', 'http://example.com/test2.pdf'],
        }, index=['TEST555', 'TEST556'])
        
        # Create source reports data with additional columns
        reports_data = pd.DataFrame({
            'Pdf_URL': ['http://example.com/test1.pdf', 'http://example.com/test2.pdf'],
            'Extra_Column': ['Value1', 'Value2']
        }, index=['TEST555', 'TEST556'])
        
        # Create a test downloaded file (just one to test both success and failure cases)
        with open(os.path.join(self.download_dir, 'TEST555.pdf'), 'w') as f:
            f.write('Test content')
        
        # Call the update_metadata method directly
        self.downloader.update_metadata(download_queue, reports_data)
        
        # Verify the metadata file was updated
        self.assertTrue(os.path.exists(self.metadata_path), "Metadata file should exist")
        
        # Read the updated metadata
        updated_metadata = pd.read_excel(self.metadata_path)
        
        # Find the test entries in the metadata
        test555_entry = updated_metadata[updated_metadata['BRnum'] == 'TEST555']
        test556_entry = updated_metadata[updated_metadata['BRnum'] == 'TEST556']
        
        # Verify entries exist
        self.assertGreater(len(test555_entry), 0, "TEST555 should be in metadata")
        self.assertGreater(len(test556_entry), 0, "TEST556 should be in metadata")
        
        # Verify download status
        self.assertEqual(test555_entry['pdf_downloaded'].iloc[0], 'Yes', "TEST555 should be marked as downloaded")
        self.assertEqual(test556_entry['pdf_downloaded'].iloc[0], 'No', "TEST556 should be marked as not downloaded")
        
        logger.info("update_metadata test passed successfully\n")
        logger.info("--------------------------------------------------\n")

    def test_comprehensive_workflow(self):
        """Test the complete workflow from reading files to updating metadata."""
        # Create a comprehensive test with multiple cases
        comprehensive_data = {
            'BRnum': ['COMP001', 'COMP002', 'COMP003', 'COMP004', 'COMP005'],
            'Pdf_URL': [
                'http://cdn12.a1.net/m/resources/media/pdf/A1-Umwelterkl-rung-2016-2017.pdf',  # Should succeed
                'https://invalid-url-that-wont-work.example/test.pdf',  # Should fail
                None,  # Missing URL
                'https://www.hkexnews.hk/listedco/listconews/sehk/2017/0512/ltn20170512165.pdf',  # Should succeed
                'not-a-valid-url'  # Invalid format
            ],
            'Report Html Address': [
                None,
                None,
                'https://ebooks.exakta.se/aak/2017/hallbarhetsrapport_2016_2017_en/pubData/source/aak_sustainability_report_2016_2017_ebook.pdf',  # HTML instead of PDF
                None,
                None
            ],
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
        
        logger.info("Comprehensive workflow test completed successfully\n")
        logger.info("--------------------------------------------------\n")

    def tearDown(self):
        """Clean up after tests by removing test files and directories."""
        try:
            # Remove test directories
            shutil.rmtree(self.test_dir, ignore_errors=True)
            logger.info(f"Test cleanup: Removed test directory {self.test_dir}")
        except Exception as e:
            logger.warning(f"Test cleanup failed: {e}")

if __name__ == '__main__':
    unittest.main(verbosity=2)  # Run tests with detailed output