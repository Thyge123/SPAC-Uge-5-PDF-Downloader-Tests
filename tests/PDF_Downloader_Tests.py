import unittest  
import sys
import os
import pandas as pd  
import glob  
import shutil  #
import requests  
from unittest.mock import patch, MagicMock, mock_open 
import coverage  

# Add parent directory to path so we can import our module
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Set up code coverage tracking - this helps us know which lines of code
# are being tested and which ones aren't
cov = coverage.Coverage(
    source=['PDF_Downloader'],  # Module to measure
    omit=['*/tests/*', '*/site-packages/*']  # Don't measure these paths
)
cov.start()  # Start measuring

# Import the module we want to test
from PDF_Downloader import PDF_Downloader

#############################################################################
#                         Main Test Class                                   #
#############################################################################

class TestPDFDownloader(unittest.TestCase):
    """Tests for the PDF_Downloader class."""

    #########################################
    # Setup and Teardown                    #
    #########################################

    def setUp(self):
        """Set up a test environment before each test
        
        This method creates a fresh test environment before each test runs.
        It creates temporary directories and a PDF_Downloader instance for testing.
        """
        # Create test directories
        self.test_data_dir = os.path.join(os.path.dirname(__file__), 'test_data')
        self.test_download_dir = os.path.join(self.test_data_dir, 'Downloads')
        self.test_output_dir = os.path.join(self.test_data_dir, 'Output')
        
        # Make sure directories exist
        os.makedirs(self.test_data_dir, exist_ok=True)
        os.makedirs(self.test_download_dir, exist_ok=True)
        os.makedirs(self.test_output_dir, exist_ok=True)
        
        # Create a PDF_Downloader object with our test settings
        self.downloader = PDF_Downloader()
        self.downloader.data_dir = self.test_data_dir
        self.downloader.download_dir = self.test_download_dir
        self.downloader.output_dir = self.test_output_dir
        self.downloader.reports_path = os.path.join(self.test_data_dir, 'test_reports.xlsx')
        self.downloader.metadata_path = os.path.join(self.test_data_dir, 'test_metadata.xlsx')
    
    def tearDown(self):
        """Clean up after each test
        
        This method removes all test files and directories after each test.
        """
        # Delete all test directories and their contents
        if os.path.exists(self.test_data_dir):
            shutil.rmtree(self.test_data_dir)
    
    #########################################
    # Basic Functionality Tests             #
    #########################################

    def test_init(self):
        """Test that PDF_Downloader initializes with correct default values"""
        # Check that default settings are correct
        self.assertEqual(self.downloader.id_column, 'BRnum')
        self.assertEqual(self.downloader.max_downloads, 10)
        self.assertEqual(self.downloader.max_concurrent_threads, 5)
    
    def test_get_existing_downloads_empty(self):
        """Test getting existing downloads from an empty directory"""
        # Clear the directory to make sure it's empty
        for file in os.listdir(self.test_download_dir):
            file_path = os.path.join(self.test_download_dir, file)
            if os.path.isfile(file_path):
                os.remove(file_path)
            
        # Call the method and check result
        existing = self.downloader.get_existing_downloads()
        self.assertEqual(existing, [])  # Should return empty list
    
    def test_get_existing_downloads_with_files(self):
        """Test getting existing downloads with files present"""
        # Create fake PDF files
        test_files = ['12345.pdf', '67890.pdf', 'abcde.pdf']
        for file in test_files:
            with open(os.path.join(self.test_download_dir, file), 'w') as f:
                f.write('test content')
        
        # Get list of downloads
        existing = self.downloader.get_existing_downloads()
        
        # Check that all files were found (without .pdf extension)
        self.assertEqual(len(existing), 3)
        self.assertIn('12345', existing)
        self.assertIn('67890', existing)
        self.assertIn('abcde', existing)
    
    #########################################
    # File Download Tests                   #
    #########################################
    
    @patch('requests.get')
    def test_download_file_success(self, mock_get):
        """Test successful file download
        
        Uses mocking to simulate a successful HTTP download without making
        an actual network request.
        """
        # STEP 1: Setup mock response (fake HTTP response)
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None  # No HTTP errors
        mock_response.content = b'PDF content'  # Fake PDF content
        mock_get.return_value = mock_response  # Make requests.get return our fake response

        # STEP 2: Setup test data
        index = '12345'  # ID for the PDF
        row = pd.Series({'Pdf_URL': 'http://example.com/test.pdf', 'Report Html Address': ''})
        download_errors = []  # List to track errors
    
        # STEP 3: Call the function we're testing
        self.downloader.download_file(index, row, download_errors)
    
        # STEP 4: Verify results
        file_path = os.path.join(self.test_download_dir, f"{index}.pdf")
        # Check file was created
        self.assertTrue(os.path.exists(file_path))
        # Check no errors were recorded
        self.assertEqual(len(download_errors), 0)
    
    @patch('requests.get')
    def test_download_file_network_error(self, mock_get):
        """Test handling of network errors during download
        
        Simulates a network failure to make sure the code handles errors gracefully.
        """
        # STEP 1: Setup mock to simulate network error
        mock_get.side_effect = requests.exceptions.ConnectionError("Connection refused")
    
        # STEP 2: Setup test data
        index = '12345'
        row = pd.Series({'Pdf_URL': 'http://example.com/test.pdf', 'Report Html Address': ''})
        download_errors = []
    
        # STEP 3: Call the function
        self.downloader.download_file(index, row, download_errors)
    
        # STEP 4: Verify results
        file_path = os.path.join(self.test_download_dir, f"{index}.pdf")
        # Check file was NOT created (download failed)
        self.assertFalse(os.path.exists(file_path))
    
        # Check error was recorded properly
        self.assertEqual(len(download_errors), 2)
        self.assertEqual(download_errors[0], '12345')  # First item should be ID
        self.assertIn('Connection refused', download_errors[1])  # Second should be error
    
    @patch('requests.get')
    def test_download_file_fallback_to_html_url(self, mock_get):
        """Test fallback to HTML URL when PDF URL is not available
        
        When a PDF URL is missing, the code should try the HTML URL instead.
        """
        # STEP 1: Setup mock
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.content = b'PDF content'
        mock_get.return_value = mock_response
        
        # STEP 2: Setup test data - PDF URL is None, but HTML URL is provided
        index = '12345'
        row = pd.Series({'Pdf_URL': None, 'Report Html Address': 'http://example.com/report.html'})
        download_errors = []
        
        # STEP 3: Call the function
        self.downloader.download_file(index, row, download_errors)
        
        # STEP 4: Verify results
        file_path = os.path.join(self.test_download_dir, f"{index}.pdf")
        self.assertTrue(os.path.exists(file_path))
        
        # Check that it used the HTML URL
        mock_get.assert_called_once_with('http://example.com/report.html', verify=False, timeout=30)
    
    #########################################
    # Threading Tests                       #
    #########################################
    
    @patch('threading.Thread')
    def test_download_pdfs(self, mock_thread):
        """Test the download_pdfs method with threading
        
        Verifies that multiple PDFs are downloaded using parallel threads.
        """
        # STEP 1: Setup mock thread
        mock_thread_instance = MagicMock()
        mock_thread.return_value = mock_thread_instance
    
        # Make thread appear to run then complete
        mock_thread_instance.is_alive.side_effect = [True, True, True, False, False]
    
        # STEP 2: Create test data with two reports
        data = {
            'Pdf_URL': ['http://example.com/1.pdf', 'http://example.com/2.pdf'],
            'Report Html Address': ['', '']
        }
        download_queue = pd.DataFrame(data, index=['12345', '67890'])
        download_errors = []
    
        # STEP 3: Call the function
        self.downloader.download_pdfs(download_queue, download_errors)
    
        # STEP 4: Verify results
        # Check directories were created
        self.assertTrue(os.path.exists(self.test_download_dir))
        self.assertTrue(os.path.exists(self.test_output_dir))
    
        # Check that threads were created for both downloads
        self.assertEqual(mock_thread.call_count, 2)
        self.assertEqual(mock_thread_instance.start.call_count, 2)
    
        # Verify thread monitoring happened
        self.assertTrue(mock_thread_instance.is_alive.called)
    
    #########################################
    # Reporting Tests                       #
    #########################################
    
    def test_create_output_report(self):
        """Test creating output report
        
        Checks that the function creates a report showing which downloads
        succeeded and which failed.
        """
        try:
            # STEP 1: Create test data - one success, one failure
            data = {
                self.downloader.id_column: ['12345', '67890'],
                'Pdf_URL': ['http://example.com/1.pdf', 'http://example.com/2.pdf'],
                'Report Html Address': ['', '']
            }
            download_queue = pd.DataFrame(data, index=['12345', '67890'])
            download_errors = ['67890', 'Network error: Connection refused']

            # Create a mock successful download file
            file_path = os.path.join(self.test_download_dir, "12345.pdf")
            with open(file_path, 'w') as f:
                f.write('test content')

            # STEP 2: Call the function
            self.downloader.create_output_report(download_queue, download_errors)

            # STEP 3: Verify output file was created
            output_path = os.path.join(self.test_output_dir, "Download_Status.xlsx")
            self.assertTrue(os.path.exists(output_path))

            # STEP 4: Check the contents of the report
            output_df = pd.read_excel(output_path)
        
            # Should have 2 rows (one per download)
            self.assertEqual(len(output_df), 2, f"Expected 2 rows, got {len(output_df)}")
        
            # Check for success and failure indicators in the report
            has_downloaded = False
            has_failed = False
            has_connection_error = False
        
            for _, row in output_df.iterrows():
                # Convert all values to strings and join them
                row_str = ' '.join([str(v).lower() for v in row.values])
            
                # Look for patterns showing success and failure
                if '12345' in row_str and ('downloaded' in row_str or 'success' in row_str):
                    has_downloaded = True
            
                if '67890' in row_str and ('failed' in row_str or 'error' in row_str):
                    has_failed = True
                
                if 'connection refused' in row_str:
                    has_connection_error = True
        
            # Make sure we found all expected status indicators
            self.assertTrue(has_downloaded, "No row showing '12345' was downloaded")
            self.assertTrue(has_failed, "No row showing '67890' failed")
            self.assertTrue(has_connection_error, "Connection error message not found")
            
        except Exception as e:
            # If anything goes wrong, fail with details
            self.fail(f"Test failed with error: {e}")
    
    #########################################
    # Metadata Tests                        #
    #########################################
    
    def test_update_metadata_new_file(self):
        """Test update_metadata when metadata file doesn't exist
        
        Checks that a new metadata file is created properly from scratch.
        """
        # STEP 1: Create test data
        data = {
            self.downloader.id_column: ['12345', '67890'], 
            'Pdf_URL': ['http://example.com/1.pdf', 'http://example.com/2.pdf'],
            'Report Html Address': ['', ''],
            'Year': [2020, 2021]
        }
        download_queue = pd.DataFrame(data, index=['12345', '67890'])
        reports_data = download_queue.copy()

        # Create a mock successful download
        file_path = os.path.join(self.test_download_dir, "12345.pdf")
        with open(file_path, 'w') as f:
            f.write('test content')

        # STEP 2: Call the function
        self.downloader.update_metadata(download_queue, reports_data)

        # STEP 3: Verify metadata file was created
        self.assertTrue(os.path.exists(self.downloader.metadata_path))

        # STEP 4: Check the contents of the metadata file
        metadata_df = pd.read_excel(self.downloader.metadata_path)
    
        # Should have rows
        self.assertGreater(len(metadata_df), 0, "Metadata file is empty")
    
        # Check for IDs and download status
        has_12345 = False
        has_67890 = False
        has_yes = False
        has_no = False
    
        for _, row in metadata_df.iterrows():
            row_str = ' '.join([str(v).lower() for v in row.values])
        
            if '12345' in row_str:
                has_12345 = True
                if 'yes' in row_str:  # Should be marked as downloaded
                    has_yes = True
                
            if '67890' in row_str:
                has_67890 = True
                if 'no' in row_str:  # Should be marked as not downloaded
                    has_no = True
    
        # Verify all expected data was found
        self.assertTrue(has_12345, "ID 12345 not found in metadata")
        self.assertTrue(has_67890, "ID 67890 not found in metadata")
        self.assertTrue(has_yes, "No 'Yes' value found for downloaded PDFs")
    
    def test_update_metadata_with_duplicates(self):
        """Test update_metadata handles duplicates correctly
        
        When updating metadata with entries already present, the existing
        entries should be updated rather than duplicated.
        """
        # STEP 1: Create an existing metadata file
        existing_data = {
            self.downloader.id_column: ['12345', '67890'],
            'pdf_downloaded': ['No', 'No'],
            'Year': [2019, 2020]
        }
        metadata_df = pd.DataFrame(existing_data)
        os.makedirs(os.path.dirname(self.downloader.metadata_path), exist_ok=True)
        metadata_df.to_excel(self.downloader.metadata_path, index=False)
        
        # STEP 2: Create new data with one duplicate entry (12345)
        new_data = {
            self.downloader.id_column: ['12345', 'ABCDE'],
            'Pdf_URL': ['http://example.com/1.pdf', 'http://example.com/2.pdf'],
            'Report Html Address': ['', ''],
            'Year': [2019, 2021]
        }
        download_queue = pd.DataFrame(new_data, index=['12345', 'ABCDE'])
        reports_data = download_queue.copy()
        
        # Create a mock successful download to change the status
        file_path = os.path.join(self.test_download_dir, "12345.pdf")
        with open(file_path, 'w') as f:
            f.write('test content')
        
        # STEP 3: Call the function
        self.downloader.update_metadata(download_queue, reports_data)
        
        # STEP 4: Check the updated metadata file
        updated_df = pd.read_excel(self.downloader.metadata_path)
        
        # Debug information
        print("Metadata DataFrame columns:", updated_df.columns.tolist())
        print("Metadata DataFrame content:")
        print(updated_df)

        # Should have 3 entries: the two original plus the new one
        self.assertEqual(len(updated_df), 3)

        # The duplicate record (12345) should be updated to 'Yes'
        updated_record = updated_df[updated_df[self.downloader.id_column] == '12345']
        self.assertEqual(updated_record['pdf_downloaded'].values[0], 'Yes')

    #########################################
    # Google Drive Tests                    #
    #########################################
    
    @patch('PDF_Downloader.GoogleAuth')
    @patch('PDF_Downloader.GoogleDrive')
    @patch('os.path.exists')
    def test_upload_to_drive(self, mock_exists, mock_drive_class, mock_auth_class):
        """Test Google Drive upload functionality
        
        Verifies files are uploaded to Google Drive correctly.
        """
        # STEP 1: Setup mocks
        mock_exists.return_value = True  # Pretend files exist
        mock_auth = MagicMock()
        mock_auth_class.return_value = mock_auth
        mock_drive = MagicMock()
        mock_drive_class.return_value = mock_drive
        
        # Mock folder already exists on Drive
        mock_folder = MagicMock()
        mock_folder.__getitem__.return_value = '12345folder'  # Folder ID
        mock_drive.ListFile.return_value.GetList.return_value = [mock_folder]
        
        # STEP 2: Create mock PDF files to "upload"
        os.makedirs(self.test_download_dir, exist_ok=True)
        test_files = ['12345.pdf', '67890.pdf'] 
        for file in test_files:
            with open(os.path.join(self.test_download_dir, file), 'w') as f:
                f.write('test content')
        
        # Mock file check (files don't exist on drive yet)
        mock_drive.ListFile.return_value.GetList.side_effect = [
            [mock_folder],  # First call: folder exists
            [],  # Second call: 12345.pdf doesn't exist yet
            []   # Third call: 67890.pdf doesn't exist yet
        ]
        
        # STEP 3: Call the function
        result = self.downloader.upload_to_drive()
        
        # STEP 4: Verify results
        self.assertTrue(result)
        
        # 2 files should be created (uploaded)
        self.assertEqual(mock_drive.CreateFile.call_count, 2)
    
    @patch('os.path.exists')
    def test_upload_to_drive_missing_secrets(self, mock_exists):
        """Test upload_to_drive when Google API credentials are missing
        
        The code should handle missing credentials gracefully.
        """
        # STEP 1: Mock that credentials file doesn't exist
        mock_exists.side_effect = lambda path: path != "client_secrets.json"
        
        # STEP 2: Call the function
        result = self.downloader.upload_to_drive()
        
        # STEP 3: Verify result - should fail gracefully
        self.assertFalse(result)

    @patch('PDF_Downloader.GoogleAuth')
    @patch('os.path.exists')
    def test_upload_to_drive_authentication_flow(self, mock_exists, mock_auth_class):
        """Test different authentication scenarios in upload_to_drive
        
        Tests three authentication scenarios:
        1. No credentials (first-time use)
        2. Expired credentials
        3. Valid credentials
        """
        # STEP 1: Setup common mocks
        mock_exists.return_value = True
        mock_auth = MagicMock()
        mock_auth_class.return_value = mock_auth
        
        # STEP 2: Test Scenario 1 - No credentials
        mock_auth.credentials = None
        mock_auth.LoadCredentialsFile.return_value = None
        
        with patch('PDF_Downloader.GoogleDrive') as mock_drive_class:
            mock_drive = MagicMock()
            mock_drive_class.return_value = mock_drive
            mock_drive.ListFile().GetList.return_value = []
            mock_drive.ListFile().GetList.side_effect = [[], []]
            
            result = self.downloader.upload_to_drive()
        
        # Should trigger browser authentication
        mock_auth.LocalWebserverAuth.assert_called_once()
        
        # Reset for next test
        mock_auth.reset_mock()
        
        # STEP 3: Test Scenario 2 - Expired credentials
        mock_auth.credentials = "something"
        mock_auth.access_token_expired = True
        
        with patch('PDF_Downloader.GoogleDrive') as mock_drive_class:
            mock_drive = MagicMock()
            mock_drive_class.return_value = mock_drive
            mock_drive.ListFile().GetList.return_value = []
            mock_drive.ListFile().GetList.side_effect = [[], []]
            
            result = self.downloader.upload_to_drive()
        
        # Should refresh the token
        mock_auth.Refresh.assert_called_once()
        
        # Reset for next test
        mock_auth.reset_mock()
        
        # STEP 4: Test Scenario 3 - Valid credentials
        mock_auth.credentials = "something"
        mock_auth.access_token_expired = False
        
        with patch('PDF_Downloader.GoogleDrive') as mock_drive_class:
            mock_drive = MagicMock()
            mock_drive_class.return_value = mock_drive
            mock_drive.ListFile().GetList.return_value = []
            mock_drive.ListFile().GetList.side_effect = [[], []]
            
            result = self.downloader.upload_to_drive()
        
        # Should use existing credentials
        mock_auth.Authorize.assert_called_once()

    @patch('PDF_Downloader.GoogleAuth')
    @patch('PDF_Downloader.GoogleDrive')
    @patch('os.path.exists')
    def test_upload_to_drive_create_folder(self, mock_exists, mock_drive_class, mock_auth_class):
        """Test folder creation in upload_to_drive
        
        If the target folder doesn't exist on Drive, it should be created.
        """
        # STEP 1: Setup mocks
        mock_exists.return_value = True
        mock_auth = MagicMock()
        mock_auth_class.return_value = mock_auth
        mock_drive = MagicMock()
        mock_drive_class.return_value = mock_drive
        
        # Mock folder doesn't exist on Drive
        mock_drive.ListFile.return_value.GetList.return_value = []
        
        # STEP 2: Create mock PDF file
        os.makedirs(self.test_download_dir, exist_ok=True)
        with open(os.path.join(self.test_download_dir, "test.pdf"), 'w') as f:
            f.write('test content')
        
        # STEP 3: Call the function
        result = self.downloader.upload_to_drive()
        
        # STEP 4: Verify folder was created
        self.assertTrue(mock_drive.CreateFile.called)
        self.assertTrue(result)

    @patch('PDF_Downloader.GoogleAuth')
    @patch('PDF_Downloader.GoogleDrive')
    @patch('os.path.exists')
    def test_upload_to_drive_no_files(self, mock_exists, mock_drive_class, mock_auth_class):
        """Test upload_to_drive with no files to upload
        
        Should handle the case of an empty directory gracefully.
        """
        # STEP 1: Setup mocks
        mock_exists.return_value = True
        mock_auth = MagicMock()
        mock_auth_class.return_value = mock_auth
        mock_drive = MagicMock()
        mock_drive_class.return_value = mock_drive
        
        # STEP 2: Ensure no PDF files exist
        if os.path.exists(self.test_download_dir):
            shutil.rmtree(self.test_download_dir)
        os.makedirs(self.test_download_dir, exist_ok=True)
        
        # STEP 3: Call the function
        result = self.downloader.upload_to_drive()
        
        # STEP 4: Verify result - should succeed even with no files
        self.assertTrue(result)

    #########################################
    # Error Handling Tests                  #
    #########################################
    
    @patch('pandas.read_excel')
    def test_run_file_not_found(self, mock_read_excel):
        """Test error handling when input file is missing
        
        The program should handle missing files gracefully.
        """
        # STEP 1: Setup mock to simulate file not found
        mock_read_excel.side_effect = FileNotFoundError("File not found")
        
        # STEP 2: Call the function - should not crash
        self.downloader.run()
        
        # STEP 3: Verify mock was called
        mock_read_excel.assert_called_once()

    @patch('pandas.read_excel')
    def test_run_general_exception(self, mock_read_excel):
        """Test error handling for unexpected errors
        
        The program should handle any unexpected errors gracefully.
        """
        # STEP 1: Setup mock to raise a generic exception
        mock_read_excel.side_effect = Exception("Some other error")
        
        # STEP 2: Call the function - should not crash
        self.downloader.run()
        
        # STEP 3: Verify mock was called
        mock_read_excel.assert_called_once()

    #########################################
    # Main Workflow Tests                   #
    #########################################
    
    def test_run_with_mocked_dependencies(self):
        """Test the main run method
        
        Verifies the high-level workflow by mocking all components.
        """
        # STEP 1: Create test Excel file
        data = {
            self.downloader.id_column: ['12345', '67890'],
            'Pdf_URL': ['http://example.com/1.pdf', 'http://example.com/2.pdf'],
            'Report Html Address': ['', '']
        }
        df = pd.DataFrame(data)
        os.makedirs(os.path.dirname(self.downloader.reports_path), exist_ok=True)
        df.to_excel(self.downloader.reports_path, index=False)
        
        # STEP 2: Patch all methods
        with patch.object(self.downloader, 'get_existing_downloads', return_value=[]) as mock_get:
            with patch.object(self.downloader, 'download_pdfs') as mock_download:
                with patch.object(self.downloader, 'create_output_report') as mock_output:
                    with patch.object(self.downloader, 'update_metadata') as mock_metadata:
                        with patch.object(self.downloader, 'upload_to_drive', return_value=True) as mock_upload:
                            # STEP 3: Run the method
                            self.downloader.run()
                            
                            # STEP 4: Verify all methods were called in order
                            mock_get.assert_called_once()
                            mock_download.assert_called_once()
                            mock_output.assert_called_once()
                            mock_metadata.assert_called_once()
                            mock_upload.assert_called_once()


    @patch('PDF_Downloader.PDF_Downloader.run')
    def test_main_function(self, mock_run):
        """Test main function
        
        Checks that the main() function correctly starts the downloader.
        """
        # STEP 1: Import main
        from PDF_Downloader import main
        
        # STEP 2: Call main
        main()
        
        # STEP 3: Verify run was called
        mock_run.assert_called_once()


# Run the tests when script is executed directly
if __name__ == '__main__':
    # Run tests but don't exit automatically
    result = unittest.main(exit=False, verbosity=2)
    
    # Generate coverage report
    cov.stop()
    cov.save()
    print("\n\nCoverage Report:")
    cov.report()
    
    # Create HTML coverage report for easy browsing
    html_dir = os.path.join(os.path.dirname(__file__), 'coverage_html')
    cov.html_report(directory=html_dir)
    print(f"\nHTML coverage report generated in: {html_dir}")
    
    # Exit with proper status code
    sys.exit(not result.result.wasSuccessful())