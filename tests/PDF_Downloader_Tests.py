import unittest
import sys
import os
import pandas as pd
import glob
import shutil
import requests 
from unittest.mock import patch, MagicMock, mock_open
import coverage

# Add the parent directory to sys.path to allow imports from the main package
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Initialize coverage before importing your module
cov = coverage.Coverage(
    source=['PDF_Downloader'],
    omit=['*/tests/*', '*/site-packages/*']
)
cov.start()

from PDF_Downloader import PDF_Downloader

class TestPDFDownloader(unittest.TestCase):
# Add test categories using docstring headers
    """Tests for the PDF_Downloader class.
    
    Test categories:
    - Initialization and core functionality
    - File operations
    - Network operations
    - Threading
    - Metadata handling
    - Integration tests
    """

    def setUp(self):
        """Set up a test environment before each test"""
        # Create a test instance with test directories
        self.test_data_dir = os.path.join(os.path.dirname(__file__), 'test_data')
        self.test_download_dir = os.path.join(self.test_data_dir, 'Downloads')
        self.test_output_dir = os.path.join(self.test_data_dir, 'Output')
        
        # Create test directories if they don't exist
        os.makedirs(self.test_data_dir, exist_ok=True)
        os.makedirs(self.test_download_dir, exist_ok=True)
        os.makedirs(self.test_output_dir, exist_ok=True)
        
        # Create a downloader instance with test directories
        self.downloader = PDF_Downloader()
        self.downloader.data_dir = self.test_data_dir
        self.downloader.download_dir = self.test_download_dir
        self.downloader.output_dir = self.test_output_dir
        self.downloader.reports_path = os.path.join(self.test_data_dir, 'test_reports.xlsx')
        self.downloader.metadata_path = os.path.join(self.test_data_dir, 'test_metadata.xlsx')
        
    
    def tearDown(self):
        """Clean up after each test"""
        # Remove test directories and their contents
        if os.path.exists(self.test_data_dir):
            shutil.rmtree(self.test_data_dir)
    
    def test_init(self):
        """Test that the PDF_Downloader initializes with correct default values"""
        self.assertEqual(self.downloader.id_column, 'BRnum')
        self.assertEqual(self.downloader.max_downloads, 10)
        self.assertEqual(self.downloader.max_concurrent_threads, 5)
    
    def test_get_existing_downloads_empty(self):
        """Test getting existing downloads from an empty directory"""
        # Clear the directory to ensure it's empty
        for file in os.listdir(self.test_download_dir):
            file_path = os.path.join(self.test_download_dir, file)
            if os.path.isfile(file_path):
                os.remove(file_path)
            
        # Now test getting downloads from the empty directory
        existing = self.downloader.get_existing_downloads()
        self.assertEqual(existing, [])
    
    def test_get_existing_downloads_with_files(self):
        """Test getting existing downloads with files present"""
        # Create some mock PDF files
        test_files = ['12345.pdf', '67890.pdf', 'abcde.pdf']
        for file in test_files:
            with open(os.path.join(self.test_download_dir, file), 'w') as f:
                f.write('test content')
        
        # Get the list of downloads
        existing = self.downloader.get_existing_downloads()
        
        # Check if all files are found (without .pdf extension)
        self.assertEqual(len(existing), 3)
        self.assertIn('12345', existing)
        self.assertIn('67890', existing)
        self.assertIn('abcde', existing)
    
    @patch('requests.get')
    def test_download_file_success(self, mock_get):
        """Test successful file download"""
        # Setup mock for successful response
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.content = b'PDF content'
        mock_get.return_value = mock_response

        # Setup test data
        index = '12345'
        row = pd.Series({'Pdf_URL': 'http://example.com/test.pdf', 'Report Html Address': ''})
        download_errors = []
    
        # Call the function
        self.downloader.download_file(index, row, download_errors)
    
        # Check if file was created
        file_path = os.path.join(self.test_download_dir, f"{index}.pdf")
        self.assertTrue(os.path.exists(file_path))
    
        # Verify no errors were recorded
        self.assertEqual(len(download_errors), 0)
    
    @patch('requests.get')
    def test_download_file_network_error(self, mock_get):
        """Test handling of network errors during download"""
        # Setup mock to raise an exception
        mock_get.side_effect = requests.exceptions.ConnectionError("Connection refused")
    
        # Setup test data
        index = '12345'
        row = pd.Series({'Pdf_URL': 'http://example.com/test.pdf', 'Report Html Address': ''})
        download_errors = []
    
        # Call the function
        self.downloader.download_file(index, row, download_errors)
    
        # Check if file was not created
        file_path = os.path.join(self.test_download_dir, f"{index}.pdf")
        self.assertFalse(os.path.exists(file_path))
    
        # Verify errors were recorded correctly
        self.assertEqual(len(download_errors), 2)
        self.assertEqual(download_errors[0], '12345')
        self.assertIn('Connection refused', download_errors[1])
    
    @patch('requests.get')
    def test_download_file_fallback_to_html_url(self, mock_get):
        """Test fallback to HTML URL when PDF URL is not available"""
        # Setup mock response
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.content = b'PDF content'
        mock_get.return_value = mock_response
        
        # Setup test data with missing PDF URL but valid HTML URL
        index = '12345'
        row = pd.Series({'Pdf_URL': None, 'Report Html Address': 'http://example.com/report.html'})
        download_errors = []
        
        # Call the function
        self.downloader.download_file(index, row, download_errors)
        
        # Check if file was created
        file_path = os.path.join(self.test_download_dir, f"{index}.pdf")
        self.assertTrue(os.path.exists(file_path))
        
        # Verify requests.get was called with the HTML URL
        mock_get.assert_called_once_with('http://example.com/report.html', verify=False, timeout=30)
    
    @patch('threading.Thread')
    def test_download_pdfs(self, mock_thread):
        """Test the download_pdfs method with threading"""
        # Setup mock thread
        mock_thread_instance = MagicMock()
        mock_thread.return_value = mock_thread_instance
    
        # Set up the is_alive method to return True initially but then False to simulate thread completion
        # First 3 checks will return True (threads running), then False (threads completed)
        mock_thread_instance.is_alive.side_effect = [True, True, True, False, False]
    
        # Create test data with two reports
        data = {
            'Pdf_URL': ['http://example.com/1.pdf', 'http://example.com/2.pdf'],
            'Report Html Address': ['', '']
        }
        download_queue = pd.DataFrame(data, index=['12345', '67890'])
        download_errors = []
    
        # Call the function
        self.downloader.download_pdfs(download_queue, download_errors)
    
        # Verify directories were created
        self.assertTrue(os.path.exists(self.test_download_dir))
        self.assertTrue(os.path.exists(self.test_output_dir))
    
        # Verify threads were created and started for both downloads
        self.assertEqual(mock_thread.call_count, 2)
        self.assertEqual(mock_thread_instance.start.call_count, 2)
    
        # Verify that is_alive was called (confirming our thread monitoring code ran)
        self.assertTrue(mock_thread_instance.is_alive.called)
    
    def test_create_output_report(self):
        """Test creating output report"""
        try:
            # Create test data with two reports - one succeeded, one failed
            data = {
                self.downloader.id_column: ['12345', '67890'],  # Add the ID column explicitly
                'Pdf_URL': ['http://example.com/1.pdf', 'http://example.com/2.pdf'],
                'Report Html Address': ['', '']
            }
            # Make index-based and column-based identifiers both available
            download_queue = pd.DataFrame(data, index=['12345', '67890'])
            download_errors = ['67890', 'Network error: Connection refused']

            # Create a mock successful download
            file_path = os.path.join(self.test_download_dir, "12345.pdf")
            with open(file_path, 'w') as f:
                f.write('test content')

            # Call the function
            self.downloader.create_output_report(download_queue, download_errors)

            # Verify output file was created
            output_path = os.path.join(self.test_output_dir, "Download_Status.xlsx")
            self.assertTrue(os.path.exists(output_path))

            # Read the output file and check its contents
            output_df = pd.read_excel(output_path)
        
            # Verify we have the right number of rows
            self.assertEqual(len(output_df), 2, f"Expected 2 rows, got {len(output_df)}")
        
            # More flexible verification - check for status patterns in any column
            has_downloaded = False
            has_failed = False
            has_connection_error = False
        
            for _, row in output_df.iterrows():
                row_str = ' '.join([str(v).lower() for v in row.values])
            
                if '12345' in row_str and ('downloaded' in row_str or 'success' in row_str):
                    has_downloaded = True
            
                if '67890' in row_str and ('failed' in row_str or 'error' in row_str):
                    has_failed = True
                
                if 'connection refused' in row_str:
                    has_connection_error = True
        
            # Assertions for the patterns we're looking for
            self.assertTrue(has_downloaded, "No row showing '12345' was downloaded")
            self.assertTrue(has_failed, "No row showing '67890' failed")
            self.assertTrue(has_connection_error, "Connection error message not found")
            
        except Exception as e:
            self.fail(f"Test failed with error: {e}")
    
    def test_update_metadata_new_file(self):
        """Test update_metadata when metadata file doesn't exist"""
        # Create test data with both index and column IDs
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

        # Call the function
        self.downloader.update_metadata(download_queue, reports_data)

        # Verify metadata file was created
        self.assertTrue(os.path.exists(self.downloader.metadata_path))

        # Read the metadata file and check its contents
        metadata_df = pd.read_excel(self.downloader.metadata_path)
    
        # Check overall length
        self.assertGreater(len(metadata_df), 0, "Metadata file is empty")
    
        # Check for patterns in any column rather than specific column structure
        has_12345 = False
        has_67890 = False
        has_yes = False
        has_no = False
    
        # Look through all values in the DataFrame for the patterns
        for _, row in metadata_df.iterrows():
            row_str = ' '.join([str(v).lower() for v in row.values])
        
            if '12345' in row_str:
                has_12345 = True
                if 'yes' in row_str:
                    has_yes = True
                
            if '67890' in row_str:
                has_67890 = True
                if 'no' in row_str:
                    has_no = True
    
        # Verify we found the expected patterns
        self.assertTrue(has_12345, "ID 12345 not found in metadata")
        self.assertTrue(has_67890, "ID 67890 not found in metadata")
    
        # We should have at least one PDF marked as downloaded
        self.assertTrue(has_yes, "No 'Yes' value found for downloaded PDFs")
    
    @patch('PDF_Downloader.GoogleAuth')
    @patch('PDF_Downloader.GoogleDrive')
    @patch('os.path.exists')
    def test_upload_to_drive(self, mock_exists, mock_drive_class, mock_auth_class):
        """Test Google Drive upload functionality"""
        # Setup mocks
        mock_exists.return_value = True # Metadata file exists
        mock_auth = MagicMock()
        mock_auth_class.return_value = mock_auth # Return the mock auth object
        mock_drive = MagicMock()
        mock_drive_class.return_value = mock_drive # Return the mock drive object
        
        # Mock folder listing
        mock_folder = MagicMock() # Mock folder object
        mock_folder.__getitem__.return_value = '12345folder' # Mock folder ID
        mock_drive.ListFile.return_value.GetList.return_value = [mock_folder] # Mock folder listing
        
        # Create mock PDF files
        os.makedirs(self.test_download_dir, exist_ok=True) # Create the download directory
        test_files = ['12345.pdf', '67890.pdf'] 
        for file in test_files:
            with open(os.path.join(self.test_download_dir, file), 'w') as f:
                f.write('test content')
        
        # Mock file check response (file doesn't exist on drive)
        mock_drive.ListFile.return_value.GetList.side_effect = [
            [mock_folder],  # First call returns the folder
            [],  # Second call checks if 12345.pdf exists (it doesn't)
            []   # Third call checks if 67890.pdf exists (it doesn't)
        ]
        
        # Call the function
        result = self.downloader.upload_to_drive()
        
        # Verify result
        self.assertTrue(result)
        
        # Verify the right calls were made (2 files should be created)
        self.assertEqual(mock_drive.CreateFile.call_count, 2)
    
    def test_run_with_mocked_dependencies(self):
        """Test the main run method with mocked dependencies"""
        # Create a simple test Excel file with two reports
        data = {
            self.downloader.id_column: ['12345', '67890'],
            'Pdf_URL': ['http://example.com/1.pdf', 'http://example.com/2.pdf'],
            'Report Html Address': ['', '']
        }
        df = pd.DataFrame(data)
        os.makedirs(os.path.dirname(self.downloader.reports_path), exist_ok=True)
        df.to_excel(self.downloader.reports_path, index=False)
        
        # Patch all the methods that would be called
        with patch.object(self.downloader, 'get_existing_downloads', return_value=[]) as mock_get:
            with patch.object(self.downloader, 'download_pdfs') as mock_download:
                with patch.object(self.downloader, 'create_output_report') as mock_output:
                    with patch.object(self.downloader, 'update_metadata') as mock_metadata:
                        with patch.object(self.downloader, 'upload_to_drive', return_value=True) as mock_upload:
                            # Run the method
                            self.downloader.run()
                            
                            # Verify all methods were called correctly
                            mock_get.assert_called_once()
                            mock_download.assert_called_once()
                            mock_output.assert_called_once()
                            mock_metadata.assert_called_once()
                            mock_upload.assert_called_once()
    
    # NEWLY ADDED TESTS
    @patch('os.path.exists')
    def test_upload_to_drive_missing_secrets(self, mock_exists):
        """Test upload_to_drive when client_secrets.json doesn't exist"""
        # Mock that client_secrets.json doesn't exist
        mock_exists.side_effect = lambda path: path != "client_secrets.json"
        
        # Call the function
        result = self.downloader.upload_to_drive()
        
        # Verify the function returns False because of missing client_secrets.json
        self.assertFalse(result)

    @patch('PDF_Downloader.GoogleAuth')
    @patch('os.path.exists')
    def test_upload_to_drive_authentication_flow(self, mock_exists, mock_auth_class):
        """Test different authentication scenarios in upload_to_drive"""
        # Setup mocks
        mock_exists.return_value = True
        mock_auth = MagicMock()
        mock_auth_class.return_value = mock_auth
        
        # Test 1: No credentials
        mock_auth.credentials = None
        mock_auth.LoadCredentialsFile.return_value = None
        
        # Call the function
        with patch('PDF_Downloader.GoogleDrive') as mock_drive_class:
            mock_drive = MagicMock()
            mock_drive_class.return_value = mock_drive
            mock_drive.ListFile().GetList.return_value = []
            mock_drive.ListFile().GetList.side_effect = [[], []]
            
            result = self.downloader.upload_to_drive()
        
        # Verify LocalWebserverAuth was called
        mock_auth.LocalWebserverAuth.assert_called_once()
        
        # Reset mocks
        mock_auth.reset_mock()
        
        # Test 2: Expired credentials
        mock_auth.credentials = "something"
        mock_auth.access_token_expired = True
        
        # Call the function again
        with patch('PDF_Downloader.GoogleDrive') as mock_drive_class:
            mock_drive = MagicMock()
            mock_drive_class.return_value = mock_drive
            mock_drive.ListFile().GetList.return_value = []
            mock_drive.ListFile().GetList.side_effect = [[], []]
            
            result = self.downloader.upload_to_drive()
        
        # Verify Refresh was called
        mock_auth.Refresh.assert_called_once()
        
        # Reset mocks
        mock_auth.reset_mock()
        
        # Test 3: Valid credentials
        mock_auth.credentials = "something"
        mock_auth.access_token_expired = False
        
        # Call the function again
        with patch('PDF_Downloader.GoogleDrive') as mock_drive_class:
            mock_drive = MagicMock()
            mock_drive_class.return_value = mock_drive
            mock_drive.ListFile().GetList.return_value = []
            mock_drive.ListFile().GetList.side_effect = [[], []]
            
            result = self.downloader.upload_to_drive()
        
        # Verify Authorize was called
        mock_auth.Authorize.assert_called_once()

    @patch('PDF_Downloader.GoogleAuth')
    @patch('PDF_Downloader.GoogleDrive')
    @patch('os.path.exists')
    def test_upload_to_drive_create_folder(self, mock_exists, mock_drive_class, mock_auth_class):
        """Test folder creation in upload_to_drive when folder doesn't exist"""
        # Setup mocks
        mock_exists.return_value = True # Files exist
        mock_auth = MagicMock()
        mock_auth_class.return_value = mock_auth
        mock_drive = MagicMock()
        mock_drive_class.return_value = mock_drive
        
        # Mock that the folder doesn't exist
        mock_drive.ListFile.return_value.GetList.return_value = []
        
        # Create mock PDF files for upload
        os.makedirs(self.test_download_dir, exist_ok=True)
        with open(os.path.join(self.test_download_dir, "test.pdf"), 'w') as f:
            f.write('test content')
        
        # Call the function
        result = self.downloader.upload_to_drive()
        
        # Verify folder was created
        self.assertTrue(mock_drive.CreateFile.called)
        self.assertTrue(result)

    @patch('PDF_Downloader.GoogleAuth')
    @patch('PDF_Downloader.GoogleDrive')
    @patch('os.path.exists')
    def test_upload_to_drive_no_files(self, mock_exists, mock_drive_class, mock_auth_class):
        """Test upload_to_drive when no files are available for upload"""
        # Setup mocks
        mock_exists.return_value = True
        mock_auth = MagicMock()
        mock_auth_class.return_value = mock_auth
        mock_drive = MagicMock()
        mock_drive_class.return_value = mock_drive
        
        # Clear download directory
        if os.path.exists(self.test_download_dir):
            shutil.rmtree(self.test_download_dir)
        os.makedirs(self.test_download_dir, exist_ok=True)
        
        # Call the function
        result = self.downloader.upload_to_drive()
        
        # Verify result
        self.assertTrue(result)  # Should succeed even when no files exist

    def test_update_metadata_with_duplicates(self):
        """Test update_metadata handles duplicates correctly"""
        # Create an existing metadata file with some entries
        existing_data = {
            self.downloader.id_column: ['12345', '67890'],
            'pdf_downloaded': ['No', 'No'],
            'Year': [2019, 2020]
        }
        metadata_df = pd.DataFrame(existing_data)
        os.makedirs(os.path.dirname(self.downloader.metadata_path), exist_ok=True)
        metadata_df.to_excel(self.downloader.metadata_path, index=False)
        
        # Create new data that includes one duplicate entry (12345) with updated status
        new_data = {
            self.downloader.id_column: ['12345', 'ABCDE'],
            'Pdf_URL': ['http://example.com/1.pdf', 'http://example.com/2.pdf'],
            'Report Html Address': ['', ''],
            'Year': [2019, 2021]
        }
        download_queue = pd.DataFrame(new_data, index=['12345', 'ABCDE'])
        reports_data = download_queue.copy()
        
        # Create a mock successful download for the first file to change its status
        file_path = os.path.join(self.test_download_dir, "12345.pdf")
        with open(file_path, 'w') as f:
            f.write('test content')
        
        # Call the function
        self.downloader.update_metadata(download_queue, reports_data)
        
        # Read the updated metadata file
        updated_df = pd.read_excel(self.downloader.metadata_path)
        
        print("Metadata DataFrame columns:", updated_df.columns.tolist())
        print("Metadata DataFrame content:")
        print(updated_df)

        # We should have 3 entries total (the two original ones, with one updated, plus the new one)
        self.assertEqual(len(updated_df), 3)

        # Check the updated status for the duplicate record
        updated_record = updated_df[updated_df[self.downloader.id_column] == '12345']
        self.assertEqual(updated_record['pdf_downloaded'].values[0], 'Yes')

    @patch('pandas.read_excel')
    def test_run_file_not_found(self, mock_read_excel):
        """Test error handling in run() when reports file is not found"""
        # Setup mock to raise FileNotFoundError
        mock_read_excel.side_effect = FileNotFoundError("File not found")
        
        # Call the function
        self.downloader.run()
        
        # There's no easy way to assert console output, but we can verify
        # the mock was called with correct arguments
        mock_read_excel.assert_called_once()

    @patch('pandas.read_excel')
    def test_run_general_exception(self, mock_read_excel):
        """Test error handling in run() for other exceptions"""
        # Setup mock to raise a generic exception
        mock_read_excel.side_effect = Exception("Some other error")
        
        # Call the function
        self.downloader.run()
        
        # Verify the mock was called
        mock_read_excel.assert_called_once()

    def test_global_helper_functions(self):
        """Test that global helper functions correctly create and call PDF_Downloader methods"""
        # Setup mock methods
        with patch('PDF_Downloader.PDF_Downloader.get_existing_downloads') as mock_get:
            with patch('PDF_Downloader.PDF_Downloader.download_file') as mock_download_file:
                with patch('PDF_Downloader.PDF_Downloader.download_pdfs') as mock_download_pdfs:
                    with patch('PDF_Downloader.PDF_Downloader.create_output_report') as mock_output:
                        with patch('PDF_Downloader.PDF_Downloader.update_metadata') as mock_metadata:
                            with patch('PDF_Downloader.PDF_Downloader.upload_to_drive') as mock_upload:
                                # Call global helper functions
                                from PDF_Downloader import (
                                    get_existing_downloads, download_file, download_pdfs,
                                    create_output_report, update_metadata, upload_to_drive
                                )
                                
                                # Test each helper function
                                get_existing_downloads()
                                mock_get.assert_called_once()
                                
                                # Test download_file with empty data
                                download_file("12345", pd.Series(), [])
                                mock_download_file.assert_called_once()

                                # Test download_pdfs with empty data
                                download_pdfs(pd.DataFrame(), [])
                                mock_download_pdfs.assert_called_once()

                                # Test create_output_report with empty data
                                create_output_report(pd.DataFrame(), [])
                                mock_output.assert_called_once()
                                
                                # Test update_metadata with empty data
                                update_metadata(pd.DataFrame(), pd.DataFrame())
                                mock_metadata.assert_called_once()
                                
                                # Test upload_to_drive
                                upload_to_drive()
                                mock_upload.assert_called_once()

    @patch('PDF_Downloader.PDF_Downloader.run')
    def test_main_function(self, mock_run):
        """Test main function calls run method"""
        from PDF_Downloader import main
        
        # Call main
        main()
        
        # Verify run was called
        mock_run.assert_called_once()

if __name__ == '__main__':
    # Run tests but prevent unittest from exiting the program
    result = unittest.main(exit=False, verbosity=2)
    
    # Stop coverage and generate report
    cov.stop()
    cov.save()
    print("\n\nCoverage Report:")
    cov.report()
    
    # Generate HTML report
    html_dir = os.path.join(os.path.dirname(__file__), 'coverage_html')
    cov.html_report(directory=html_dir)
    print(f"\nHTML coverage report generated in: {html_dir}")
    
    # Now exit with proper status code
    sys.exit(not result.result.wasSuccessful())