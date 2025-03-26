import pandas as pd
import glob
import os
import os.path
import threading
import requests
from requests.packages import urllib3  
from time import sleep
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive


urllib3.disable_warnings(category=urllib3.exceptions.InsecureRequestWarning)

# The name of the column that contains the unique ID for each report
ID_COLUMN = 'BRnum'

# Maximum number of PDF files to download in a single run
MAX_DOWNLOADS = 10

# Maximum number of files to download at the same time
MAX_CONCURRENT_THREADS = 5

# Main data directory
DATA_DIR = 'Data'

# Excel file containing report URLs and information
REPORTS_PATH = os.path.join(DATA_DIR, 'GRI_2017_2020 (1).xlsx')

# Excel file for tracking which reports have been downloaded
METADATA_PATH = os.path.join(DATA_DIR, 'Metadata2006_2016.xlsx')

# Directory where PDF files will be saved
DOWNLOAD_DIR = os.path.join(DATA_DIR, 'Downloads')

# Directory for output reports and logs
OUTPUT_DIR = os.path.join(DATA_DIR, 'Output')


class PDF_Downloader:
    """
    A class to handle the downloading of PDF files from URLs.
    """
    def __init__(self):
        # Initialize the class with default settings
        self.id_column = ID_COLUMN
        self.max_downloads = MAX_DOWNLOADS
        self.max_concurrent_threads = MAX_CONCURRENT_THREADS
        self.data_dir = DATA_DIR
        self.reports_path = REPORTS_PATH
        self.metadata_path = METADATA_PATH
        self.download_dir = DOWNLOAD_DIR
        self.output_dir = OUTPUT_DIR
        
    def get_existing_downloads(self):
        """
        Check which PDF files have already been downloaded.
        
        Returns:
            list: IDs of PDF files that already exist in the download folder
        """
        # Get list of all PDF files in the download directory
        downloaded_files = glob.glob(os.path.join(self.download_dir, "*.pdf")) 
        
        # Extract just the ID portion from each filename (removing .pdf extension)
        existing_ids = [os.path.basename(f)[:-4] for f in downloaded_files]
        
        return existing_ids
        
    def download_file(self, index, row, download_errors):
        """Implementation of download_file method"""
        # Original implementation moved to class method
        success = False
        try:
            # Figure out which URL to use - prefer PDF_URL if available
            if pd.notna(row['Pdf_URL']):
                url = row['Pdf_URL']
                print(f"Downloading {index} from PDF URL...")
            else:
                url = row['Report Html Address']
                print(f"Downloading {index} from HTML URL...")
            
            # Download the file content
            # verify=False skips SSL certificate validation
            response = requests.get(url, verify=False, timeout=30)
            
            # Check if the download was successful
            response.raise_for_status()
            
            # Save the content to a PDF file
            file_path = os.path.join(self.download_dir, f"{index}.pdf")
            with open(file_path, 'wb') as f:
                f.write(response.content)
            
            success = True
        except requests.exceptions.RequestException as e:
            # Handle network or URL errors
            error_message = f"Network error: {e}"
            download_errors.append(index)
            download_errors.append(error_message)
            print(f"Error downloading {index}: {error_message}")
        except Exception as e:
            # Handle any other errors
            error_message = f"Unexpected error: {e}"
            download_errors.append(index)
            download_errors.append(error_message)
            print(f"Error downloading {index}: {error_message}")
        finally:
            # Report whether the download succeeded or failed
            if success:
                print(f"✓ Successfully downloaded {index}")
            else:
                print(f"✗ Failed to download {index}")
                
    def download_pdfs(self, download_queue, download_errors):
        """Moved download_pdfs to class method"""
        # Create directories if they don't exist
        os.makedirs(self.download_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Show how many files we'll be downloading
        total_files = len(download_queue)
        print(f"\nStarting download of {total_files} files\n")
        
        # Create and start threads for each download
        threads = []
        files_started = 0
        
        for index, row in download_queue.iterrows():
            # Create a thread for this download
            thread = threading.Thread(
                target=self.download_file,
                args=(index, row, download_errors),
                name=f"Download-{index}"
            )
            threads.append(thread)
            
            # Start the thread
            thread.start()
            files_started += 1
            
            # Show progress
            print(f"Started download {files_started}/{total_files} ({index})")
            
            # If we've reached the max concurrent downloads, wait for one to finish
            # before starting more downloads
            while sum(1 for t in threads if t.is_alive()) >= self.max_concurrent_threads:
                print(f"Reached max concurrent downloads ({self.max_concurrent_threads}). Waiting...")
                sleep(0.5)  # Wait 0.5 second before checking again
        
        # Wait for all threads to complete
        active_threads = sum(1 for t in threads if t.is_alive())
        while active_threads > 0:
            print(f"Waiting for {active_threads} downloads to complete...")
            sleep(1)  # Check every 2 seconds
            active_threads = sum(1 for t in threads if t.is_alive())
        
        # Final completion message
        print("\nAll downloads finished\n")
        
    def create_output_report(self, download_queue, download_errors):
        """Moved create_output_report to class method"""
        # Similar implementation as before but using self.xxx for paths
        # ...existing implementation changed to use class variables...
        print("Creating download status report...")
        
        output = []
        for index, row in download_queue.iterrows():
            # Check if this file was downloaded successfully
            if os.path.exists(os.path.join(self.download_dir, f"{index}.pdf")):
                output.append([index, "Downloaded", ""])
            else:
                # Find the error message if there was one
                error_msg = ""
                if index in download_errors:
                    error_idx = download_errors.index(index)
                    if error_idx + 1 < len(download_errors):
                        error_msg = str(download_errors[error_idx + 1])
                else:
                    error_msg = "File not found"
                output.append([index, "Failed", error_msg])

        # Create a DataFrame from the output data
        new_output_df = pd.DataFrame(output, columns=["Brnum", "Status", "Error Message"])
        
        # Define the output file path
        output_path = os.path.join(self.output_dir, "Download_Status.xlsx")
        
        # Check if the file already exists
        if os.path.exists(output_path):
            print(f"Appending to existing download status report: {output_path}")
            # Read the existing file
            try:
                existing_df = pd.read_excel(output_path)
                # Combine existing data with new data
                output_df = pd.concat([existing_df, new_output_df], ignore_index=True)
                # Remove duplicates, keeping the latest entry if there's a conflict
                output_df.drop_duplicates(subset=["Brnum"], keep="last", inplace=True)
            except Exception as e:
                print(f"Error reading existing report file: {e}")
                print("Creating a new report file instead.")
                output_df = new_output_df
        else:
            print("Creating new download status report.")
            output_df = new_output_df
        
        # Save to an Excel file
        output_df.to_excel(output_path, index=False)
        print(f'Download status report saved to: {output_path}')
    
    def update_metadata(self, download_queue, reports_data):
        """Moved update_metadata to class method"""
        # Similar implementation as before but using self.xxx for paths
        # ...existing implementation changed to use class variables...
        print("Updating metadata file...")
        
        # Check if metadata file exists
        if not os.path.exists(self.metadata_path):
            print(f"Creating new metadata file (not found at {self.metadata_path})")
            metadata_df = pd.DataFrame(columns=[self.id_column, 'pdf_downloaded'])
        else:
            # Load existing metadata
            metadata_df = pd.read_excel(self.metadata_path, sheet_name=0)
            print(f"Loaded existing metadata with {len(metadata_df)} entries")

        # Get list of successfully downloaded files
        downloaded_files = self.get_existing_downloads()
        print(f"Found {len(downloaded_files)} downloaded PDF files")

        # Create a list for new metadata records
        new_records = []

        # For each file we attempted to download, create a new record
        for report_id in download_queue.index:
            # Convert ID to string for consistent comparison
            status = 'Yes' if str(report_id) in downloaded_files else 'No'
            
            # Create a record with the same columns as the metadata file
            new_record = {self.id_column: report_id, 'pdf_downloaded': status}
            
            # Copy any other columns from the source file that we want to preserve
            if report_id in reports_data.index:
                for col in reports_data.columns:
                    # Only copy columns that exist in the metadata file and haven't been set yet
                    if col in metadata_df.columns and col not in [self.id_column, 'pdf_downloaded']:
                        new_record[col] = reports_data.loc[report_id, col]
        
            new_records.append(new_record)

        # Create DataFrame from new records
        new_data = pd.DataFrame(new_records)
        print(f"Created {len(new_data)} new metadata entries")

        # Make a backup of the original metadata
        backup_path = os.path.join(self.output_dir, "Metadata2006_2016_Backup.xlsx")
        metadata_df.to_excel(backup_path, index=False)
        print(f"Saved metadata backup to: {backup_path}")

        # Append the new data to the existing metadata
        updated_metadata = pd.concat([metadata_df, new_data], ignore_index=True)

        # Remove duplicates if any, keeping the latest entry
        before_dedup = len(updated_metadata)
        updated_metadata.drop_duplicates(subset=[self.id_column], keep='last', inplace=True)
        after_dedup = len(updated_metadata)
        
        if before_dedup != after_dedup:
            print(f"Removed {before_dedup - after_dedup} duplicate entries")

        # Update the original metadata file
        updated_metadata.to_excel(self.metadata_path, index=False)
        print(f"Saved updated metadata with {len(updated_metadata)} entries to: {self.metadata_path}")
    
    def upload_to_drive(self):
        """Moved upload_to_drive to class method"""
        print("\nStarting Google Drive Upload\n")
        
        # Check if client_secrets.json exists
        if not os.path.exists("client_secrets.json"):
            print("ERROR: client_secrets.json not found in the project directory.")
            print("Please download your OAuth credentials from the Google Cloud Console")
            print("and save them as client_secrets.json in this directory.")
            return False
        
        try:
            # Authenticate with Google Drive
            gauth = GoogleAuth()
            
            # Try to load saved credentials
            gauth.LoadCredentialsFile("credentials.json")
            
            if gauth.credentials is None:
                # No credentials available, need to authenticate
                print("No stored credentials found. Starting authentication flow...")
                print("A browser window will open for you to authorize access.")
                gauth.LocalWebserverAuth()
            elif gauth.access_token_expired:
                # Credentials exist but are expired
                print("Credentials expired. Refreshing...")
                gauth.Refresh()
            else:
                # Credentials exist and are valid
                print("Using existing credentials")
                gauth.Authorize()
                
            # Save the current credentials
            gauth.SaveCredentialsFile("credentials.json")
            
            drive = GoogleDrive(gauth)
            
            # Get list of all PDF files in the download directory
            downloaded_files = glob.glob(os.path.join(self.download_dir, "*.pdf"))
            
            if not downloaded_files:
                print("No PDF files found to upload.")
                return True
                
            print(f"Found {len(downloaded_files)} PDF files to upload.")
            
            # Create a folder for our uploads if it doesn't exist
            folder_name = "PDF-Downloader-Uploads"
            folder_id = None
            
            # Check if our folder already exists
            file_list = drive.ListFile({'q': f"title='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"}).GetList()
            
            # Use the first folder found (if any)
            if file_list:
                folder_id = file_list[0]['id']
                print(f"Using existing folder: {folder_name}")
            else:
                # Create the folder
                folder_metadata = {
                    'title': folder_name,
                    'mimeType': 'application/vnd.google-apps.folder'
                }
                folder = drive.CreateFile(folder_metadata)
                folder.Upload()
                folder_id = folder['id']
                folder.InsertPermission({
                    'type': 'anyone',
                    'value': 'anyone',
                    'role': 'reader'
                })
                print(f"Created new folder: {folder_name}")
            
            # Upload each file to Google Drive
            successful_uploads = 0
            for file_path in downloaded_files:
                file_name = os.path.basename(file_path)
                
                try:
                    # Check if file already exists
                    existing_files = drive.ListFile({'q': f"title='{file_name}' and '{folder_id}' in parents and trashed=false"}).GetList()
                    
                    if existing_files:
                        print(f"File {file_name} already exists in Google Drive. Skipping.")
                        successful_uploads += 1
                        continue
                    
                    # Create a file on Google Drive
                    drive_file = drive.CreateFile({
                        'title': file_name,
                        'parents': [{'id': folder_id}]
                    })
                    
                    # Set the content
                    drive_file.SetContentFile(file_path)
                    
                    # Upload the file
                    drive_file.Upload()
                    
                    # Report the status
                    print(f"✓ Uploaded {file_name} to Google Drive")
                   
                    successful_uploads += 1
                
                except Exception as e:
                    print(f"✗ Error uploading {file_name}: {str(e)}")

            # Print the folder link and upload status      
            print(f"\nUploaded {successful_uploads} of {len(downloaded_files)} files to Google Drive")
            print(f"\nFolder: https://drive.google.com/drive/folders/{folder_id}")
            return True
                
        except Exception as e:
            print(f"Error during Google Drive upload: {str(e)}")
            print("Make sure client_secrets.json exists in the project directory.")
            return False
    
    def run(self):
        """Main method to orchestrate the PDF download process."""
        # Read the Excel file
        try:
            print(f"\nReading reports data from {self.reports_path}...")
            reports_data = pd.read_excel(self.reports_path, sheet_name=0, index_col=self.id_column)
            print(f"   Found {len(reports_data)} reports in the file")
        except FileNotFoundError:
            print(f"ERROR: Reports file not found at {self.reports_path}")
            print("Please make sure the file exists and try again.")
            return
        except Exception as e:
            print(f"ERROR: Could not read reports file: {e}")
            return
        
        # Keep only rows with valid download URLs
        print("\nFinding reports with valid download URLs...")
        has_valid_url = (reports_data.Pdf_URL.notnull()) | (reports_data['Report Html Address'].notnull())
        reports_data = reports_data[has_valid_url]
        print(f"   Found {len(reports_data)} reports with valid URLs")
        
        # Make a copy for download processing
        download_queue = reports_data.copy()

        # Check which files have already been downloaded
        print("\nChecking for previously downloaded reports...")
        existing_downloads = self.get_existing_downloads()
        print(f"   Found {len(existing_downloads)} already downloaded PDFs")
        
        # Remove files that have already been downloaded
        to_download = [idx for idx in download_queue.index if str(idx) not in existing_downloads]
        download_queue = download_queue.loc[to_download]
        print(f"   {len(download_queue)} reports need to be downloaded")

        # Limit batch size to prevent overloading
        if len(download_queue) > self.max_downloads:
            print(f"\nLimiting to {self.max_downloads} downloads this run (from {len(download_queue)} available)")
            download_queue = download_queue.head(self.max_downloads)
        else:
            print(f"\nWill download all {len(download_queue)} reports")

        # List to track any errors that occur during downloading
        download_errors = []

        # Download the PDFs
        if len(download_queue) > 0:
            self.download_pdfs(download_queue, download_errors)
        else:
            print("\nNo new reports to download.")
        
        # Generate reports
        print("Creating reports...")
        self.create_output_report(download_queue, download_errors)
        self.update_metadata(download_queue, reports_data)

        # Upload to Google Drive
        self.upload_to_drive()

        print("\nProgram completed successfully.")


# Keep the original helper functions for backward compatibility
def get_existing_downloads():
    downloader = PDF_Downloader()
    return downloader.get_existing_downloads()

def download_file(index, row, download_errors):
    downloader = PDF_Downloader()
    return downloader.download_file(index, row, download_errors)

def download_pdfs(download_queue, download_errors):
    downloader = PDF_Downloader()
    return downloader.download_pdfs(download_queue, download_errors)

def create_output_report(download_queue, download_errors):
    downloader = PDF_Downloader()
    return downloader.create_output_report(download_queue, download_errors)

def update_metadata(download_queue, reports_data):
    downloader = PDF_Downloader()
    return downloader.update_metadata(download_queue, reports_data)

def upload_to_drive():
    downloader = PDF_Downloader()
    return downloader.upload_to_drive()

# MAIN PROGRAM
def main():
    downloader = PDF_Downloader()
    downloader.run()

# Run the program if this file is executed directly
if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"\nERROR: An unexpected error occurred: {e}")
        print("The program will now exit.")