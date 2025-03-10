"""
Utilities for working with different file types in the S3 data lake.

This module provides the S3FileUtils class with helper methods for handling various file formats
like CSV, JSON, YAML, Excel, PDF, etc. when working with the data lake.
"""

import os
import io
import json
import csv
import logging
import pandas as pd
import yaml
from pathlib import Path
from typing import Dict, List, Union, Any, Optional, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class S3FileUtils:
    """
    Utilities for working with different file types in the S3 data lake.
    
    This class provides helper methods for handling various file formats like CSV, JSON, 
    YAML, Excel, PDF, etc. when working with the data lake.
    """
    
    def __init__(self, s3_data_lake):
        """
        Initialize the file utilities with a reference to the S3 data lake.
        
        Args:
            s3_data_lake: Instance of S3DataLake class
        """
        self.s3_data_lake = s3_data_lake
        self.temp_dir = os.path.join(os.getcwd(), 'temp')
        os.makedirs(self.temp_dir, exist_ok=True)
    
    def _get_file_extension(self, file_path: str) -> str:
        """
        Get file extension from path.
        
        Args:
            file_path: Path to the file
            
        Returns:
            str: File extension (lowercase) including the dot
        """
        return os.path.splitext(file_path)[1].lower()
    
    def download_and_parse(self, s3_key: str, local_dir: Optional[str] = None) -> Tuple[Any, Optional[str]]:
        """
        Download a file from S3 and parse it based on its extension.
        
        Args:
            s3_key (str): Full S3 key of the file
            local_dir (str, optional): Local directory to save the file to. Defaults to self.temp_dir.
        
        Returns:
            tuple: (parsed_content, mime_type) or (None, None) if error
        """
        if local_dir is None:
            local_dir = self.temp_dir
            
        try:
            # Create temp directory if it doesn't exist
            os.makedirs(local_dir, exist_ok=True)
            
            # Generate local path
            local_path = os.path.join(local_dir, os.path.basename(s3_key))
            
            # Download the file
            if not self.s3_data_lake.download_file(s3_key, local_path):
                return None, None
            
            # Parse the file based on extension
            ext = self._get_file_extension(local_path)
            
            if ext in ['.csv', '.tsv']:
                content = self._parse_csv(local_path)
                mime_type = 'text/csv'
            elif ext == '.json':
                content = self._parse_json(local_path)
                mime_type = 'application/json'
            elif ext in ['.yaml', '.yml']:
                content = self._parse_yaml(local_path)
                mime_type = 'application/yaml'
            elif ext in ['.xlsx', '.xls']:
                content = self._parse_excel(local_path)
                mime_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            elif ext == '.txt':
                content = self._parse_text(local_path)
                mime_type = 'text/plain'
            elif ext == '.pdf':
                # This would require additional libraries like PyPDF2 or pdfplumber
                content = None
                mime_type = 'application/pdf'
                logger.warning("PDF parsing not implemented. Install additional libraries for PDF support.")
            else:
                content = None
                mime_type = 'application/octet-stream'
                logger.warning(f"No parser implemented for extension {ext}")
            
            # Clean up - remove the temporary file
            os.remove(local_path)
            
            return content, mime_type
        
        except Exception as e:
            logger.error(f"Error parsing file {s3_key}: {str(e)}")
            return None, None
    
    def _parse_csv(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Parse CSV file.
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            List[Dict[str, Any]]: List of records as dictionaries
        """
        try:
            return pd.read_csv(file_path).to_dict('records')
        except Exception:
            # Try with different encodings and delimiters if the default fails
            try:
                return pd.read_csv(file_path, encoding='latin1').to_dict('records')
            except Exception as e:
                logger.error(f"Error parsing CSV {file_path}: {str(e)}")
                
                # Fall back to simple csv reader
                with open(file_path, 'r', newline='', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    return list(reader)
    
    def _parse_json(self, file_path: str) -> Any:
        """
        Parse JSON file.
        
        Args:
            file_path: Path to the JSON file
            
        Returns:
            Any: Parsed JSON content (dict, list, etc.)
        """
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def _parse_yaml(self, file_path: str) -> Any:
        """
        Parse YAML file.
        
        Args:
            file_path: Path to the YAML file
            
        Returns:
            Any: Parsed YAML content
        """
        with open(file_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    def _parse_excel(self, file_path: str) -> Dict[str, List[Dict[str, Any]]]:
        """
        Parse Excel file.
        
        Args:
            file_path: Path to the Excel file
            
        Returns:
            Dict[str, List[Dict[str, Any]]]: Dictionary mapping sheet names to lists of records
        """
        # Read all sheets into a dict
        xl = pd.ExcelFile(file_path)
        result = {}
        
        for sheet_name in xl.sheet_names:
            result[sheet_name] = pd.read_excel(file_path, sheet_name=sheet_name).to_dict('records')
        
        return result
    
    def _parse_text(self, file_path: str) -> str:
        """
        Parse plain text file.
        
        Args:
            file_path: Path to the text file
            
        Returns:
            str: Text content
        """
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    
    def save_and_upload(self, content: Any, file_name: str, zone: str = 'processed', 
                        s3_path: Optional[str] = None, metadata: Optional[Dict[str, str]] = None) -> Optional[str]:
        """
        Save content to a file and upload it to S3.
        
        Args:
            content: Content to save
            file_name (str): Name for the file
            zone (str, optional): Data lake zone. Defaults to 'processed'.
            s3_path (str, optional): Custom path in S3. Defaults to file_name.
            metadata (dict, optional): Metadata for the file. Defaults to None.
        
        Returns:
            str: S3 key of the uploaded file or None if error
        """
        try:
            # Generate local path
            local_path = os.path.join(self.temp_dir, file_name)
            
            # Save content based on file extension
            ext = self._get_file_extension(file_name)
            
            if ext in ['.csv', '.tsv']:
                self._save_csv(content, local_path)
            elif ext == '.json':
                self._save_json(content, local_path)
            elif ext in ['.yaml', '.yml']:
                self._save_yaml(content, local_path)
            elif ext in ['.xlsx', '.xls']:
                self._save_excel(content, local_path)
            elif ext == '.txt':
                self._save_text(content, local_path)
            else:
                logger.warning(f"No saver implemented for extension {ext}")
                return None
            
            # Upload the file
            s3_file_path = s3_path or file_name
            success = self.s3_data_lake.upload_file(local_path, zone, s3_file_path, metadata)
            
            # Clean up - remove the temporary file
            os.remove(local_path)
            
            if success:
                return f"{zone}/{s3_file_path}"
            else:
                return None
        
        except Exception as e:
            logger.error(f"Error saving and uploading file {file_name}: {str(e)}")
            return None
    
    def _save_csv(self, content: Union[List[Dict[str, Any]], pd.DataFrame], file_path: str) -> None:
        """
        Save content as CSV file.
        
        Args:
            content: Data to save (list of dictionaries or pandas DataFrame)
            file_path: Path to save the CSV file
        """
        if isinstance(content, list) and content and isinstance(content[0], dict):
            pd.DataFrame(content).to_csv(file_path, index=False)
        elif isinstance(content, pd.DataFrame):
            content.to_csv(file_path, index=False)
        else:
            raise ValueError("Content must be a list of dictionaries or a pandas DataFrame")
    
    def _save_json(self, content: Any, file_path: str) -> None:
        """
        Save content as JSON file.
        
        Args:
            content: Data to save
            file_path: Path to save the JSON file
        """
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(content, f, indent=4, ensure_ascii=False)
    
    def _save_yaml(self, content: Any, file_path: str) -> None:
        """
        Save content as YAML file.
        
        Args:
            content: Data to save
            file_path: Path to save the YAML file
        """
        with open(file_path, 'w', encoding='utf-8') as f:
            yaml.dump(content, f, default_flow_style=False)
    
    def _save_excel(self, content: Union[Dict[str, List[Dict[str, Any]]], List[Dict[str, Any]], pd.DataFrame], 
                   file_path: str) -> None:
        """
        Save content as Excel file.
        
        Args:
            content: Data to save (dict of sheet_name -> data, list of dictionaries, or pandas DataFrame)
            file_path: Path to save the Excel file
        """
        if isinstance(content, dict):
            # If it's a dict of sheet_name -> data, save multiple sheets
            with pd.ExcelWriter(file_path) as writer:
                for sheet_name, sheet_data in content.items():
                    if isinstance(sheet_data, pd.DataFrame):
                        sheet_data.to_excel(writer, sheet_name=sheet_name, index=False)
                    else:
                        pd.DataFrame(sheet_data).to_excel(writer, sheet_name=sheet_name, index=False)
        elif isinstance(content, list) and content and isinstance(content[0], dict):
            # If it's a list of dictionaries, save as a single sheet
            pd.DataFrame(content).to_excel(file_path, index=False)
        elif isinstance(content, pd.DataFrame):
            # If it's a pandas DataFrame, save as a single sheet
            content.to_excel(file_path, index=False)
        else:
            raise ValueError("Content must be a dictionary of sheet names to data, a list of dictionaries, or a pandas DataFrame")
    
    def _save_text(self, content: str, file_path: str) -> None:
        """
        Save content as text file.
        
        Args:
            content: Text to save
            file_path: Path to save the text file
        """
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
    
    def get_file_content_type(self, file_path: str) -> str:
        """
        Determine the content type (MIME type) from a file extension.
        
        Args:
            file_path: Path to the file
            
        Returns:
            str: MIME type
        """
        ext = self._get_file_extension(file_path)
        
        # Map of extensions to MIME types
        content_types = {
            '.csv': 'text/csv',
            '.tsv': 'text/tab-separated-values',
            '.json': 'application/json',
            '.yaml': 'application/yaml',
            '.yml': 'application/yaml',
            '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            '.xls': 'application/vnd.ms-excel',
            '.txt': 'text/plain',
            '.pdf': 'application/pdf',
            '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            '.doc': 'application/msword',
            '.pptx': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
            '.ppt': 'application/vnd.ms-powerpoint',
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg',
            '.png': 'image/png',
            '.gif': 'image/gif',
            '.mp3': 'audio/mpeg',
            '.mp4': 'video/mp4',
            '.wav': 'audio/wav',
            '.html': 'text/html',
            '.htm': 'text/html',
            '.xml': 'application/xml',
            '.zip': 'application/zip'
        }
        
        return content_types.get(ext, 'application/octet-stream')
    
    def extract_metadata(self, file_path: str) -> Dict[str, Any]:
        """
        Extract metadata from a file.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Dict[str, Any]: Metadata dictionary
        """
        try:
            # Basic metadata
            stats = os.stat(file_path)
            metadata = {
                'filename': os.path.basename(file_path),
                'size_bytes': stats.st_size,
                'created_time': stats.st_ctime,
                'modified_time': stats.st_mtime,
                'content_type': self.get_file_content_type(file_path)
            }
            
            # Extension-specific metadata
            ext = self._get_file_extension(file_path)
            
            # Extract more metadata based on file type
            if ext in ['.csv', '.tsv']:
                try:
                    df = pd.read_csv(file_path, nrows=10)  # Read just a sample
                    metadata.update({
                        'columns': df.columns.tolist(),
                        'row_count_estimate': sum(1 for _ in open(file_path)) - 1  # Approximate count
                    })
                except Exception as e:
                    logger.warning(f"Could not extract CSV metadata: {str(e)}")
                    
            elif ext in ['.xlsx', '.xls']:
                try:
                    xl = pd.ExcelFile(file_path)
                    metadata.update({
                        'sheets': xl.sheet_names
                    })
                except Exception as e:
                    logger.warning(f"Could not extract Excel metadata: {str(e)}")
                    
            # Add more file type specific metadata extraction as needed
            
            return metadata
            
        except Exception as e:
            logger.error(f"Error extracting metadata for {file_path}: {str(e)}")
            return {'filename': os.path.basename(file_path)}
    
    def batch_process_files(self, file_paths: List[str], target_zone: str = 'processed', 
                           process_func=None) -> Dict[str, str]:
        """
        Process multiple files and upload them to the data lake.
        
        Args:
            file_paths: List of file paths to process
            target_zone: Data lake zone to upload to
            process_func: Function to apply to each file's content before uploading
                         If None, files are uploaded as-is
        
        Returns:
            Dict[str, str]: Dictionary mapping input file paths to their S3 keys
        """
        results = {}
        
        for file_path in file_paths:
            try:
                filename = os.path.basename(file_path)
                
                if process_func is not None:
                    # Parse the file
                    ext = self._get_file_extension(file_path)
                    
                    if ext in ['.csv', '.tsv']:
                        content = self._parse_csv(file_path)
                    elif ext == '.json':
                        content = self._parse_json(file_path)
                    elif ext in ['.yaml', '.yml']:
                        content = self._parse_yaml(file_path)
                    elif ext in ['.xlsx', '.xls']:
                        content = self._parse_excel(file_path)
                    elif ext == '.txt':
                        content = self._parse_text(file_path)
                    else:
                        logger.warning(f"Skipping {file_path} - unsupported extension {ext}")
                        continue
                    
                    # Process the content
                    processed_content = process_func(content)
                    
                    # Save and upload
                    s3_key = self.save_and_upload(
                        processed_content, 
                        filename, 
                        target_zone, 
                        metadata=self.extract_metadata(file_path)
                    )
                else:
                    # Upload the file directly
                    if self.s3_data_lake.upload_file(
                        file_path, 
                        target_zone, 
                        metadata=self.extract_metadata(file_path)
                    ):
                        s3_key = f"{target_zone}/{filename}"
                    else:
                        s3_key = None
                
                if s3_key:
                    results[file_path] = s3_key
                    logger.info(f"Processed and uploaded {file_path} to {s3_key}")
                else:
                    logger.error(f"Failed to process and upload {file_path}")
                
            except Exception as e:
                logger.error(f"Error processing file {file_path}: {str(e)}")
        
        return results