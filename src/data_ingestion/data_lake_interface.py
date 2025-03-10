"""
Main interface for the S3 Data Lake functionality.

This module provides a unified interface for working with the S3 data lake,
combining the core S3 infrastructure, configuration management, and file utilities.
"""

import os
import logging
import pandas as pd
from typing import Dict, List, Any, Optional, Union, Tuple, Callable
import uuid
from datetime import datetime

from src.data_ingestion.data_lake import S3DataLake
from src.data_ingestion.file_utils import S3FileUtils
from config.s3_config import S3DataLakeConfig

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataLakeInterface:
    """
    Main interface for interacting with the S3 data lake.
    
    This class provides a unified interface combining the data lake infrastructure,
    configuration management, and file utilities.
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the data lake interface.
        
        Args:
            config_path (str, optional): Path to the configuration file.
                                         If None, uses the default path.
        """
        # Load configuration
        self.config = S3DataLakeConfig(config_path)
        
        # Validate configuration
        valid, message = self.config.validate_config()
        if not valid:
            raise ValueError(f"Invalid configuration: {message}")
        
        # Initialize S3 data lake
        self.data_lake = S3DataLake(
            bucket_name=self.config.get_bucket_name(),
            region_name=self.config.get_region_name(),
            profile_name=self.config.get_profile_name()
        )
        
        # Initialize file utilities
        self.file_utils = S3FileUtils(self.data_lake)
        
        logger.info(f"Initialized data lake interface with bucket {self.config.get_bucket_name()}")
    
    def upload_file(self, local_file_path: str, zone: str = 'raw', 
                   s3_file_path: Optional[str] = None, metadata: Optional[Dict[str, str]] = None) -> Optional[str]:
        """
        Upload a file to the data lake.
        
        Args:
            local_file_path (str): Path to the local file
            zone (str, optional): Data lake zone. Defaults to 'raw'.
            s3_file_path (str, optional): Custom path in the zone. Defaults to filename.
            metadata (dict, optional): Metadata for the file. Defaults to None.
        
        Returns:
            str: S3 key of the uploaded file or None if error
        """
        if metadata is None:
            # Auto-extract metadata if not provided
            metadata = self.file_utils.extract_metadata(local_file_path)
        
        if self.data_lake.upload_file(local_file_path, zone, s3_file_path, metadata):
            if s3_file_path is None:
                s3_file_path = os.path.basename(local_file_path)
            return f"{zone}/{s3_file_path}"
        else:
            return None
    
    def download_file(self, s3_key: str, local_path: str) -> bool:
        """
        Download a file from the data lake.
        
        Args:
            s3_key (str): Full S3 key of the file to download
            local_path (str): Local path to save the file to
        
        Returns:
            bool: True if download was successful, False otherwise
        """
        return self.data_lake.download_file(s3_key, local_path)
    
    def list_files(self, zone: Optional[str] = None, prefix: Optional[str] = None, 
                  recursive: bool = True) -> List[Dict[str, Any]]:
        """
        List files in the data lake.
        
        Args:
            zone (str, optional): Data lake zone to list. Defaults to None (all zones).
            prefix (str, optional): Additional prefix filter. Defaults to None.
            recursive (bool, optional): Whether to list files recursively. Defaults to True.
        
        Returns:
            list: List of file information dictionaries
        """
        return self.data_lake.list_files(zone, prefix, recursive)
    
    def delete_file(self, s3_key: str) -> bool:
        """
        Delete a file from the data lake.
        
        Args:
            s3_key (str): Full S3 key of the file to delete
        
        Returns:
            bool: True if deletion was successful, False otherwise
        """
        return self.data_lake.delete_file(s3_key)
    
    def move_file(self, source_key: str, target_zone: str, target_path: Optional[str] = None) -> bool:
        """
        Move a file from one zone to another in the data lake.
        
        Args:
            source_key (str): Full S3 key of the source file
            target_zone (str): Target data lake zone
            target_path (str, optional): Custom path in the target zone. Defaults to original filename.
        
        Returns:
            bool: True if move was successful, False otherwise
        """
        return self.data_lake.move_file(source_key, target_zone, target_path)
    
    def get_file_metadata(self, s3_key: str) -> Optional[Dict[str, Any]]:
        """
        Get metadata for a file in the data lake.
        
        Args:
            s3_key (str): Full S3 key of the file
        
        Returns:
            dict: File metadata or None if error
        """
        return self.data_lake.get_file_metadata(s3_key)
    
    def update_file_metadata(self, s3_key: str, metadata: Dict[str, str]) -> bool:
        """
        Update metadata for a file in the data lake.
        
        Args:
            s3_key (str): Full S3 key of the file
            metadata (dict): New metadata to set
        
        Returns:
            bool: True if update was successful, False otherwise
        """
        return self.data_lake.update_file_metadata(s3_key, metadata)
    
    def parse_file(self, s3_key: str) -> Tuple[Any, Optional[str]]:
        """
        Download and parse a file from the data lake.
        
        Args:
            s3_key (str): Full S3 key of the file
        
        Returns:
            tuple: (parsed_content, mime_type) or (None, None) if error
        """
        return self.file_utils.download_and_parse(s3_key)
    
    def save_and_upload(self, content: Any, file_name: str, zone: str = 'processed',
                        s3_path: Optional[str] = None, metadata: Optional[Dict[str, str]] = None) -> Optional[str]:
        """
        Save content to a file and upload it to the data lake.
        
        Args:
            content: Content to save
            file_name (str): Name for the file
            zone (str, optional): Data lake zone. Defaults to 'processed'.
            s3_path (str, optional): Custom path in S3. Defaults to file_name.
            metadata (dict, optional): Metadata for the file. Defaults to None.
        
        Returns:
            str: S3 key of the uploaded file or None if error
        """
        return self.file_utils.save_and_upload(content, file_name, zone, s3_path, metadata)
    
    def process_data_pipeline(self, input_files: List[str], process_funcs: List[Callable]) -> List[str]:
        """
        Process data through a pipeline of functions and store results in the data lake.
        
        Args:
            input_files (List[str]): List of local file paths or S3 keys to process
            process_funcs (List[callable]): List of processing functions to apply sequentially
        
        Returns:
            List[str]: List of S3 keys for the final processed files
        """
        results = []
        
        for input_file in input_files:
            try:
                current_content = None
                current_zone = 'raw'
                current_path = None
                
                # Check if the input is a local file or an S3 key
                if os.path.exists(input_file):
                    # If local file, upload to raw zone first
                    filename = os.path.basename(input_file)
                    s3_key = self.upload_file(input_file, zone='raw')
                    if not s3_key:
                        logger.error(f"Failed to upload {input_file} to raw zone")
                        continue
                    
                    # Parse the uploaded file
                    current_content, mime_type = self.parse_file(s3_key)
                    current_path = filename
                    
                else:
                    # If it's an S3 key, parse it directly
                    current_content, mime_type = self.parse_file(input_file)
                    current_path = os.path.basename(input_file)
                    
                    # Determine the zone from the S3 key
                    parts = input_file.split('/')
                    if len(parts) > 1:
                        current_zone = parts[0]
                
                if current_content is None:
                    logger.error(f"Failed to parse {input_file}")
                    continue
                
                # Apply processing functions sequentially
                for i, process_func in enumerate(process_funcs):
                    # Determine target zone based on processing step
                    if i == 0:
                        target_zone = 'processed'
                    elif i == len(process_funcs) - 1:
                        target_zone = 'curated'
                    else:
                        target_zone = 'enriched'
                    
                    # Apply the processing function
                    try:
                        processed_content = process_func(current_content)
                        
                        # Generate a unique filename for the processed file
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        name_parts = os.path.splitext(current_path)
                        processed_file_name = f"{name_parts[0]}_{i+1}_{timestamp}{name_parts[1]}"
                        
                        # Save the processed content and upload to the target zone
                        s3_key = self.save_and_upload(
                            processed_content,
                            processed_file_name,
                            target_zone,
                            metadata={
                                'original_file': input_file,
                                'processing_step': f"step_{i+1}",
                                'processor': process_func.__name__ if hasattr(process_func, '__name__') else 'unknown'
                            }
                        )
                        
                        if s3_key:
                            # Update for next step in the pipeline
                            current_content = processed_content
                            current_zone = target_zone
                            current_path = processed_file_name
                        else:
                            logger.error(f"Failed to save processed file for {input_file} at step {i+1}")
                            break
                        
                    except Exception as e:
                        logger.error(f"Error processing {input_file} at step {i+1}: {str(e)}")
                        break
                
                # Add the final processed file to results if processing completed
                if current_zone == 'curated' or current_zone == 'enriched':
                    results.append(f"{current_zone}/{current_path}")
                
            except Exception as e:
                logger.error(f"Error in data pipeline for {input_file}: {str(e)}")
        
        return results
    
    def bulk_upload(self, directory: str, zone: str = 'raw', recursive: bool = True) -> List[str]:
        """
        Upload all files in a directory to the data lake.
        
        Args:
            directory (str): Path to the local directory
            zone (str, optional): Data lake zone. Defaults to 'raw'.
            recursive (bool, optional): Whether to upload files recursively. Defaults to True.
        
        Returns:
            List[str]: List of S3 keys for the uploaded files
        """
        uploaded_files = []
        
        # Collect all files
        if recursive:
            file_paths = []
            for root, _, files in os.walk(directory):
                for file in files:
                    file_paths.append(os.path.join(root, file))
        else:
            file_paths = [os.path.join(directory, f) for f in os.listdir(directory) 
                         if os.path.isfile(os.path.join(directory, f))]
        
        # Upload each file
        for file_path in file_paths:
            try:
                rel_path = os.path.relpath(file_path, directory)
                s3_file_path = rel_path.replace('\\', '/') if '\\' in rel_path else rel_path
                
                s3_key = self.upload_file(file_path, zone, s3_file_path)
                if s3_key:
                    uploaded_files.append(s3_key)
                    logger.info(f"Uploaded {file_path} to {s3_key}")
                else:
                    logger.error(f"Failed to upload {file_path}")
            
            except Exception as e:
                logger.error(f"Error uploading {file_path}: {str(e)}")
        
        return uploaded_files
    
    def bulk_download(self, s3_prefix: str, local_directory: str, flatten: bool = False) -> int:
        """
        Download all files with a prefix from the data lake.
        
        Args:
            s3_prefix (str): S3 prefix to filter files
            local_directory (str): Local directory to save files to
            flatten (bool, optional): Whether to flatten the directory structure. Defaults to False.
        
        Returns:
            int: Number of files downloaded
        """
        count = 0
        
        try:
            # Create the local directory if it doesn't exist
            os.makedirs(local_directory, exist_ok=True)
            
            # List all files with the prefix
            files = self.list_files(prefix=s3_prefix)
            
            # Download each file
            for file_info in files:
                s3_key = file_info['key']
                
                if flatten:
                    # If flattening, use just the filename
                    local_path = os.path.join(local_directory, os.path.basename(s3_key))
                else:
                    # Otherwise, maintain the directory structure (minus the prefix)
                    relative_path = s3_key
                    if s3_prefix:
                        if s3_key.startswith(s3_prefix):
                            relative_path = s3_key[len(s3_prefix):].lstrip('/')
                    
                    local_path = os.path.join(local_directory, relative_path)
                    os.makedirs(os.path.dirname(local_path), exist_ok=True)
                
                if self.download_file(s3_key, local_path):
                    count += 1
                    logger.info(f"Downloaded {s3_key} to {local_path}")
                else:
                    logger.error(f"Failed to download {s3_key}")
            
            return count
            
        except Exception as e:
            logger.error(f"Error in bulk download: {str(e)}")
            return count
    
    def search_files_by_metadata(self, metadata_filters: Dict[str, str], zone: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Search for files by metadata in the data lake.
        
        Args:
            metadata_filters (Dict[str, str]): Key-value pairs to match in metadata
            zone (str, optional): Data lake zone to search in. Defaults to None (all zones).
        
        Returns:
            List[Dict[str, Any]]: List of matching file information dictionaries
        """
        results = []
        
        try:
            # List all files
            files = self.list_files(zone=zone)
            
            # Check each file's metadata
            for file_info in files:
                s3_key = file_info['key']
                metadata = self.get_file_metadata(s3_key)
                
                if metadata and 'metadata' in metadata:
                    # Check if all filters match
                    matches = True
                    for key, value in metadata_filters.items():
                        if key not in metadata['metadata'] or metadata['metadata'][key] != value:
                            matches = False
                            break
                    
                    if matches:
                        results.append(file_info)
            
            return results
            
        except Exception as e:
            logger.error(f"Error searching files by metadata: {str(e)}")
            return []
    
    def get_presigned_url(self, s3_key: str, expiration: int = 3600) -> Optional[str]:
        """
        Generate a presigned URL for a file in the data lake.
        
        Args:
            s3_key (str): Full S3 key of the file
            expiration (int, optional): URL expiration time in seconds. Defaults to 3600 (1 hour).
        
        Returns:
            str: Presigned URL or None if error
        """
        return self.data_lake.get_presigned_url(s3_key, expiration)