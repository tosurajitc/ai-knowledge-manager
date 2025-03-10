"""
S3 Data Lake Implementation for the AI Knowledge Manager.

This module provides the S3DataLake class for managing the AWS S3 data lake infrastructure.
It handles:
- Data lake creation with raw, processed, enriched, and curated zones
- File management within these zones
- Basic file operations (upload, download, delete, list)
- Moving files across zones as they are processed
"""

import boto3
import os
import json
import logging
from pathlib import Path
from datetime import datetime
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class S3DataLake:
    """
    S3DataLake class to manage the AWS S3 data lake infrastructure for the AI Knowledge Manager.
    
    This class handles:
    - Data lake creation with raw, processed, enriched, and curated zones
    - File management within these zones
    - Basic file operations (upload, download, delete, list)
    - Moving files across zones as they are processed
    """
    
    def __init__(self, bucket_name, region_name='us-east-1', profile_name=None):
        """
        Initialize the S3DataLake with a bucket name and optional region.
        
        Args:
            bucket_name (str): The name of the S3 bucket to use for the data lake
            region_name (str, optional): AWS region. Defaults to 'us-east-1'.
            profile_name (str, optional): AWS profile name for credentials. Defaults to None.
        """
        self.bucket_name = bucket_name
        self.region_name = region_name
        
        # Set up AWS session
        if profile_name:
            session = boto3.Session(profile_name=profile_name, region_name=region_name)
        else:
            session = boto3.Session(region_name=region_name)
        
        self.s3 = session.client('s3')
        self.s3_resource = session.resource('s3')
        
        # Define the data lake zones
        self.zones = ['raw', 'processed', 'enriched', 'curated']
        
        # Create the data lake if it doesn't exist
        self._ensure_data_lake_exists()
    
    def _ensure_data_lake_exists(self):
        """Ensure the data lake bucket and its zone prefixes exist."""
        try:
            # Check if bucket exists
            self.s3.head_bucket(Bucket=self.bucket_name)
            logger.info(f"Bucket {self.bucket_name} already exists.")
        except ClientError as e:
            # If a 404 error, then the bucket does not exist
            if e.response['Error']['Code'] == '404':
                logger.info(f"Creating bucket {self.bucket_name} in region {self.region_name}")
                if self.region_name == 'us-east-1':
                    self.s3.create_bucket(Bucket=self.bucket_name)
                else:
                    self.s3.create_bucket(
                        Bucket=self.bucket_name,
                        CreateBucketConfiguration={'LocationConstraint': self.region_name}
                    )
            else:
                # If another error (e.g., access denied), raise it
                raise
        
        # Ensure lifecycle policies are set up for the bucket
        self._setup_lifecycle_policies()
        
        # Create zone folders if they don't exist
        for zone in self.zones:
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=f'{zone}/'
            )
            logger.info(f"Created or confirmed zone: {zone}")
    
    def _setup_lifecycle_policies(self):
        """Set up lifecycle policies for the data lake."""
        lifecycle_config = {
            'Rules': [
                {
                    'ID': 'TransitionToInfrequentAccess',
                    'Status': 'Enabled',
                    'Filter': {
                        'Prefix': 'raw/'
                    },
                    'Transitions': [
                        {
                            'Days': 90,
                            'StorageClass': 'STANDARD_IA'
                        }
                    ]
                },
                {
                    'ID': 'TransitionToGlacier',
                    'Status': 'Enabled',
                    'Filter': {
                        'Prefix': 'processed/'
                    },
                    'Transitions': [
                        {
                            'Days': 180,
                            'StorageClass': 'GLACIER'
                        }
                    ]
                }
            ]
        }
        
        try:
            self.s3.put_bucket_lifecycle_configuration(
                Bucket=self.bucket_name,
                LifecycleConfiguration=lifecycle_config
            )
            logger.info(f"Lifecycle policies set up for bucket {self.bucket_name}")
        except ClientError as e:
            logger.error(f"Error setting lifecycle policies: {str(e)}")
    
    def upload_file(self, local_file_path, zone='raw', s3_file_path=None, metadata=None):
        """
        Upload a file to a specific zone in the data lake.
        
        Args:
            local_file_path (str): Path to the local file
            zone (str, optional): Data lake zone. Defaults to 'raw'.
            s3_file_path (str, optional): Custom path in the zone. Defaults to filename.
            metadata (dict, optional): Metadata for the file. Defaults to None.
        
        Returns:
            bool: True if upload was successful, False otherwise
        """
        if zone not in self.zones:
            logger.error(f"Invalid zone '{zone}'. Must be one of {self.zones}")
            return False
        
        try:
            # If no specific S3 path is given, use the filename
            if s3_file_path is None:
                filename = os.path.basename(local_file_path)
                s3_file_path = filename
            
            # Construct full S3 key
            key = f"{zone}/{s3_file_path}"
            
            # Prepare extra args with metadata if provided
            extra_args = {}
            if metadata:
                extra_args['Metadata'] = metadata
            
            # Upload the file
            self.s3.upload_file(
                local_file_path, 
                self.bucket_name, 
                key,
                ExtraArgs=extra_args
            )
            
            logger.info(f"Successfully uploaded {local_file_path} to {key}")
            return True
        
        except ClientError as e:
            logger.error(f"Error uploading file: {str(e)}")
            return False
    
    def download_file(self, s3_key, local_path):
        """
        Download a file from the data lake.
        
        Args:
            s3_key (str): Full S3 key of the file to download
            local_path (str): Local path to save the file to
        
        Returns:
            bool: True if download was successful, False otherwise
        """
        try:
            # Create the directory if it doesn't exist
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            # Download the file
            self.s3.download_file(self.bucket_name, s3_key, local_path)
            logger.info(f"Downloaded {s3_key} to {local_path}")
            return True
        
        except ClientError as e:
            logger.error(f"Error downloading file: {str(e)}")
            return False
    
    def list_files(self, zone=None, prefix=None, recursive=True):
        """
        List files in the data lake, optionally filtered by zone and prefix.
        
        Args:
            zone (str, optional): Data lake zone to list. Defaults to None (all zones).
            prefix (str, optional): Additional prefix filter. Defaults to None.
            recursive (bool, optional): Whether to list files recursively. Defaults to True.
        
        Returns:
            list: List of file information dictionaries
        """
        try:
            # Set up the prefix to use
            if zone and prefix:
                full_prefix = f"{zone}/{prefix}"
            elif zone:
                full_prefix = f"{zone}/"
            elif prefix:
                full_prefix = prefix
            else:
                full_prefix = ""
            
            # Get objects with the prefix
            paginator = self.s3.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.bucket_name, Prefix=full_prefix)
            
            files = []
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        # Skip "directory" markers
                        if obj['Key'].endswith('/'):
                            continue
                        
                        # Add file information to the list
                        files.append({
                            'key': obj['Key'],
                            'size': obj['Size'],
                            'last_modified': obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S'),
                            'zone': obj['Key'].split('/')[0] if '/' in obj['Key'] else 'unknown'
                        })
            
            return files
        
        except ClientError as e:
            logger.error(f"Error listing files: {str(e)}")
            return []
    
    def delete_file(self, s3_key):
        """
        Delete a file from the data lake.
        
        Args:
            s3_key (str): Full S3 key of the file to delete
        
        Returns:
            bool: True if deletion was successful, False otherwise
        """
        try:
            self.s3.delete_object(Bucket=self.bucket_name, Key=s3_key)
            logger.info(f"Deleted {s3_key}")
            return True
        
        except ClientError as e:
            logger.error(f"Error deleting file: {str(e)}")
            return False
    
    def move_file(self, source_key, target_zone, target_path=None):
        """
        Move a file from one location to another in the data lake.
        This is useful for moving data between zones as processing progresses.
        
        Args:
            source_key (str): Full S3 key of the source file
            target_zone (str): Target data lake zone
            target_path (str, optional): Custom path in the target zone. 
                                        Defaults to original filename.
        
        Returns:
            bool: True if move was successful, False otherwise
        """
        if target_zone not in self.zones:
            logger.error(f"Invalid target zone '{target_zone}'. Must be one of {self.zones}")
            return False
        
        try:
            # If no target path specified, use the original filename
            if target_path is None:
                filename = os.path.basename(source_key)
                target_path = filename
            
            # Construct full target key
            target_key = f"{target_zone}/{target_path}"
            
            # Copy the object
            self.s3.copy_object(
                CopySource={'Bucket': self.bucket_name, 'Key': source_key},
                Bucket=self.bucket_name,
                Key=target_key
            )
            
            # Delete the source object
            self.s3.delete_object(Bucket=self.bucket_name, Key=source_key)
            
            logger.info(f"Moved {source_key} to {target_key}")
            return True
        
        except ClientError as e:
            logger.error(f"Error moving file: {str(e)}")
            return False
    
    def get_file_metadata(self, s3_key):
        """
        Get metadata for a file in the data lake.
        
        Args:
            s3_key (str): Full S3 key of the file
        
        Returns:
            dict: File metadata or None if error
        """
        try:
            response = self.s3.head_object(Bucket=self.bucket_name, Key=s3_key)
            return {
                'key': s3_key,
                'size': response['ContentLength'],
                'last_modified': response['LastModified'].strftime('%Y-%m-%d %H:%M:%S'),
                'content_type': response.get('ContentType', 'unknown'),
                'metadata': response.get('Metadata', {})
            }
        
        except ClientError as e:
            logger.error(f"Error getting file metadata: {str(e)}")
            return None
    
    def update_file_metadata(self, s3_key, metadata):
        """
        Update metadata for a file in the data lake.
        
        Args:
            s3_key (str): Full S3 key of the file
            metadata (dict): New metadata to set
        
        Returns:
            bool: True if update was successful, False otherwise
        """
        try:
            # First, we need to get the existing object metadata to preserve it
            existing_obj = self.s3.head_object(Bucket=self.bucket_name, Key=s3_key)
            
            # Then, copy the object onto itself with the new metadata
            self.s3.copy_object(
                CopySource={'Bucket': self.bucket_name, 'Key': s3_key},
                Bucket=self.bucket_name,
                Key=s3_key,
                Metadata=metadata,
                MetadataDirective='REPLACE'
            )
            
            logger.info(f"Updated metadata for {s3_key}")
            return True
        
        except ClientError as e:
            logger.error(f"Error updating file metadata: {str(e)}")
            return False
    
    def create_folder(self, zone, folder_path):
        """
        Create a folder (prefix) in a specific zone.
        
        Args:
            zone (str): Data lake zone
            folder_path (str): Path for the new folder
        
        Returns:
            bool: True if creation was successful, False otherwise
        """
        if zone not in self.zones:
            logger.error(f"Invalid zone '{zone}'. Must be one of {self.zones}")
            return False
        
        try:
            # Ensure the folder path ends with a /
            if not folder_path.endswith('/'):
                folder_path = folder_path + '/'
            
            # Create the folder
            key = f"{zone}/{folder_path}"
            self.s3.put_object(Bucket=self.bucket_name, Key=key)
            
            logger.info(f"Created folder {key}")
            return True
        
        except ClientError as e:
            logger.error(f"Error creating folder: {str(e)}")
            return False

    def get_presigned_url(self, s3_key, expiration=3600):
        """
        Generate a presigned URL for a file in the data lake.
        
        Args:
            s3_key (str): Full S3 key of the file
            expiration (int, optional): URL expiration time in seconds. Defaults to 3600 (1 hour).
        
        Returns:
            str: Presigned URL or None if error
        """
        try:
            url = self.s3.generate_presigned_url(
                'get_object',
                Params={'Bucket': self.bucket_name, 'Key': s3_key},
                ExpiresIn=expiration
            )
            return url
        
        except ClientError as e:
            logger.error(f"Error generating presigned URL: {str(e)}")
            return None