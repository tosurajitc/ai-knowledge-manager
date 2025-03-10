"""
Configuration manager for S3 Data Lake settings.

This module provides the S3DataLakeConfig class for loading, saving, and validating
configuration for the S3 data lake.
"""

import os
import json
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class S3DataLakeConfig:
    """
    Configuration manager for S3 Data Lake settings.
    
    This class handles loading, saving, and validating configuration for the S3 data lake.
    """
    
    def __init__(self, config_path=None):
        """
        Initialize the configuration manager.
        
        Args:
            config_path (str, optional): Path to the configuration file. 
                                         Defaults to './config/s3_config.json'.
        """
        self.config_path = config_path or os.path.join('config', 's3_config.json')
        self.config = self._load_config()
    
    def _load_config(self):
        """Load configuration from file if it exists, otherwise use defaults."""
        default_config = {
            "aws": {
                "bucket_name": "ai-knowledge-manager",
                "region_name": "us-east-1",
                "profile_name": None
            },
            "data_lake": {
                "zones": ["raw", "processed", "enriched", "curated"],
                "lifecycle_rules": {
                    "raw": {
                        "days_to_ia": 90,
                        "days_to_glacier": None
                    },
                    "processed": {
                        "days_to_ia": None,
                        "days_to_glacier": 180
                    }
                }
            }
        }
        
        # If config file exists, load it
        if os.path.exists(self.config_path):
            try:
                with open(self.config_path, 'r') as f:
                    loaded_config = json.load(f)
                
                # Merge loaded config with defaults
                default_config.update(loaded_config)
                logger.info(f"Loaded configuration from {self.config_path}")
            except Exception as e:
                logger.error(f"Error loading configuration: {str(e)}")
        else:
            logger.info(f"Configuration file {self.config_path} not found. Using defaults.")
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
            # Save default config
            self.save_config(default_config)
        
        return default_config
    
    def save_config(self, config=None):
        """
        Save configuration to file.
        
        Args:
            config (dict, optional): Configuration to save. Defaults to current config.
        
        Returns:
            bool: True if save was successful, False otherwise
        """
        config_to_save = config or self.config
        
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
            
            # Save configuration
            with open(self.config_path, 'w') as f:
                json.dump(config_to_save, f, indent=4)
            
            logger.info(f"Saved configuration to {self.config_path}")
            return True
        
        except Exception as e:
            logger.error(f"Error saving configuration: {str(e)}")
            return False
    
    def update_config(self, updates):
        """
        Update configuration with new values.
        
        Args:
            updates (dict): Dictionary with updated values
        
        Returns:
            bool: True if update was successful, False otherwise
        """
        # Helper function to update nested dictionaries
        def update_nested_dict(d, u):
            for k, v in u.items():
                if isinstance(v, dict) and k in d and isinstance(d[k], dict):
                    update_nested_dict(d[k], v)
                else:
                    d[k] = v
        
        try:
            update_nested_dict(self.config, updates)
            return self.save_config()
        
        except Exception as e:
            logger.error(f"Error updating configuration: {str(e)}")
            return False
    
    def validate_config(self):
        """
        Validate the current configuration.
        
        Returns:
            tuple: (is_valid, error_message)
        """
        try:
            # Check required AWS settings
            if not self.config.get('aws', {}).get('bucket_name'):
                return False, "AWS bucket name is required"
            
            if not self.config.get('aws', {}).get('region_name'):
                return False, "AWS region name is required"
            
            # Check if zones are defined
            if not self.config.get('data_lake', {}).get('zones'):
                return False, "Data lake zones are required"
            
            return True, "Configuration is valid"
        
        except Exception as e:
            return False, f"Error validating configuration: {str(e)}"
    
    def get_aws_config(self):
        """
        Get AWS configuration.
        
        Returns:
            dict: AWS configuration
        """
        return self.config.get('aws', {})
    
    def get_data_lake_config(self):
        """
        Get data lake configuration.
        
        Returns:
            dict: Data lake configuration
        """
        return self.config.get('data_lake', {})
    
    def get_bucket_name(self):
        """
        Get AWS S3 bucket name.
        
        Returns:
            str: Bucket name
        """
        return self.config.get('aws', {}).get('bucket_name')
    
    def get_region_name(self):
        """
        Get AWS region name.
        
        Returns:
            str: Region name
        """
        return self.config.get('aws', {}).get('region_name')
    
    def get_profile_name(self):
        """
        Get AWS profile name.
        
        Returns:
            str: Profile name or None
        """
        return self.config.get('aws', {}).get('profile_name')