"""
S3 Access Control and Policies Management.

This module provides functions to create and manage IAM policies and bucket policies
for controlling access to the S3 data lake.
"""

import json
import logging
import boto3
from botocore.exceptions import ClientError
from typing import Dict, List, Optional, Union, Any

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class S3AccessControl:
    """
    Class for managing S3 data lake access control and policies.
    
    This class provides methods to create and manage IAM policies and bucket policies,
    set up encryption, and control access to the S3 data lake.
    """
    
    def __init__(self, bucket_name: str, region_name: str = 'us-east-1', profile_name: Optional[str] = None):
        """
        Initialize the S3 access control.
        
        Args:
            bucket_name (str): The name of the S3 bucket
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
        self.iam = session.client('iam')
        self.kms = session.client('kms')
    
    def set_bucket_policy(self, policy: Dict[str, Any]) -> bool:
        """
        Set a bucket policy for the S3 bucket.
        
        Args:
            policy (Dict[str, Any]): The bucket policy as a dictionary
        
        Returns:
            bool: True if the policy was set successfully, False otherwise
        """
        try:
            policy_str = json.dumps(policy)
            self.s3.put_bucket_policy(Bucket=self.bucket_name, Policy=policy_str)
            logger.info(f"Bucket policy set for {self.bucket_name}")
            return True
        except ClientError as e:
            logger.error(f"Error setting bucket policy: {str(e)}")
            return False
    
    def get_bucket_policy(self) -> Optional[Dict[str, Any]]:
        """
        Get the current bucket policy.
        
        Returns:
            Optional[Dict[str, Any]]: The bucket policy as a dictionary, or None if no policy exists
        """
        try:
            response = self.s3.get_bucket_policy(Bucket=self.bucket_name)
            return json.loads(response['Policy'])
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchBucketPolicy':
                logger.info(f"No bucket policy exists for {self.bucket_name}")
                return None
            else:
                logger.error(f"Error getting bucket policy: {str(e)}")
                return None
    
    def create_default_bucket_policy(self, allow_public_read: bool = False) -> bool:
        """
        Create a default bucket policy based on best practices.
        
        Args:
            allow_public_read (bool, optional): Whether to allow public read access. Defaults to False.
        
        Returns:
            bool: True if the policy was created and set successfully, False otherwise
        """
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "DenyUnencryptedObjectUploads",
                    "Effect": "Deny",
                    "Principal": "*",
                    "Action": "s3:PutObject",
                    "Resource": f"arn:aws:s3:::{self.bucket_name}/*",
                    "Condition": {
                        "StringNotEquals": {
                            "s3:x-amz-server-side-encryption": "AES256"
                        }
                    }
                },
                {
                    "Sid": "DenyHTTP",
                    "Effect": "Deny",
                    "Principal": "*",
                    "Action": "s3:*",
                    "Resource": f"arn:aws:s3:::{self.bucket_name}/*",
                    "Condition": {
                        "Bool": {
                            "aws:SecureTransport": "false"
                        }
                    }
                }
            ]
        }
        
        # Add public read access if requested (not recommended for production)
        if allow_public_read:
            policy["Statement"].append({
                "Sid": "AllowPublicRead",
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": f"arn:aws:s3:::{self.bucket_name}/curated/*"
            })
        
        return self.set_bucket_policy(policy)
    
    def create_role_based_access_policy(self, role_arn: str, read_zones: List[str], write_zones: List[str]) -> bool:
        """
        Create a bucket policy that grants access to specific zones based on IAM role.
        
        Args:
            role_arn (str): The ARN of the IAM role
            read_zones (List[str]): List of zones to grant read access to
            write_zones (List[str]): List of zones to grant write access to
        
        Returns:
            bool: True if the policy was created and set successfully, False otherwise
        """
        policy = {
            "Version": "2012-10-17",
            "Statement": []
        }
        
        # Add read permissions
        if read_zones:
            read_resources = [f"arn:aws:s3:::{self.bucket_name}/{zone}/*" for zone in read_zones]
            policy["Statement"].append({
                "Sid": "AllowRoleRead",
                "Effect": "Allow",
                "Principal": {"AWS": role_arn},
                "Action": [
                    "s3:GetObject",
                    "s3:ListBucket"
                ],
                "Resource": read_resources + [f"arn:aws:s3:::{self.bucket_name}"]
            })
        
        # Add write permissions
        if write_zones:
            write_resources = [f"arn:aws:s3:::{self.bucket_name}/{zone}/*" for zone in write_zones]
            policy["Statement"].append({
                "Sid": "AllowRoleWrite",
                "Effect": "Allow",
                "Principal": {"AWS": role_arn},
                "Action": [
                    "s3:PutObject",
                    "s3:DeleteObject"
                ],
                "Resource": write_resources
            })
        
        # Get the current policy (if any)
        current_policy = self.get_bucket_policy()
        
        if current_policy:
            # Merge with the existing policy
            current_statements = current_policy.get("Statement", [])
            
            # Remove any statements for the same role to avoid conflicts
            filtered_statements = [
                stmt for stmt in current_statements
                if not (isinstance(stmt.get("Principal", {}), dict) and 
                       stmt.get("Principal", {}).get("AWS") == role_arn)
            ]
            
            # Add the new statements
            filtered_statements.extend(policy["Statement"])
            
            # Update the policy
            merged_policy = current_policy.copy()
            merged_policy["Statement"] = filtered_statements
            return self.set_bucket_policy(merged_policy)
        else:
            # Set the new policy
            return self.set_bucket_policy(policy)
    
    def enable_bucket_encryption(self, kms_key_id: Optional[str] = None) -> bool:
        """
        Enable default encryption for the S3 bucket.
        
        Args:
            kms_key_id (str, optional): KMS key ID for SSE-KMS encryption. 
                                       If None, uses AES256 (SSE-S3). Defaults to None.
        
        Returns:
            bool: True if encryption was enabled successfully, False otherwise
        """
        try:
            if kms_key_id:
                # Use SSE-KMS with the specified key
                encryption_config = {
                    'Rules': [
                        {
                            'ApplyServerSideEncryptionByDefault': {
                                'SSEAlgorithm': 'aws:kms',
                                'KMSMasterKeyID': kms_key_id
                            },
                            'BucketKeyEnabled': True
                        }
                    ]
                }
            else:
                # Use SSE-S3 (AES256)
                encryption_config = {
                    'Rules': [
                        {
                            'ApplyServerSideEncryptionByDefault': {
                                'SSEAlgorithm': 'AES256'
                            }
                        }
                    ]
                }
            
            self.s3.put_bucket_encryption(
                Bucket=self.bucket_name,
                ServerSideEncryptionConfiguration=encryption_config
            )
            
            logger.info(f"Encryption enabled for bucket {self.bucket_name}")
            return True
            
        except ClientError as e:
            logger.error(f"Error enabling bucket encryption: {str(e)}")
            return False
    
    def create_kms_key(self, description: str = "KMS key for S3 data lake encryption") -> Optional[str]:
        """
        Create a new KMS key for S3 bucket encryption.
        
        Args:
            description (str, optional): Description for the KMS key. 
                                       Defaults to "KMS key for S3 data lake encryption".
        
        Returns:
            Optional[str]: KMS key ID if created successfully, None otherwise
        """
        try:
            response = self.kms.create_key(
                Description=description,
                KeyUsage='ENCRYPT_DECRYPT',
                Origin='AWS_KMS'
            )
            
            key_id = response['KeyMetadata']['KeyId']
            
            # Create an alias for the key
            alias_name = f"alias/s3-datalake-{self.bucket_name}"
            self.kms.create_alias(
                AliasName=alias_name,
                TargetKeyId=key_id
            )
            
            logger.info(f"Created KMS key {key_id} with alias {alias_name}")
            return key_id
            
        except ClientError as e:
            logger.error(f"Error creating KMS key: {str(e)}")
            return None
    
    def enable_bucket_versioning(self) -> bool:
        """
        Enable versioning for the S3 bucket.
        
        Returns:
            bool: True if versioning was enabled successfully, False otherwise
        """
        try:
            self.s3.put_bucket_versioning(
                Bucket=self.bucket_name,
                VersioningConfiguration={'Status': 'Enabled'}
            )
            
            logger.info(f"Versioning enabled for bucket {self.bucket_name}")
            return True
            
        except ClientError as e:
            logger.error(f"Error enabling bucket versioning: {str(e)}")
            return False
    
    def create_iam_policy(self, policy_name: str, policy_document: Dict[str, Any], 
                         description: str = "IAM policy for S3 data lake access") -> Optional[str]:
        """
        Create an IAM policy for access to the S3 bucket.
        
        Args:
            policy_name (str): Name for the IAM policy
            policy_document (Dict[str, Any]): Policy document
            description (str, optional): Description for the policy.
                                       Defaults to "IAM policy for S3 data lake access".
        
        Returns:
            Optional[str]: Policy ARN if created successfully, None otherwise
        """
        try:
            response = self.iam.create_policy(
                PolicyName=policy_name,
                PolicyDocument=json.dumps(policy_document),
                Description=description
            )
            
            policy_arn = response['Policy']['Arn']
            logger.info(f"Created IAM policy {policy_name} with ARN {policy_arn}")
            return policy_arn
            
        except ClientError as e:
            logger.error(f"Error creating IAM policy: {str(e)}")
            return None
    
    def create_data_scientist_policy(self, policy_name: str = "DataLakeDataScientistPolicy") -> Optional[str]:
        """
        Create an IAM policy for data scientists with read access to processed and curated zones,
        and write access to the enriched zone.
        
        Args:
            policy_name (str, optional): Name for the IAM policy.
                                      Defaults to "DataLakeDataScientistPolicy".
        
        Returns:
            Optional[str]: Policy ARN if created successfully, None otherwise
        """
        policy_document = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "ListBucket",
                    "Effect": "Allow",
                    "Action": "s3:ListBucket",
                    "Resource": f"arn:aws:s3:::{self.bucket_name}",
                    "Condition": {
                        "StringLike": {
                            "s3:prefix": [
                                "processed/*",
                                "enriched/*",
                                "curated/*"
                            ]
                        }
                    }
                },
                {
                    "Sid": "ReadAccess",
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject"
                    ],
                    "Resource": [
                        f"arn:aws:s3:::{self.bucket_name}/processed/*",
                        f"arn:aws:s3:::{self.bucket_name}/curated/*"
                    ]
                },
                {
                    "Sid": "WriteAccess",
                    "Effect": "Allow",
                    "Action": [
                        "s3:PutObject",
                        "s3:DeleteObject"
                    ],
                    "Resource": [
                        f"arn:aws:s3:::{self.bucket_name}/enriched/*"
                    ]
                }
            ]
        }
        
        return self.create_iam_policy(policy_name, policy_document, 
                                     "IAM policy for data scientists to access the data lake")
    
    def create_data_engineer_policy(self, policy_name: str = "DataLakeDataEngineerPolicy") -> Optional[str]:
        """
        Create an IAM policy for data engineers with access to all zones.
        
        Args:
            policy_name (str, optional): Name for the IAM policy.
                                      Defaults to "DataLakeDataEngineerPolicy".
        
        Returns:
            Optional[str]: Policy ARN if created successfully, None otherwise
        """
        policy_document = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "ListBucket",
                    "Effect": "Allow",
                    "Action": "s3:ListBucket",
                    "Resource": f"arn:aws:s3:::{self.bucket_name}"
                },
                {
                    "Sid": "ReadWriteAccess",
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:DeleteObject"
                    ],
                    "Resource": [
                        f"arn:aws:s3:::{self.bucket_name}/raw/*",
                        f"arn:aws:s3:::{self.bucket_name}/processed/*",
                        f"arn:aws:s3:::{self.bucket_name}/enriched/*",
                        f"arn:aws:s3:::{self.bucket_name}/curated/*"
                    ]
                }
            ]
        }
        
        return self.create_iam_policy(policy_name, policy_document, 
                                     "IAM policy for data engineers to access the data lake")
    
    def create_read_only_policy(self, policy_name: str = "DataLakeReadOnlyPolicy") -> Optional[str]:
        """
        Create an IAM policy for read-only access to the curated zone.
        
        Args:
            policy_name (str, optional): Name for the IAM policy.
                                      Defaults to "DataLakeReadOnlyPolicy".
        
        Returns:
            Optional[str]: Policy ARN if created successfully, None otherwise
        """
        policy_document = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "ListBucket",
                    "Effect": "Allow",
                    "Action": "s3:ListBucket",
                    "Resource": f"arn:aws:s3:::{self.bucket_name}",
                    "Condition": {
                        "StringLike": {
                            "s3:prefix": [
                                "curated/*"
                            ]
                        }
                    }
                },
                {
                    "Sid": "ReadAccess",
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject"
                    ],
                    "Resource": [
                        f"arn:aws:s3:::{self.bucket_name}/curated/*"
                    ]
                }
            ]
        }
        
        return self.create_iam_policy(policy_name, policy_document, 
                                     "IAM policy for read-only access to the curated zone")
    
    def configure_public_website(self, index_document: str = "index.html", 
                               error_document: Optional[str] = None) -> bool:
        """
        Configure the bucket for static website hosting.
        This is useful for making curated data available through a web interface.
        
        Args:
            index_document (str, optional): Index document. Defaults to "index.html".
            error_document (str, optional): Error document. Defaults to None.
        
        Returns:
            bool: True if the bucket was configured successfully, False otherwise
        """
        try:
            website_config = {
                'IndexDocument': {'Suffix': index_document}
            }
            
            if error_document:
                website_config['ErrorDocument'] = {'Key': error_document}
            
            self.s3.put_bucket_website(
                Bucket=self.bucket_name,
                WebsiteConfiguration=website_config
            )
            
            logger.info(f"Website configuration enabled for bucket {self.bucket_name}")
            
            # Get the website endpoint
            website_endpoint = f"{self.bucket_name}.s3-website-{self.region_name}.amazonaws.com"
            logger.info(f"Website endpoint: http://{website_endpoint}")
            
            return True
            
        except ClientError as e:
            logger.error(f"Error configuring website: {str(e)}")
            return False
    
    def configure_cors(self, allowed_origins: List[str] = ["*"]) -> bool:
        """
        Configure CORS for the bucket.
        This is useful for allowing web applications to access the data lake.
        
        Args:
            allowed_origins (List[str], optional): List of allowed origins. Defaults to ["*"].
        
        Returns:
            bool: True if CORS was configured successfully, False otherwise
        """
        try:
            cors_config = {
                'CORSRules': [
                    {
                        'AllowedOrigins': allowed_origins,
                        'AllowedMethods': ['GET', 'HEAD'],
                        'AllowedHeaders': ['*'],
                        'ExposeHeaders': ['ETag', 'Content-Length'],
                        'MaxAgeSeconds': 3000
                    }
                ]
            }
            
            self.s3.put_bucket_cors(
                Bucket=self.bucket_name,
                CORSConfiguration=cors_config
            )
            
            logger.info(f"CORS configuration enabled for bucket {self.bucket_name}")
            return True
            
        except ClientError as e:
            logger.error(f"Error configuring CORS: {str(e)}")
            return False
    
    def add_lifecycle_rule(self, prefix: str, days_to_ia: Optional[int] = None, 
                          days_to_glacier: Optional[int] = None, days_to_expire: Optional[int] = None) -> bool:
        """
        Add a lifecycle rule to the bucket.
        
        Args:
            prefix (str): Prefix for the objects (e.g. 'raw/')
            days_to_ia (int, optional): Days until transition to IA storage. Defaults to None.
            days_to_glacier (int, optional): Days until transition to Glacier. Defaults to None.
            days_to_expire (int, optional): Days until expiration. Defaults to None.
        
        Returns:
            bool: True if the rule was added successfully, False otherwise
        """
        try:
            # Get the current lifecycle configuration
            try:
                response = self.s3.get_bucket_lifecycle_configuration(Bucket=self.bucket_name)
                lifecycle_rules = response.get('Rules', [])
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchLifecycleConfiguration':
                    lifecycle_rules = []
                else:
                    raise
            
            # Create the new rule
            rule_id = f"Rule-{prefix.replace('/', '-')}"
            
            # Remove any existing rule with the same ID
            lifecycle_rules = [rule for rule in lifecycle_rules if rule.get('ID') != rule_id]
            
            new_rule = {
                'ID': rule_id,
                'Status': 'Enabled',
                'Filter': {
                    'Prefix': prefix
                },
                'Transitions': [],
                'Expiration': {}
            }
            
            # Add transitions
            if days_to_ia:
                new_rule['Transitions'].append({
                    'Days': days_to_ia,
                    'StorageClass': 'STANDARD_IA'
                })
            
            if days_to_glacier:
                new_rule['Transitions'].append({
                    'Days': days_to_glacier,
                    'StorageClass': 'GLACIER'
                })
            
            # Add expiration
            if days_to_expire:
                new_rule['Expiration'] = {
                    'Days': days_to_expire
                }
            else:
                del new_rule['Expiration']
            
            # If no transitions, remove the key
            if not new_rule['Transitions']:
                del new_rule['Transitions']
            
            # Add the new rule
            lifecycle_rules.append(new_rule)
            
            # Update the lifecycle configuration
            self.s3.put_bucket_lifecycle_configuration(
                Bucket=self.bucket_name,
                LifecycleConfiguration={'Rules': lifecycle_rules}
            )
            
            logger.info(f"Lifecycle rule added for prefix {prefix}")
            return True
            
        except ClientError as e:
            logger.error(f"Error adding lifecycle rule: {str(e)}")
            return False
    
    def setup_standard_lifecycle_rules(self) -> bool:
        """
        Set up standard lifecycle rules for the data lake zones.
        
        Returns:
            bool: True if all rules were added successfully, False otherwise
        """
        success = True
        
        # Raw zone: Move to IA after 90 days, Glacier after 180 days
        if not self.add_lifecycle_rule('raw/', days_to_ia=90, days_to_glacier=180):
            success = False
        
        # Processed zone: Move to IA after 60 days, Glacier after 120 days
        if not self.add_lifecycle_rule('processed/', days_to_ia=60, days_to_glacier=120):
            success = False
        
        # Enriched zone: Move to IA after 30 days
        if not self.add_lifecycle_rule('enriched/', days_to_ia=30):
            success = False
        
        # Curated zone: No automatic transitions
        
        return success