"""
Base Agent class for Ripplesense AI Agent Framework
Provides common functionality for all Ripplesense agents
"""

import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Optional

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


class RipplesenseAgent(ABC):
    """Base class for all Ripplesense agents"""

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        region: str = "us-east-1",
        use_ssl: bool = False,
    ):
        """
        Initialize the agent with S3 connection parameters

        Args:
            endpoint: RGW endpoint URL
            access_key: AWS access key ID
            secret_key: AWS secret access key
            region: AWS region (default: us-east-1)
            use_ssl: Whether to use SSL
        """
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.use_ssl = use_ssl
        self.logger = logging.getLogger(self.__class__.__name__)
        self.s3_client = None
        self.alerts: List[Dict] = []
        self.warnings: List[Dict] = []
        self.recommendations: List[Dict] = []

    def connect(self):
        """Establish connection to S3 endpoint"""
        try:
            self.s3_client = boto3.client(
                "s3",
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                endpoint_url=self.endpoint,
                use_ssl=self.use_ssl,
                verify=False,
                region_name=self.region,
                config=Config(signature_version="s3v4"),
            )
            self.logger.info(f"Connected to endpoint: {self.endpoint}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect: {e}")
            return False

    def add_alert(
        self,
        severity: str,
        category: str,
        message: str,
        bucket: Optional[str] = None,
        object_key: Optional[str] = None,
        metadata: Optional[Dict] = None,
    ):
        """
        Add an alert to the agent's alert list

        Args:
            severity: Alert severity (critical, high, medium, low)
            category: Alert category
            message: Alert message
            bucket: Bucket name (if applicable)
            object_key: Object key (if applicable)
            metadata: Additional metadata
        """
        alert = {
            "timestamp": datetime.now().isoformat(),
            "agent": self.__class__.__name__,
            "severity": severity,
            "category": category,
            "message": message,
            "bucket": bucket,
            "object_key": object_key,
            "metadata": metadata or {},
        }
        self.alerts.append(alert)
        self.logger.warning(f"ALERT [{severity}]: {message}")

    def add_warning(
        self,
        category: str,
        message: str,
        bucket: Optional[str] = None,
        object_key: Optional[str] = None,
        metadata: Optional[Dict] = None,
    ):
        """
        Add a warning to the agent's warning list

        Args:
            category: Warning category
            message: Warning message
            bucket: Bucket name (if applicable)
            object_key: Object key (if applicable)
            metadata: Additional metadata
        """
        warning = {
            "timestamp": datetime.now().isoformat(),
            "agent": self.__class__.__name__,
            "category": category,
            "message": message,
            "bucket": bucket,
            "object_key": object_key,
            "metadata": metadata or {},
        }
        self.warnings.append(warning)
        self.logger.info(f"WARNING: {message}")

    def add_recommendation(
        self,
        category: str,
        message: str,
        action: str,
        bucket: Optional[str] = None,
        metadata: Optional[Dict] = None,
    ):
        """
        Add a recommendation to the agent's recommendation list

        Args:
            category: Recommendation category
            message: Recommendation message
            action: Recommended action
            bucket: Bucket name (if applicable)
            metadata: Additional metadata
        """
        recommendation = {
            "timestamp": datetime.now().isoformat(),
            "agent": self.__class__.__name__,
            "category": category,
            "message": message,
            "action": action,
            "bucket": bucket,
            "metadata": metadata or {},
        }
        self.recommendations.append(recommendation)
        self.logger.info(f"RECOMMENDATION: {message}")

    def get_bucket_versioning(self, bucket_name: str) -> Optional[Dict]:
        """
        Get bucket versioning configuration

        Args:
            bucket_name: Name of the bucket

        Returns:
            Versioning configuration dict or None
        """
        try:
            response = self.s3_client.get_bucket_versioning(Bucket=bucket_name)
            return response
        except ClientError as e:
            self.logger.error(f"Error getting versioning for {bucket_name}: {e}")
            return None

    def get_bucket_lifecycle(self, bucket_name: str) -> Optional[Dict]:
        """
        Get bucket lifecycle configuration

        Args:
            bucket_name: Name of the bucket

        Returns:
            Lifecycle configuration dict or None
        """
        try:
            response = self.s3_client.get_bucket_lifecycle_configuration(
                Bucket=bucket_name
            )
            return response
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "NoSuchLifecycleConfiguration":
                return None
            self.logger.error(f"Error getting lifecycle for {bucket_name}: {e}")
            return None

    def list_buckets(self) -> List[str]:
        """
        List all buckets accessible to the user

        Returns:
            List of bucket names
        """
        try:
            response = self.s3_client.list_buckets()
            return [bucket["Name"] for bucket in response.get("Buckets", [])]
        except ClientError as e:
            self.logger.error(f"Error listing buckets: {e}")
            return []

    def list_objects(
        self, bucket_name: str, prefix: str = "", max_keys: int = 1000
    ) -> List[Dict]:
        """
        List objects in a bucket

        Args:
            bucket_name: Name of the bucket
            prefix: Object key prefix filter
            max_keys: Maximum number of keys to return

        Returns:
            List of object metadata dictionaries
        """
        objects = []
        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(
                Bucket=bucket_name, Prefix=prefix, MaxKeys=max_keys
            ):
                objects.extend(page.get("Contents", []))
        except ClientError as e:
            self.logger.error(f"Error listing objects in {bucket_name}: {e}")
        return objects

    def get_object_metadata(self, bucket_name: str, object_key: str) -> Optional[Dict]:
        """
        Get object metadata

        Args:
            bucket_name: Name of the bucket
            object_key: Object key

        Returns:
            Object metadata dict or None
        """
        try:
            response = self.s3_client.head_object(Bucket=bucket_name, Key=object_key)
            return response
        except ClientError as e:
            self.logger.error(
                f"Error getting metadata for {bucket_name}/{object_key}: {e}"
            )
            return None

    def get_bucket_object_lock_configuration(self, bucket_name: str) -> Optional[Dict]:
        """
        Get bucket Object Lock configuration

        Args:
            bucket_name: Name of the bucket

        Returns:
            Object Lock configuration dict or None
        """
        try:
            response = self.s3_client.get_object_lock_configuration(Bucket=bucket_name)
            return response
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code in ["NoSuchBucket", "ObjectLockConfigurationNotFoundError"]:
                return None
            self.logger.debug(f"Object Lock not enabled for bucket {bucket_name}: {e}")
            return None

    def has_bucket_object_lock_enabled(self, bucket_name: str) -> bool:
        """
        Check if bucket has Object Lock enabled

        Args:
            bucket_name: Name of the bucket

        Returns:
            True if Object Lock is enabled, False otherwise
        """
        config = self.get_bucket_object_lock_configuration(bucket_name)
        if config and config.get("ObjectLockConfiguration"):
            return config["ObjectLockConfiguration"].get("ObjectLockEnabled") == "Enabled"
        return False

    def get_object_retention(self, bucket_name: str, object_key: str) -> Optional[Dict]:
        """
        Get object retention configuration

        Args:
            bucket_name: Name of the bucket
            object_key: Object key

        Returns:
            Object retention dict or None
        """
        try:
            response = self.s3_client.get_object_retention(
                Bucket=bucket_name, Key=object_key
            )
            return response
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code in ["NoSuchKey", "InvalidRequest"]:
                return None
            self.logger.debug(
                f"Object retention not set for {bucket_name}/{object_key}: {e}"
            )
            return None

    def get_object_legal_hold(self, bucket_name: str, object_key: str) -> Optional[Dict]:
        """
        Get object legal hold status

        Args:
            bucket_name: Name of the bucket
            object_key: Object key

        Returns:
            Object legal hold dict or None
        """
        try:
            response = self.s3_client.get_object_legal_hold(
                Bucket=bucket_name, Key=object_key
            )
            return response
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code in ["NoSuchKey", "InvalidRequest"]:
                return None
            self.logger.debug(
                f"Object legal hold not set for {bucket_name}/{object_key}: {e}"
            )
            return None

    def has_object_lock_enabled(
        self, bucket_name: str, object_key: str
    ) -> bool:
        """
        Check if object has Object Lock enabled (either retention or legal hold)

        Args:
            bucket_name: Name of the bucket
            object_key: Object key

        Returns:
            True if object has Object Lock enabled, False otherwise
        """
        # Check if bucket has Object Lock enabled first
        if not self.has_bucket_object_lock_enabled(bucket_name):
            return False

        # Check for object retention
        retention = self.get_object_retention(bucket_name, object_key)
        if retention and retention.get("Retention"):
            return True

        # Check for legal hold
        legal_hold = self.get_object_legal_hold(bucket_name, object_key)
        if legal_hold and legal_hold.get("LegalHold"):
            status = legal_hold["LegalHold"].get("Status")
            if status == "ON":
                return True

        return False

    @abstractmethod
    def analyze(self) -> Dict:
        """
        Perform analysis - must be implemented by subclasses

        Returns:
            Dictionary containing analysis results
        """
        pass

    def get_results(self) -> Dict:
        """
        Get all results from the agent

        Returns:
            Dictionary containing alerts, warnings, and recommendations
        """
        return {
            "agent_name": self.__class__.__name__,
            "agent_description": self.__doc__ or "No description",
            "alerts": self.alerts,
            "warnings": self.warnings,
            "recommendations": self.recommendations,
            "summary": {
                "total_alerts": len(self.alerts),
                "total_warnings": len(self.warnings),
                "total_recommendations": len(self.recommendations),
                "critical_alerts": len(
                    [a for a in self.alerts if a["severity"] == "critical"]
                ),
                "high_alerts": len([a for a in self.alerts if a["severity"] == "high"]),
            },
        }

    def reset(self):
        """Reset agent state"""
        self.alerts = []
        self.warnings = []
        self.recommendations = []

