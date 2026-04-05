"""
Agent for detecting versioned buckets without lifecycle rules
This agent identifies buckets with versioning enabled but no lifecycle policies,
which can lead to exponential storage cost growth.
"""

import logging
from typing import Dict

from .base_agent import RipplesenseAgent


class VersioningLifecycleAgent(RipplesenseAgent):
    """
    Detects versioned buckets without lifecycle rules and provides recommendations
    for cost optimization through proper lifecycle policy management.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)

    def analyze(self) -> Dict:
        """
        Analyze buckets for versioning and lifecycle policy gaps

        Returns:
            Dictionary containing analysis results
        """
        if not self.s3_client:
            if not self.connect():
                return self.get_results()

        self.logger.info("Starting versioning and lifecycle policy analysis...")

        buckets = self.list_buckets()
        self.logger.info(f"Analyzing {len(buckets)} buckets...")

        versioned_buckets = []
        versioned_without_lifecycle = []
        suspended_versioning = []

        for bucket_name in buckets:
            try:
                # Check versioning status
                versioning = self.get_bucket_versioning(bucket_name)
                versioning_status = versioning.get("Status", "Disabled")

                if versioning_status == "Enabled":
                    versioned_buckets.append(bucket_name)
                    # Check for lifecycle configuration
                    lifecycle = self.get_bucket_lifecycle(bucket_name)

                    if lifecycle is None or not lifecycle.get("Rules"):
                        versioned_without_lifecycle.append(bucket_name)
                        # Estimate potential cost impact
                        objects = self.list_objects(bucket_name, max_keys=100)
                        object_count = len(objects)

                        # Calculate estimated storage (rough estimate)
                        total_size = sum(
                            obj.get("Size", 0) for obj in objects[:100]
                        )  # Sample first 100
                        avg_size = total_size / max(len(objects), 1)

                        # Add critical alert
                        self.add_alert(
                            severity="critical",
                            category="versioning_lifecycle_gap",
                            message=f"Bucket '{bucket_name}' has versioning enabled but no lifecycle policy. "
                            f"This can lead to exponential storage cost growth from accumulating "
                            f"delete markers and object versions.",
                            bucket=bucket_name,
                            metadata={
                                "versioning_status": versioning_status,
                                "estimated_objects": object_count,
                                "avg_object_size_bytes": avg_size,
                                "risk_level": "high",
                            },
                        )

                        # Add recommendation
                        self.add_recommendation(
                            category="lifecycle_policy",
                            message=f"Configure lifecycle policy for versioned bucket '{bucket_name}' to "
                            f"automatically expire old versions and delete markers.",
                            action=f"Create lifecycle policy for bucket '{bucket_name}' with rules to:\n"
                            f"  - Expire noncurrent versions after 30-90 days\n"
                            f"  - Expire delete markers after 7 days\n"
                            f"  - Consider transitioning old versions to lower cost storage classes",
                            bucket=bucket_name,
                            metadata={
                                "policy_template": {
                                    "Rules": [
                                        {
                                            "Id": "ExpireOldVersions",
                                            "Status": "Enabled",
                                            "NoncurrentVersionExpiration": {
                                                "NoncurrentDays": 90
                                            },
                                            "ExpiredObjectDeleteMarker": True,
                                        }
                                    ]
                                }
                            },
                        )

                elif versioning_status == "Suspended":
                    suspended_versioning.append(bucket_name)
                    self.add_warning(
                        category="versioning_suspended",
                        message=f"Bucket '{bucket_name}' has versioning suspended. "
                        f"Consider enabling lifecycle policies to manage existing versions.",
                        bucket=bucket_name,
                    )

            except Exception as e:
                self.logger.error(f"Error analyzing bucket {bucket_name}: {e}")
                self.add_warning(
                    category="analysis_error",
                    message=f"Failed to analyze bucket '{bucket_name}': {str(e)}",
                    bucket=bucket_name,
                )

        # Summary statistics
        self.logger.info(
            f"Analysis complete: {len(versioned_buckets)} versioned buckets, "
            f"{len(versioned_without_lifecycle)} without lifecycle policies"
        )

        return self.get_results()

