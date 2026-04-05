"""
Agent for providing Garbage Collection (GC) configuration recommendations
based on bucket usage patterns and bulk delete operations
"""

import logging
from typing import Dict

from .base_agent import RipplesenseAgent


class GCConfigAgent(RipplesenseAgent):
    """
    Analyzes bucket configurations and usage patterns to provide
    Garbage Collection (GC) configuration recommendations for optimal performance.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)

    def analyze(self) -> Dict:
        """
        Analyze buckets for GC configuration optimization opportunities

        Returns:
            Dictionary containing analysis results
        """
        if not self.s3_client:
            if not self.connect():
                return self.get_results()

        self.logger.info("Starting GC configuration analysis...")

        buckets = self.list_buckets()
        self.logger.info(f"Analyzing {len(buckets)} buckets for GC optimization...")

        versioned_buckets = []
        buckets_with_lifecycle = []
        high_object_count_buckets = []

        for bucket_name in buckets:
            try:
                # Check versioning status
                versioning = self.get_bucket_versioning(bucket_name)
                versioning_status = versioning.get("Status", "Disabled")

                # Check lifecycle configuration
                lifecycle = self.get_bucket_lifecycle(bucket_name)
                has_lifecycle = lifecycle is not None and len(lifecycle.get("Rules", [])) > 0

                # Get accurate object count using pagination
                # For performance, we'll count up to a reasonable limit or use pagination
                object_count = 0
                try:
                    paginator = self.s3_client.get_paginator("list_objects_v2")
                    page_iterator = paginator.paginate(Bucket=bucket_name, MaxKeys=1000)
                    
                    # Count objects, but limit to first 15000 for performance
                    max_count_check = 15000
                    for page in page_iterator:
                        page_objects = page.get("Contents", [])
                        object_count += len(page_objects)
                        if object_count >= max_count_check:
                            # Indicate this is an estimate
                            object_count = max_count_check
                            break
                except Exception as e:
                    self.logger.warning(f"Error counting objects in {bucket_name}: {e}")
                    # Fallback to simple list if pagination fails
                    objects = self.list_objects(bucket_name, max_keys=1000)
                    object_count = len(objects)

                # Check if bucket has high object count (indicating potential bulk operations)
                if object_count >= 1000:
                    high_object_count_buckets.append(
                        {
                            "bucket": bucket_name,
                            "object_count": object_count,
                            "versioning": versioning_status,
                            "has_lifecycle": has_lifecycle,
                        }
                    )

                if versioning_status == "Enabled":
                    versioned_buckets.append(bucket_name)

                    # Versioned buckets with lifecycle policies need optimized GC
                    if has_lifecycle:
                        buckets_with_lifecycle.append(bucket_name)

                        # Check for expiration rules that would trigger bulk deletes
                        expiration_rules = []
                        for rule in lifecycle.get("Rules", []):
                            if "Expiration" in rule or "NoncurrentVersionExpiration" in rule:
                                expiration_rules.append(rule)

                        if expiration_rules:
                            self.add_recommendation(
                                category="gc_config_optimization",
                                message=f"Bucket '{bucket_name}' has versioning enabled with lifecycle "
                                f"expiration rules. Optimize GC configuration for bulk delete operations.",
                                action=f"Configure GC settings for bucket '{bucket_name}':\n"
                                f"  - Increase GC concurrency for faster processing\n"
                                f"  - Tune GC batch size based on object count\n"
                                f"  - Monitor GC queue depth and adjust accordingly\n"
                                f"  - Consider staggered expiration to avoid GC spikes",
                                bucket=bucket_name,
                                metadata={
                                    "versioning_status": versioning_status,
                                    "has_lifecycle": True,
                                    "expiration_rules_count": len(expiration_rules),
                                    "estimated_object_count": object_count,
                                },
                            )

                # High object count buckets may need GC tuning
                if object_count >= 10000:
                    self.add_warning(
                        category="high_object_count",
                        message=f"Bucket '{bucket_name}' has a high object count ({object_count}+ objects). "
                        f"Consider optimizing GC configuration for better performance during bulk operations.",
                        bucket=bucket_name,
                        metadata={
                            "object_count": object_count,
                            "versioning_status": versioning_status,
                        },
                    )
                    
                    # Add specific recommendation for heavy delete workloads
                    # Based on IBM Storage Ceph documentation for delete-heavy workloads
                    self.add_recommendation(
                        category="gc_heavy_delete_config",
                        message=f"Bucket '{bucket_name}' has {object_count}+ objects and may benefit from "
                        f"optimized GC settings for delete-heavy workloads. This is especially important "
                        f"for workloads where many objects are stored for short periods and then deleted.",
                        action=f"For delete-heavy workloads with bucket '{bucket_name}', consider adjusting "
                        f"garbage collection settings:\n\n"
                        f"1. Set rgw_gc_max_concurrent_io to 20:\n"
                        f"   ceph config set client.rgw rgw_gc_max_concurrent_io 20\n\n"
                        f"2. Set rgw_gc_max_trim_chunk to 64:\n"
                        f"   ceph config set client.rgw rgw_gc_max_trim_chunk 64\n\n"
                        f"3. Restart the Ceph Object Gateway to apply changes\n"
                        f"4. Monitor the storage cluster during GC activity to verify the increased "
                        f"values do not adversely affect performance\n\n"
                        f"Reference: https://www.ibm.com/docs/en/storage-ceph/8.1.0?topic=collection-adjusting-garbage-delete-heavy-workloads\n\n"
                        f"Important: Never modify rgw_gc_max_objs in a running cluster. "
                        f"Only change this value before deploying RGW nodes.",
                        bucket=bucket_name,
                        metadata={
                            "object_count": object_count,
                            "versioning_status": versioning_status,
                            "has_lifecycle": has_lifecycle,
                            "recommended_rgw_gc_max_concurrent_io": 20,
                            "recommended_rgw_gc_max_trim_chunk": 64,
                            "reference_url": "https://www.ibm.com/docs/en/storage-ceph/8.1.0?topic=collection-adjusting-garbage-delete-heavy-workloads",
                        },
                    )

            except Exception as e:
                self.logger.error(f"Error analyzing bucket {bucket_name}: {e}")
                self.add_warning(
                    category="analysis_error",
                    message=f"Failed to analyze bucket '{bucket_name}': {str(e)}",
                    bucket=bucket_name,
                )

        # General GC recommendations
        if versioned_buckets:
            self.add_recommendation(
                category="general_gc_config",
                message=f"Found {len(versioned_buckets)} versioned buckets. "
                f"Ensure GC configuration is optimized for versioned bucket operations.",
                action="General GC configuration recommendations:\n"
                "  - Set appropriate GC worker threads based on cluster size\n"
                "  - Configure GC batch size (default: 32, consider 64-128 for large clusters)\n"
                "  - Monitor GC queue depth and adjust max_queue_size if needed\n"
                "  - Enable GC logging for troubleshooting\n"
                "  - Consider GC scheduling during low-traffic periods for large operations",
                metadata={
                    "versioned_buckets_count": len(versioned_buckets),
                    "buckets_with_lifecycle": len(buckets_with_lifecycle),
                    "high_object_count_buckets": len(high_object_count_buckets),
                },
            )

        self.logger.info(
            f"Analysis complete: {len(versioned_buckets)} versioned buckets, "
            f"{len(buckets_with_lifecycle)} with lifecycle policies"
        )

        return self.get_results()

