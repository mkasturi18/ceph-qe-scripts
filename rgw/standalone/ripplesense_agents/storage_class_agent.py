"""
Agent for detecting objects in Standard storage class that could be
placed in lower cost storage classes for efficiency
"""

import logging
from typing import Dict, List

from .base_agent import RipplesenseAgent


class StorageClassAgent(RipplesenseAgent):
    """
    Analyzes objects with Object Lock enabled to identify those in Standard storage class
    that could be moved to lower cost storage classes (e.g., GLACIER, DEEP_ARCHIVE, ERASURE)
    based on access patterns and object age. This agent focuses on compliance objects
    that require Object Lock for regulatory or business requirements.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)
        # Thresholds for recommendations
        self.cold_storage_age_days = 90  # Objects older than this can go to cold storage
        self.large_object_size_mb = 100  # Objects larger than this can use erasure coding

    def analyze(self) -> Dict:
        """
        Analyze objects for storage class optimization opportunities

        Returns:
            Dictionary containing analysis results
        """
        if not self.s3_client:
            if not self.connect():
                return self.get_results()

        self.logger.info("Starting storage class optimization analysis for Object Lock enabled objects...")

        buckets = self.list_buckets()
        self.logger.info(f"Analyzing objects with Object Lock in {len(buckets)} buckets...")

        total_objects_analyzed = 0
        objects_with_object_lock = 0
        objects_in_standard = 0
        optimization_opportunities = []
        buckets_with_object_lock = []

        for bucket_name in buckets:
            try:
                # Check if bucket has Object Lock enabled
                if not self.has_bucket_object_lock_enabled(bucket_name):
                    self.logger.debug(f"Bucket {bucket_name} does not have Object Lock enabled, skipping")
                    continue

                buckets_with_object_lock.append(bucket_name)
                self.logger.info(f"Analyzing Object Lock enabled objects in bucket: {bucket_name}")

                # Sample objects from bucket (analyze first 1000 for performance)
                objects = self.list_objects(bucket_name, max_keys=1000)
                total_objects_analyzed += len(objects)

                for obj in objects:
                    object_key = obj["Key"]
                    size_bytes = obj.get("Size", 0)
                    last_modified = obj.get("LastModified")
                    storage_class = obj.get("StorageClass", "STANDARD")

                    # Check if object has Object Lock enabled
                    if not self.has_object_lock_enabled(bucket_name, object_key):
                        continue  # Skip objects without Object Lock

                    objects_with_object_lock += 1

                    # Get detailed metadata
                    metadata = self.get_object_metadata(bucket_name, object_key)
                    if metadata:
                        storage_class = metadata.get("StorageClass", "STANDARD")

                    if storage_class == "STANDARD" or storage_class is None:
                        objects_in_standard += 1

                        # Calculate object age
                        from datetime import datetime, timezone

                        if last_modified:
                            if isinstance(last_modified, str):
                                # Parse if string - try ISO format first
                                try:
                                    last_modified = datetime.fromisoformat(
                                        last_modified.replace("Z", "+00:00")
                                    )
                                except ValueError:
                                    # Fallback to strptime for common formats
                                    try:
                                        last_modified = datetime.strptime(
                                            last_modified, "%Y-%m-%dT%H:%M:%S.%fZ"
                                        )
                                    except ValueError:
                                        # If parsing fails, skip age calculation
                                        last_modified = None
                            if last_modified:
                                if last_modified.tzinfo is None:
                                    last_modified = last_modified.replace(
                                        tzinfo=timezone.utc
                                    )
                                age_days = (
                                    datetime.now(timezone.utc) - last_modified
                                ).days
                            else:
                                age_days = 0
                        else:
                            age_days = 0

                        size_mb = size_bytes / (1024 * 1024)

                        # Determine optimization recommendation
                        recommendation = None
                        target_storage_class = None
                        estimated_savings = 0

                        if age_days > self.cold_storage_age_days:
                            # Old compliance objects can go to cold storage
                            target_storage_class = "GLACIER"
                            recommendation = (
                                f"Object Lock enabled object '{object_key}' is {age_days} days old and hasn't been accessed. "
                                f"Consider transitioning to GLACIER or DEEP_ARCHIVE storage class for compliance objects."
                            )
                            # Rough estimate: 50-70% cost savings for cold storage
                            estimated_savings = size_mb * 0.6

                        elif size_mb > self.large_object_size_mb:
                            # Large compliance objects can use erasure coding
                            target_storage_class = "ERASURE"
                            recommendation = (
                                f"Object Lock enabled object '{object_key}' is {size_mb:.2f} MB. "
                                f"Consider using ERASURE storage class for better cost efficiency on compliance objects."
                            )
                            # Rough estimate: 30-40% cost savings
                            estimated_savings = size_mb * 0.35

                        if recommendation:
                            optimization_opportunities.append(
                                {
                                    "bucket": bucket_name,
                                    "object_key": object_key,
                                    "size_mb": size_mb,
                                    "age_days": age_days,
                                    "current_storage_class": storage_class or "STANDARD",
                                    "target_storage_class": target_storage_class,
                                    "estimated_savings_mb": estimated_savings,
                                }
                            )

                            # Add warning for optimization opportunity
                            self.add_warning(
                                category="storage_class_optimization",
                                message=recommendation,
                                bucket=bucket_name,
                                object_key=object_key,
                                metadata={
                                    "size_bytes": size_bytes,
                                    "size_mb": round(size_mb, 2),
                                    "age_days": age_days,
                                    "current_storage_class": storage_class or "STANDARD",
                                    "target_storage_class": target_storage_class,
                                    "estimated_savings_mb": round(estimated_savings, 2),
                                    "object_lock_enabled": True,
                                },
                            )

            except Exception as e:
                self.logger.error(f"Error analyzing bucket {bucket_name}: {e}")
                self.add_warning(
                    category="analysis_error",
                    message=f"Failed to analyze objects in bucket '{bucket_name}': {str(e)}",
                    bucket=bucket_name,
                )

        # Generate summary recommendations
        if optimization_opportunities:
            total_potential_savings = sum(
                opp["estimated_savings_mb"] for opp in optimization_opportunities
            )
            total_potential_savings_gb = total_potential_savings / 1024

            self.add_recommendation(
                category="bulk_storage_class_optimization",
                message=f"Found {len(optimization_opportunities)} Object Lock enabled objects that could be optimized. "
                f"Estimated potential savings: {total_potential_savings_gb:.2f} GB/month in storage costs.",
                action="Consider implementing lifecycle policies to automatically transition compliance objects:\n"
                "  - Objects older than 90 days → GLACIER or DEEP_ARCHIVE (maintains Object Lock)\n"
                "  - Large objects (>100MB) → ERASURE storage class\n"
                "  - Review access patterns to determine optimal transition timing\n"
                "  - Ensure Object Lock compliance is maintained during transitions",
                metadata={
                    "total_opportunities": len(optimization_opportunities),
                    "total_potential_savings_gb": round(total_potential_savings_gb, 2),
                    "objects_in_standard": objects_in_standard,
                    "objects_with_object_lock": objects_with_object_lock,
                    "total_objects_analyzed": total_objects_analyzed,
                    "buckets_with_object_lock": len(buckets_with_object_lock),
                },
            )
        elif objects_with_object_lock > 0:
            # Inform that objects were analyzed but no optimization opportunities found
            self.logger.info(
                f"Analyzed {objects_with_object_lock} Object Lock enabled objects, "
                f"all are already optimized or don't meet optimization criteria"
            )

        self.logger.info(
            f"Analysis complete: {total_objects_analyzed} objects scanned, "
            f"{objects_with_object_lock} Object Lock enabled objects found, "
            f"{len(optimization_opportunities)} optimization opportunities identified"
        )

        return self.get_results()

