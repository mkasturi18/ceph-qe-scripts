"""
Agent for validating object naming conventions in S3
Identifies objects with non-recommended characters that may cause
compatibility issues or management complexity
"""

import logging
import re
from typing import Dict, List

from .base_agent import RipplesenseAgent


class ObjectNamingAgent(RipplesenseAgent):
    """
    Validates object naming conventions and identifies objects with
    non-recommended characters that may cause compatibility issues.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)

        # S3 recommended characters: alphanumeric, forward slash, hyphen, underscore, period
        # Non-recommended: special characters, unicode, spaces (though allowed)
        self.recommended_pattern = re.compile(r"^[a-zA-Z0-9._/-]+$")
        self.problematic_patterns = [
            (re.compile(r"\s"), "spaces"),  # Spaces
            (re.compile(r"[^\x00-\x7F]"), "non-ASCII"),  # Non-ASCII characters
            (re.compile(r"[<>:\"|?*]"), "reserved_chars"),  # Windows reserved characters
            (re.compile(r"^\.|\.\.|\.$"), "dot_issues"),  # Leading/trailing dots, double dots
            (re.compile(r"^/|/$"), "leading_trailing_slash"),  # Leading/trailing slashes
        ]

    def analyze(self) -> Dict:
        """
        Analyze object names for naming convention violations

        Returns:
            Dictionary containing analysis results
        """
        if not self.s3_client:
            if not self.connect():
                return self.get_results()

        self.logger.info("Starting object naming validation analysis...")

        buckets = self.list_buckets()
        self.logger.info(f"Analyzing object names in {len(buckets)} buckets...")

        total_objects_analyzed = 0
        problematic_objects: List[Dict] = []

        for bucket_name in buckets:
            try:
                # Sample objects from bucket (analyze first 1000 for performance)
                objects = self.list_objects(bucket_name, max_keys=1000)
                total_objects_analyzed += len(objects)

                for obj in objects:
                    object_key = obj["Key"]

                    # Check for problematic patterns
                    issues = []
                    severity = "low"

                    # Check each problematic pattern
                    for pattern, issue_type in self.problematic_patterns:
                        if pattern.search(object_key):
                            issues.append(issue_type)

                            # Determine severity
                            if issue_type in ["reserved_chars", "non-ASCII"]:
                                severity = "high"
                            elif issue_type in ["leading_trailing_slash", "dot_issues"]:
                                severity = "medium"

                    # Check if object key matches recommended pattern
                    if not self.recommended_pattern.match(object_key):
                        if not issues:  # If no specific issues found, mark as non-standard
                            issues.append("non_standard_chars")

                    if issues:
                        problematic_objects.append(
                            {
                                "bucket": bucket_name,
                                "object_key": object_key,
                                "issues": issues,
                                "severity": severity,
                            }
                        )

                        # Add warning or alert based on severity
                        issue_description = ", ".join(issues)
                        message = (
                            f"Object '{object_key}' contains non-recommended characters: {issue_description}. "
                            f"This may cause compatibility issues with some tools and platforms."
                        )

                        if severity == "high":
                            self.add_alert(
                                severity="high",
                                category="object_naming_violation",
                                message=message,
                                bucket=bucket_name,
                                object_key=object_key,
                                metadata={
                                    "issues": issues,
                                    "severity": severity,
                                    "recommended_format": "Use only: a-z, A-Z, 0-9, ., _, -, /",
                                },
                            )
                        else:
                            self.add_warning(
                                category="object_naming_warning",
                                message=message,
                                bucket=bucket_name,
                                object_key=object_key,
                                metadata={
                                    "issues": issues,
                                    "severity": severity,
                                    "recommended_format": "Use only: a-z, A-Z, 0-9, ., _, -, /",
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
        if problematic_objects:
            high_severity_count = len(
                [obj for obj in problematic_objects if obj["severity"] == "high"]
            )
            medium_severity_count = len(
                [obj for obj in problematic_objects if obj["severity"] == "medium"]
            )

            self.add_recommendation(
                category="object_naming_standards",
                message=f"Found {len(problematic_objects)} objects with naming convention issues "
                f"({high_severity_count} high severity, {medium_severity_count} medium severity). "
                f"Consider renaming objects to follow S3 naming best practices.",
                action="Object naming best practices:\n"
                "  - Use only alphanumeric characters (a-z, A-Z, 0-9)\n"
                "  - Use forward slash (/) for hierarchical organization\n"
                "  - Use hyphen (-), underscore (_), and period (.) as separators\n"
                "  - Avoid spaces, special characters, and non-ASCII characters\n"
                "  - Avoid Windows reserved characters: < > : \" | ? *\n"
                "  - Avoid leading/trailing dots or slashes\n"
                "  - Keep object keys under 1024 characters for compatibility",
                metadata={
                    "total_problematic_objects": len(problematic_objects),
                    "high_severity_count": high_severity_count,
                    "medium_severity_count": medium_severity_count,
                    "total_objects_analyzed": total_objects_analyzed,
                },
            )

        self.logger.info(
            f"Analysis complete: {total_objects_analyzed} objects analyzed, "
            f"{len(problematic_objects)} objects with naming issues found"
        )

        return self.get_results()

