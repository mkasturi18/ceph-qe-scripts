#!/usr/bin/env python3
"""
Ripplesense AI Agent Orchestrator
Main script to run all Ripplesense agents and generate HTML dashboard

This script orchestrates all Ripplesense agents to analyze Ceph RGW deployments
and generate a comprehensive HTML dashboard with alerts and warnings.

Usage:
    python ripplesense_orchestrator.py --endpoint <endpoint> --access-key <key> --secret-key <secret> [options]

Example:
    python ripplesense_orchestrator.py --endpoint http://10.1.172.232:5000 --access-key s3cmduser --secret-key s3cmduser
"""

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

from ripplesense_agents import (
    GCConfigAgent,
    ObjectNamingAgent,
    StorageClassAgent,
    VersioningLifecycleAgent,
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


class RipplesenseOrchestrator:
    """Orchestrates all Ripplesense agents and generates reports"""

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        region: str = "us-east-1",
        use_ssl: bool = False,
    ):
        """
        Initialize the orchestrator

        Args:
            endpoint: RGW endpoint URL
            access_key: AWS access key ID
            secret_key: AWS secret access key
            region: AWS region
            use_ssl: Whether to use SSL
        """
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.use_ssl = use_ssl

        # Initialize all agents
        agent_kwargs = {
            "endpoint": endpoint,
            "access_key": access_key,
            "secret_key": secret_key,
            "region": region,
            "use_ssl": use_ssl,
        }

        self.agents = {
            "versioning_lifecycle": VersioningLifecycleAgent(**agent_kwargs),
            "storage_class": StorageClassAgent(**agent_kwargs),
            "gc_config": GCConfigAgent(**agent_kwargs),
            "object_naming": ObjectNamingAgent(**agent_kwargs),
        }

    def verify_connection(self) -> bool:
        """
        Verify connection to the S3 endpoint before running agents
        
        Returns:
            True if connection is successful, False otherwise
        """
        log.info("Verifying connection to endpoint...")
        
        # Use the first agent to test connection (all agents share same connection parameters)
        agent = list(self.agents.values())[0]
        
        try:
            if not agent.connect():
                log.error(f"Failed to connect to endpoint: {self.endpoint}")
                return False
            
            # Try to list buckets as a connection test
            try:
                buckets = agent.list_buckets()
                log.info(f"Connection verified successfully. Found {len(buckets)} accessible bucket(s)")
                return True
            except Exception as e:
                log.error(f"Connection test failed - unable to list buckets: {e}")
                return False
                
        except Exception as e:
            log.error(f"Connection verification failed: {e}")
            return False

    def collect_bucket_versioning_info(self) -> dict:
        """
        Collect versioning information for all buckets

        Returns:
            Dictionary containing bucket versioning status
        """
        log.info("Collecting bucket versioning information...")
        
        # Use the first agent to get S3 client (all agents share same connection)
        agent = list(self.agents.values())[0]
        if not agent.s3_client:
            if not agent.connect():
                log.error("Failed to connect for bucket versioning collection")
                return {"buckets": [], "versioned": [], "non_versioned": [], "suspended": []}
        
        buckets_info = {
            "buckets": [],
            "versioned": [],
            "non_versioned": [],
            "suspended": []
        }
        
        try:
            buckets = agent.list_buckets()
            log.info(f"Found {len(buckets)} buckets to analyze")
            
            for bucket_name in buckets:
                try:
                    versioning = agent.get_bucket_versioning(bucket_name)
                    versioning_status = versioning.get("Status", "Disabled")
                    
                    bucket_info = {
                        "name": bucket_name,
                        "versioning_status": versioning_status,
                        "has_lifecycle": False
                    }
                    
                    # Check for lifecycle configuration
                    lifecycle = agent.get_bucket_lifecycle(bucket_name)
                    if lifecycle and lifecycle.get("Rules"):
                        bucket_info["has_lifecycle"] = True
                        bucket_info["lifecycle_rules_count"] = len(lifecycle.get("Rules", []))
                    
                    buckets_info["buckets"].append(bucket_info)
                    
                    if versioning_status == "Enabled":
                        buckets_info["versioned"].append(bucket_info)
                    elif versioning_status == "Suspended":
                        buckets_info["suspended"].append(bucket_info)
                    else:
                        buckets_info["non_versioned"].append(bucket_info)
                        
                except Exception as e:
                    log.warning(f"Error getting versioning for bucket {bucket_name}: {e}")
                    # Add bucket with unknown status
                    bucket_info = {
                        "name": bucket_name,
                        "versioning_status": "Unknown",
                        "has_lifecycle": False,
                        "error": str(e)
                    }
                    buckets_info["buckets"].append(bucket_info)
                    buckets_info["non_versioned"].append(bucket_info)
            
            log.info(f"Bucket analysis complete: {len(buckets_info['versioned'])} versioned, "
                    f"{len(buckets_info['non_versioned'])} non-versioned, "
                    f"{len(buckets_info['suspended'])} suspended")
            
        except Exception as e:
            log.error(f"Error collecting bucket versioning info: {e}")
        
        return buckets_info

    def run_all_agents(self) -> dict:
        """
        Run all agents and collect results

        Returns:
            Dictionary containing results from all agents
        """
        log.info("=" * 80)
        log.info("Ripplesense AI Agent Framework - Starting Analysis")
        log.info("=" * 80)

        all_results = {
            "timestamp": datetime.now().isoformat(),
            "endpoint": self.endpoint,
            "agents": {},
            "buckets": {},
            "connection_status": "unknown",
            "summary": {
                "total_alerts": 0,
                "total_warnings": 0,
                "total_recommendations": 0,
                "critical_alerts": 0,
                "high_alerts": 0,
                "medium_alerts": 0,
                "low_alerts": 0,
            },
        }
        
        # Verify connection before proceeding
        if not self.verify_connection():
            log.error("=" * 80)
            log.error("Connection verification failed. Cannot proceed with agent analysis.")
            log.error("=" * 80)
            all_results["connection_status"] = "failed"
            all_results["connection_error"] = f"Failed to connect to endpoint: {self.endpoint}"
            
            # Mark all agents as failed
            for agent_name in self.agents.keys():
                all_results["agents"][agent_name] = {
                    "error": f"Connection verification failed. Unable to connect to endpoint: {self.endpoint}",
                    "alerts": [],
                    "warnings": [],
                    "recommendations": [],
                }
            
            return all_results
        
        all_results["connection_status"] = "success"
        log.info("Connection verified. Proceeding with agent analysis...")
        
        # Collect bucket versioning information
        all_results["buckets"] = self.collect_bucket_versioning_info()

        for agent_name, agent in self.agents.items():
            log.info(f"\n{'=' * 80}")
            log.info(f"Running Agent: {agent_name}")
            log.info(f"{'=' * 80}")

            try:
                # Connect agent
                if not agent.connect():
                    log.error(f"Failed to connect agent {agent_name}")
                    continue

                # Run analysis
                results = agent.analyze()
                all_results["agents"][agent_name] = results

                # Update summary
                summary = results.get("summary", {})
                all_results["summary"]["total_alerts"] += summary.get(
                    "total_alerts", 0
                )
                all_results["summary"]["total_warnings"] += summary.get(
                    "total_warnings", 0
                )
                all_results["summary"]["total_recommendations"] += summary.get(
                    "total_recommendations", 0
                )
                all_results["summary"]["critical_alerts"] += summary.get(
                    "critical_alerts", 0
                )
                all_results["summary"]["high_alerts"] += summary.get("high_alerts", 0)

                # Count medium and low alerts
                alerts = results.get("alerts", [])
                for alert in alerts:
                    severity = alert.get("severity", "low")
                    if severity == "medium":
                        all_results["summary"]["medium_alerts"] += 1
                    elif severity == "low":
                        all_results["summary"]["low_alerts"] += 1

                log.info(
                    f"Agent {agent_name} completed: "
                    f"{summary.get('total_alerts', 0)} alerts, "
                    f"{summary.get('total_warnings', 0)} warnings, "
                    f"{summary.get('total_recommendations', 0)} recommendations"
                )

            except Exception as e:
                log.error(f"Error running agent {agent_name}: {e}", exc_info=True)
                all_results["agents"][agent_name] = {
                    "error": str(e),
                    "alerts": [],
                    "warnings": [],
                    "recommendations": [],
                }

        log.info(f"\n{'=' * 80}")
        log.info("Analysis Complete")
        log.info(f"{'=' * 80}")
        log.info(f"Total Alerts: {all_results['summary']['total_alerts']}")
        log.info(f"Total Warnings: {all_results['summary']['total_warnings']}")
        log.info(
            f"Total Recommendations: {all_results['summary']['total_recommendations']}"
        )

        return all_results

    def generate_html_dashboard(self, results: dict, output_file: str):
        """
        Generate HTML dashboard from agent results

        Args:
            results: Results dictionary from run_all_agents()
            output_file: Path to output HTML file
        """
        log.info(f"Generating HTML dashboard: {output_file}")

        # Get bucket information
        buckets_info = results.get("buckets", {})
        
        # Generate HTML - organize by agents
        html_content = self._generate_html_content(
            results, buckets_info
        )

        # Write to file
        try:
            with open(output_file, "w", encoding="utf-8") as f:
                f.write(html_content)
            log.info(f"HTML dashboard saved to: {output_file}")
        except Exception as e:
            log.error(f"Failed to write HTML dashboard: {e}")
            raise

    def _generate_html_content(
        self, results: dict, buckets_info: dict
    ) -> str:
        """Generate HTML content for the dashboard organized by agents"""
        summary = results.get("summary", {})
        timestamp = results.get("timestamp", datetime.now().isoformat())

        # Format timestamp
        try:
            dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            formatted_timestamp = dt.strftime("%Y-%m-%d %H:%M:%S UTC")
        except:
            formatted_timestamp = timestamp

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Ripplesense AI Agent - Ceph RGW Optimization Dashboard</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
            color: #333;
        }}

        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 20px;
            box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
            padding: 40px;
        }}

        .header {{
            text-align: center;
            margin-bottom: 40px;
            padding-bottom: 20px;
            border-bottom: 3px solid #4CAF50;
        }}

        .header h1 {{
            color: #333;
            font-size: 2.5em;
            margin-bottom: 10px;
        }}

        .header .subtitle {{
            color: #666;
            font-size: 1.2em;
        }}

        .info-section {{
            background-color: #e3f2fd;
            padding: 20px;
            border-radius: 10px;
            margin-bottom: 30px;
        }}

        .info-section p {{
            margin: 8px 0;
            color: #333;
        }}

        .info-section strong {{
            color: #1976d2;
        }}

        .summary-cards {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 40px;
        }}

        .summary-card {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 25px;
            border-radius: 10px;
            text-align: center;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }}

        .summary-card.critical {{
            background: linear-gradient(135deg, #f44336 0%, #d32f2f 100%);
        }}

        .summary-card.high {{
            background: linear-gradient(135deg, #ff9800 0%, #f57c00 100%);
        }}

        .summary-card.warning {{
            background: linear-gradient(135deg, #ffc107 0%, #ffa000 100%);
        }}

        .summary-card.success {{
            background: linear-gradient(135deg, #4CAF50 0%, #388e3c 100%);
        }}

        .summary-card .value {{
            font-size: 48px;
            font-weight: bold;
            margin: 10px 0;
        }}

        .summary-card .label {{
            font-size: 18px;
            opacity: 0.9;
        }}

        .section {{
            margin-bottom: 40px;
        }}

        .section-title {{
            font-size: 1.8em;
            color: #333;
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 2px solid #4CAF50;
        }}

        .alert-item, .warning-item, .recommendation-item {{
            background: #f9f9f9;
            border-left: 4px solid #4CAF50;
            padding: 20px;
            margin-bottom: 15px;
            border-radius: 5px;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
        }}

        .alert-item.critical {{
            border-left-color: #f44336;
            background: #ffebee;
        }}

        .alert-item.high {{
            border-left-color: #ff9800;
            background: #fff3e0;
        }}

        .alert-item.medium {{
            border-left-color: #ffc107;
            background: #fffde7;
        }}

        .alert-item.low {{
            border-left-color: #2196f3;
            background: #e3f2fd;
        }}

        .warning-item {{
            border-left-color: #ff9800;
            background: #fff3e0;
        }}

        .recommendation-item {{
            border-left-color: #4CAF50;
            background: #e8f5e9;
        }}

        .item-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 10px;
        }}

        .item-title {{
            font-weight: bold;
            font-size: 1.1em;
            color: #333;
        }}

        .item-meta {{
            font-size: 0.9em;
            color: #666;
        }}

        .item-message {{
            margin: 10px 0;
            line-height: 1.6;
        }}

        .item-metadata {{
            margin-top: 10px;
            padding-top: 10px;
            border-top: 1px solid #ddd;
            font-size: 0.9em;
            color: #666;
        }}

        .badge {{
            display: inline-block;
            padding: 4px 8px;
            border-radius: 4px;
            font-size: 0.8em;
            font-weight: bold;
            margin-left: 10px;
        }}

        .badge.critical {{
            background: #f44336;
            color: white;
        }}

        .badge.high {{
            background: #ff9800;
            color: white;
        }}

        .badge.medium {{
            background: #ffc107;
            color: #333;
        }}

        .badge.low {{
            background: #2196f3;
            color: white;
        }}

        .badge.agent {{
            background: #9c27b0;
            color: white;
        }}

        .no-items {{
            text-align: center;
            padding: 40px;
            color: #999;
            font-size: 1.2em;
        }}

        .footer {{
            text-align: center;
            color: #777;
            margin-top: 40px;
            padding-top: 20px;
            border-top: 1px solid #ddd;
            font-size: 0.9em;
        }}

        .action-box {{
            background: #fff3cd;
            border: 1px solid #ffc107;
            border-radius: 5px;
            padding: 15px;
            margin-top: 10px;
            white-space: pre-wrap;
            font-family: 'Courier New', monospace;
            font-size: 0.9em;
        }}

        .bucket-table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }}

        .bucket-table thead {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
        }}

        .bucket-table th {{
            padding: 15px;
            text-align: left;
            font-weight: 600;
            font-size: 16px;
        }}

        .bucket-table td {{
            padding: 12px 15px;
            border-bottom: 1px solid #e0e0e0;
        }}

        .bucket-table tbody tr {{
            transition: background-color 0.2s ease;
        }}

        .bucket-table tbody tr:hover {{
            background-color: #f5f5f5;
        }}

        .bucket-table tbody tr:nth-child(even) {{
            background-color: #fafafa;
        }}

        .status-badge {{
            display: inline-block;
            padding: 4px 12px;
            border-radius: 12px;
            font-size: 0.85em;
            font-weight: bold;
        }}

        .status-badge.enabled {{
            background: #4CAF50;
            color: white;
        }}

        .status-badge.disabled {{
            background: #9e9e9e;
            color: white;
        }}

        .status-badge.suspended {{
            background: #ff9800;
            color: white;
        }}

        .status-badge.unknown {{
            background: #757575;
            color: white;
        }}

        .lifecycle-badge {{
            display: inline-block;
            padding: 4px 8px;
            border-radius: 4px;
            font-size: 0.8em;
            background: #4CAF50;
            color: white;
            margin-left: 8px;
        }}

        .bucket-stats {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 15px;
            margin-bottom: 20px;
        }}

        .bucket-stat-card {{
            background: #f5f5f5;
            padding: 15px;
            border-radius: 8px;
            text-align: center;
            border-left: 4px solid #667eea;
        }}

        .bucket-stat-card .stat-value {{
            font-size: 32px;
            font-weight: bold;
            color: #333;
            margin: 5px 0;
        }}

        .bucket-stat-card .stat-label {{
            font-size: 14px;
            color: #666;
        }}

        .agent-section {{
            margin-bottom: 50px;
            border: 2px solid #e0e0e0;
            border-radius: 10px;
            padding: 25px;
            background: #fafafa;
        }}

        .agent-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 25px;
            padding-bottom: 15px;
            border-bottom: 2px solid #4CAF50;
        }}

        .agent-title {{
            font-size: 1.5em;
            color: #333;
            font-weight: bold;
        }}

        .agent-description {{
            color: #666;
            font-size: 0.95em;
            margin-top: 5px;
            font-style: italic;
        }}

        .agent-stats {{
            display: flex;
            gap: 15px;
            margin-bottom: 20px;
            flex-wrap: wrap;
        }}

        .agent-stat-badge {{
            padding: 8px 16px;
            border-radius: 20px;
            font-size: 0.9em;
            font-weight: bold;
        }}

        .agent-stat-badge.alerts {{
            background: #ffebee;
            color: #c62828;
            border: 1px solid #ef5350;
        }}

        .agent-stat-badge.warnings {{
            background: #fff3e0;
            color: #e65100;
            border: 1px solid #ff9800;
        }}

        .agent-stat-badge.recommendations {{
            background: #e8f5e9;
            color: #2e7d32;
            border: 1px solid #4CAF50;
        }}

        .agent-subsection {{
            margin-top: 25px;
        }}

        .agent-subsection-title {{
            font-size: 1.2em;
            color: #333;
            margin-bottom: 15px;
            padding-bottom: 8px;
            border-bottom: 1px solid #ddd;
        }}

        .agent-navigation {{
            background: #f5f5f5;
            padding: 25px;
            border-radius: 10px;
            margin-bottom: 40px;
            border: 2px solid #e0e0e0;
        }}

        .agent-navigation-title {{
            font-size: 1.5em;
            color: #333;
            margin-bottom: 20px;
            font-weight: bold;
        }}

        .agent-nav-links {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 15px;
        }}

        .agent-nav-link {{
            display: block;
            padding: 15px 20px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            text-decoration: none;
            border-radius: 8px;
            transition: transform 0.2s ease, box-shadow 0.2s ease;
            font-weight: 500;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
        }}

        .agent-nav-link:hover {{
            transform: translateY(-2px);
            box-shadow: 0 4px 8px rgba(0, 0, 0, 0.2);
            text-decoration: none;
            color: white;
        }}

        .agent-nav-link:visited {{
            color: white;
        }}

        @media print {{
            body {{
                background: white;
                padding: 0;
            }}
            .container {{
                box-shadow: none;
                border-radius: 0;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🌊 Ripplesense AI Agent</h1>
            <div class="subtitle">Ceph RGW Optimization & Cost Management Dashboard</div>
        </div>

        <div class="info-section">
            <p><strong>Generated:</strong> {formatted_timestamp}</p>
            <p><strong>Endpoint:</strong> {results.get('endpoint', 'N/A')}</p>
            <p><strong>Connection Status:</strong> 
                <span style="color: {'#4CAF50' if results.get('connection_status') == 'success' else '#f44336'}; font-weight: bold;">
                    {results.get('connection_status', 'unknown').upper()}
                </span>
            </p>
            <p><strong>Analysis Status:</strong> {'Complete' if results.get('connection_status') == 'success' else 'Failed - Connection Error'}</p>
            {f"<p><strong>Error:</strong> <span style='color: #f44336;'>{results.get('connection_error', '')}</span></p>" if results.get('connection_status') == 'failed' else ''}
        </div>

        <div class="summary-cards">
            <div class="summary-card success">
                <div class="value">{len(buckets_info.get('buckets', []))}</div>
                <div class="label">Total Buckets</div>
            </div>
            <div class="summary-card critical">
                <div class="value">{summary.get('critical_alerts', 0)}</div>
                <div class="label">Critical Alerts</div>
            </div>
            <div class="summary-card high">
                <div class="value">{summary.get('high_alerts', 0)}</div>
                <div class="label">High Alerts</div>
            </div>
            <div class="summary-card warning">
                <div class="value">{summary.get('total_warnings', 0)}</div>
                <div class="label">Warnings</div>
            </div>
            <div class="summary-card success">
                <div class="value">{summary.get('total_recommendations', 0)}</div>
                <div class="label">Recommendations</div>
            </div>
        </div>
"""

        # Bucket Versioning Section
        versioned_buckets = buckets_info.get("versioned", [])
        non_versioned_buckets = buckets_info.get("non_versioned", [])
        suspended_buckets = buckets_info.get("suspended", [])
        all_buckets = buckets_info.get("buckets", [])
        total_buckets = len(all_buckets)
        
        html += """
        <div class="section">
            <h2 class="section-title">📦 Bucket Versioning Status</h2>
"""
        
        if total_buckets > 0:
            html += f"""
            <div class="bucket-stats">
                <div class="bucket-stat-card">
                    <div class="stat-value">{total_buckets}</div>
                    <div class="stat-label">Total Buckets</div>
                </div>
                <div class="bucket-stat-card">
                    <div class="stat-value">{len(versioned_buckets)}</div>
                    <div class="stat-label">Versioned</div>
                </div>
                <div class="bucket-stat-card">
                    <div class="stat-value">{len(non_versioned_buckets)}</div>
                    <div class="stat-label">Non-Versioned</div>
                </div>
                <div class="bucket-stat-card">
                    <div class="stat-value">{len(suspended_buckets)}</div>
                    <div class="stat-label">Suspended</div>
                </div>
            </div>
            
            <table class="bucket-table">
                <thead>
                    <tr>
                        <th>Bucket Name</th>
                        <th>Versioning Status</th>
                        <th>Lifecycle Policy</th>
                        <th>Notes</th>
                    </tr>
                </thead>
                <tbody>
"""
            
            # Sort buckets: versioned first, then by name
            sorted_buckets = sorted(
                all_buckets,
                key=lambda x: (
                    0 if x.get("versioning_status") == "Enabled" else
                    1 if x.get("versioning_status") == "Suspended" else
                    2,
                    x.get("name", "")
                )
            )
            
            for bucket in sorted_buckets:
                bucket_name = bucket.get("name", "N/A")
                versioning_status = bucket.get("versioning_status", "Unknown")
                has_lifecycle = bucket.get("has_lifecycle", False)
                lifecycle_rules_count = bucket.get("lifecycle_rules_count", 0)
                error = bucket.get("error")
                
                # Determine status badge class
                status_class = "disabled"
                if versioning_status == "Enabled":
                    status_class = "enabled"
                elif versioning_status == "Suspended":
                    status_class = "suspended"
                elif versioning_status == "Unknown":
                    status_class = "unknown"
                
                # Notes column
                notes = []
                if versioning_status == "Enabled" and not has_lifecycle:
                    notes.append("⚠️ No lifecycle policy")
                elif has_lifecycle:
                    notes.append(f"✓ {lifecycle_rules_count} lifecycle rule(s)")
                if error:
                    notes.append(f"Error: {error}")
                
                notes_text = " | ".join(notes) if notes else "-"
                
                html += f"""
                    <tr>
                        <td class="bucket-name">{bucket_name}</td>
                        <td>
                            <span class="status-badge {status_class}">{versioning_status}</span>
                        </td>
                        <td>
                            {"Yes" if has_lifecycle else "No"}
                            {f'<span class="lifecycle-badge">{lifecycle_rules_count} rule(s)</span>' if has_lifecycle else ''}
                        </td>
                        <td>{notes_text}</td>
                    </tr>
"""
            
            html += """
                </tbody>
            </table>
"""
        else:
            html += '            <div class="no-items">No buckets found or unable to retrieve bucket information.</div>\n'
        
        html += "        </div>\n"

        # Agent Navigation Links Section
        agents_data = results.get("agents", {})
        # Define agent display names dictionary (outside f-string to avoid syntax errors)
        agent_display_names = {
            "versioning_lifecycle": "Versioning & Lifecycle Agent",
            "storage_class": "Storage Class Optimization Agent",
            "gc_config": "Garbage Collection Config Agent",
            "object_naming": "Object Naming Validation Agent",
        }
        
        # Create navigation links for all agents
        html += """
        <div class="agent-navigation">
            <div class="agent-navigation-title">🔗 Agent Analysis Results</div>
            <div class="agent-nav-links">
"""
        
        for agent_name in agent_display_names.keys():
            agent_id = f"agent-{agent_name}"
            display_name = agent_display_names[agent_name]
            html += f'                <a href="#{agent_id}" class="agent-nav-link">{display_name}</a>\n'
        
        html += """
            </div>
        </div>
"""
        
        # Agent Results Sections - Organized by Agent
        # Process all agents in display_names to ensure all are shown
        for agent_name in agent_display_names.keys():
            agent_id = f"agent-{agent_name}"
            agent_results = agents_data.get(agent_name, {})
            agent_warnings = agent_results.get("warnings", [])
            agent_recommendations = agent_results.get("recommendations", [])
            agent_description = agent_results.get("agent_description", "No description available")
            
            total_items = len(agent_warnings) + len(agent_recommendations)
            
            if "error" in agent_results:
                # Show error for agent
                html += f"""
        <div class="agent-section" id="{agent_id}">
            <div class="agent-header">
                <div>
                    <div class="agent-title">{agent_display_names.get(agent_name, agent_name.replace('_', ' ').title())}</div>
                    <div class="agent-description">Error during analysis</div>
                </div>
            </div>
            <div class="no-items">Error: {agent_results.get('error', 'Unknown error')}</div>
        </div>
"""
                continue
            
            html += f"""
        <div class="agent-section" id="{agent_id}">
            <div class="agent-header">
                <div>
                    <div class="agent-title">{agent_display_names.get(agent_name, agent_name.replace('_', ' ').title())}</div>
                    <div class="agent-description">{agent_description}</div>
                </div>
            </div>
            
            <div class="agent-stats">
                <span class="agent-stat-badge warnings">
                    {len(agent_warnings)} Warning{'s' if len(agent_warnings) != 1 else ''}
                </span>
                <span class="agent-stat-badge recommendations">
                    {len(agent_recommendations)} Recommendation{'s' if len(agent_recommendations) != 1 else ''}
                </span>
            </div>
"""
            
            if total_items == 0:
                # Show message if no results
                html += """
            <div class="no-items">✅ No warnings or recommendations. All checks passed!</div>
"""
            
            # Warnings for this agent
            if agent_warnings:
                html += """
            <div class="agent-subsection">
                <h3 class="agent-subsection-title">⚠️ Warnings</h3>
"""
                for warning in agent_warnings:
                    category = warning.get("category", "unknown")
                    message = warning.get("message", "")
                    bucket = warning.get("bucket", "")
                    object_key = warning.get("object_key", "")
                    metadata = warning.get("metadata", {})

                    html += f"""
                <div class="warning-item">
                    <div class="item-header">
                        <div class="item-title">
                            {category.replace('_', ' ').title()}
                        </div>
                        <div class="item-meta">{warning.get('timestamp', '')[:19]}</div>
                    </div>
                    <div class="item-message">{message}</div>
"""
                    if bucket:
                        html += f'                    <div class="item-metadata"><strong>Bucket:</strong> {bucket}</div>\n'
                    if object_key:
                        html += f'                    <div class="item-metadata"><strong>Object:</strong> {object_key}</div>\n'
                    if metadata:
                        html += f'                    <div class="item-metadata"><strong>Details:</strong> {json.dumps(metadata, indent=2)}</div>\n'
                    html += "                </div>\n"
                html += "            </div>\n"
            
            # Recommendations for this agent
            if agent_recommendations:
                html += """
            <div class="agent-subsection">
                <h3 class="agent-subsection-title">💡 Recommendations</h3>
"""
                for rec in agent_recommendations:
                    category = rec.get("category", "unknown")
                    message = rec.get("message", "")
                    action = rec.get("action", "")
                    bucket = rec.get("bucket", "")
                    metadata = rec.get("metadata", {})

                    html += f"""
                <div class="recommendation-item">
                    <div class="item-header">
                        <div class="item-title">
                            {category.replace('_', ' ').title()}
                        </div>
                        <div class="item-meta">{rec.get('timestamp', '')[:19]}</div>
                    </div>
                    <div class="item-message">{message}</div>
"""
                    if action:
                        html += f'                    <div class="action-box"><strong>Recommended Action:</strong>\n{action}</div>\n'
                    if bucket:
                        html += f'                    <div class="item-metadata"><strong>Bucket:</strong> {bucket}</div>\n'
                    if metadata:
                        html += f'                    <div class="item-metadata"><strong>Details:</strong> {json.dumps(metadata, indent=2)}</div>\n'
                    html += "                </div>\n"
                html += "            </div>\n"
            
            html += "        </div>\n"

        # Footer
        html += f"""
        <div class="footer">
            <p>Report generated by Ripplesense AI Agent Framework</p>
            <p>For more information, visit the Ceph RGW documentation</p>
        </div>
    </div>
</body>
</html>
"""

        return html


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="Ripplesense AI Agent Orchestrator - Ceph RGW Optimization",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Required arguments
    parser.add_argument(
        "--endpoint",
        required=True,
        help="RGW endpoint URL (e.g., http://10.1.172.232:5000)",
    )
    parser.add_argument(
        "--access-key",
        required=True,
        help="AWS access key ID",
    )
    parser.add_argument(
        "--secret-key",
        required=True,
        help="AWS secret access key",
    )

    # Optional arguments
    parser.add_argument(
        "--region",
        default="us-east-1",
        help="AWS region (default: us-east-1)",
    )
    parser.add_argument(
        "--use-ssl",
        action="store_true",
        help="Use SSL for connection",
    )
    parser.add_argument(
        "--output",
        default="ripplesense_dashboard.html",
        help="Output HTML file path (default: ripplesense_dashboard.html)",
    )
    parser.add_argument(
        "--json-output",
        help="Optional JSON output file path",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Set logging level (default: INFO)",
    )

    args = parser.parse_args()

    # Set logging level
    log.setLevel(getattr(logging, args.log_level.upper()))

    try:
        # Initialize orchestrator
        orchestrator = RipplesenseOrchestrator(
            endpoint=args.endpoint,
            access_key=args.access_key,
            secret_key=args.secret_key,
            region=args.region,
            use_ssl=args.use_ssl,
        )

        # Run all agents
        results = orchestrator.run_all_agents()

        # Generate HTML dashboard
        orchestrator.generate_html_dashboard(results, args.output)

        # Optionally save JSON results
        if args.json_output:
            with open(args.json_output, "w", encoding="utf-8") as f:
                json.dump(results, f, indent=2, default=str)
            log.info(f"JSON results saved to: {args.json_output}")

        log.info("=" * 80)
        log.info("Ripplesense analysis complete!")
        log.info(f"Dashboard: {args.output}")
        log.info("=" * 80)

        sys.exit(0)

    except KeyboardInterrupt:
        log.info("Interrupted by user")
        sys.exit(130)
    except Exception as e:
        log.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

