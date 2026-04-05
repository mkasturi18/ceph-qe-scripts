# Ripplesense AI Agent - Quick Start Guide

## Overview

Ripplesense is an AI-powered framework for analyzing and optimizing Ceph RGW deployments. It automatically detects common issues and provides actionable recommendations.

## Installation

No special installation required. The framework uses standard Python libraries and boto3.

## Quick Start

### 1. Run the Orchestrator

```bash
cd rgw/standalone
python ripplesense_orchestrator.py \
    --endpoint http://10.1.172.232:5000 \
    --access-key s3cmduser \
    --secret-key s3cmduser \
    --output ripplesense_dashboard.html
```

### 2. View the Dashboard

Open the generated `ripplesense_dashboard.html` file in your web browser to view:
- **Alerts**: Critical and high-priority issues requiring attention
- **Warnings**: Issues that should be monitored
- **Recommendations**: Actionable optimization suggestions

## What Ripplesense Detects

### 1. Versioned Buckets without Lifecycle Rules
- **Issue**: Versioned buckets without lifecycle policies accumulate delete markers and object versions indefinitely
- **Impact**: Exponential storage cost growth, OMAP performance issues
- **Recommendation**: Configure lifecycle policies to expire old versions and delete markers

### 2. Storage Class Optimization Opportunities
- **Issue**: Objects in Standard storage class that could be in lower cost tiers
- **Impact**: Unnecessary premium storage costs
- **Recommendation**: Transition old objects (>90 days) to GLACIER/DEEP_ARCHIVE, large objects to ERASURE

### 3. GC Configuration Recommendations
- **Issue**: Suboptimal garbage collection configuration for bulk operations
- **Impact**: Performance degradation during bulk deletes
- **Recommendation**: Tune GC settings based on bucket usage patterns

### 4. Object Naming Issues
- **Issue**: Objects with non-recommended characters (spaces, special chars, non-ASCII)
- **Impact**: Compatibility issues with tools and platforms
- **Recommendation**: Follow S3 naming best practices

## Command Line Options

```
--endpoint ENDPOINT     RGW endpoint URL (required)
--access-key KEY        AWS access key ID (required)
--secret-key SECRET     AWS secret access key (required)
--region REGION         AWS region (default: us-east-1)
--use-ssl               Use SSL for connection
--output OUTPUT         Output HTML file (default: ripplesense_dashboard.html)
--json-output FILE      Optional JSON output file
--log-level LEVEL       Logging level: DEBUG, INFO, WARNING, ERROR (default: INFO)
```

## Example Output

The HTML dashboard includes:

1. **Summary Cards**: Quick overview of alerts, warnings, and recommendations
2. **Alerts Section**: Detailed critical and high-priority issues
3. **Warnings Section**: Issues to monitor
4. **Recommendations Section**: Actionable optimization steps

## Integration

### Using Individual Agents

```python
from ripplesense_agents import VersioningLifecycleAgent

agent = VersioningLifecycleAgent(
    endpoint="http://10.1.172.232:5000",
    access_key="s3cmduser",
    secret_key="s3cmduser"
)

agent.connect()
results = agent.analyze()

# Access results
for alert in results['alerts']:
    print(f"Alert: {alert['message']}")
```

### Programmatic Access

```python
from ripplesense_orchestrator import RipplesenseOrchestrator

orchestrator = RipplesenseOrchestrator(
    endpoint="http://10.1.172.232:5000",
    access_key="s3cmduser",
    secret_key="s3cmduser"
)

results = orchestrator.run_all_agents()
orchestrator.generate_html_dashboard(results, "dashboard.html")
```

## Best Practices

1. **Run Regularly**: Schedule Ripplesense to run periodically (daily/weekly)
2. **Act on Critical Alerts**: Address critical and high severity alerts promptly
3. **Review Recommendations**: Implement lifecycle policies and storage class optimizations
4. **Monitor Trends**: Track alert counts over time to measure improvement

## Troubleshooting

### Connection Issues
- Verify endpoint URL is correct and accessible
- Check access key and secret key credentials
- Ensure network connectivity to RGW endpoint

### No Results
- Verify buckets exist and are accessible
- Check IAM permissions for bucket listing and metadata access
- Review log output for specific errors

## Support

For more information, see:
- `ripplesense_agents/README.md` - Detailed agent documentation
- Ceph RGW documentation
- S3 lifecycle policy best practices

