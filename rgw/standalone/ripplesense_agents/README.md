# Ripplesense AI Agent Framework

Ripplesense is an AI-powered framework for analyzing and optimizing Ceph RGW (Rados Gateway) deployments. It provides automated detection and recommendations for common cost management and optimization challenges.

## Overview

Ripplesense consists of specialized agents that inspect Ceph RGW deployments and provide recommendations on:

1. **Versioned Buckets without Lifecycle Rules** - Detects buckets with versioning enabled but no lifecycle policies, which can lead to exponential storage cost growth
2. **Storage Class Optimization** - Identifies objects in Standard storage class that could be placed in lower cost storage classes (for Object Lock enabled objects)
3. **GC Configuration Recommendations** - Provides garbage collection configuration recommendations based on bucket usage patterns
4. **Object Naming Validation** - Validates object naming conventions and identifies objects with non-recommended characters

## Architecture

### Base Agent (`base_agent.py`)
The `RipplesenseAgent` base class provides common functionality for all agents:
- S3 connection management
- Alert, warning, and recommendation tracking
- Common bucket and object operations
- Object Lock detection methods
- Result aggregation

### Specialized Agents

#### 1. VersioningLifecycleAgent (`versioning_lifecycle_agent.py`)
- Detects versioned buckets without lifecycle policies
- Estimates potential cost impact
- Provides lifecycle policy recommendations

#### 2. StorageClassAgent (`storage_class_agent.py`)
- Analyzes Object Lock enabled objects for storage class optimization opportunities
- Identifies old objects suitable for cold storage (GLACIER, DEEP_ARCHIVE)
- Recommends erasure coding for large objects
- Calculates potential cost savings

#### 3. GCConfigAgent (`gc_config_agent.py`)
- Analyzes bucket configurations for GC optimization
- Provides recommendations for versioned buckets with lifecycle policies
- Suggests GC configuration tuning for high object count buckets (>10,000 objects)
- Includes recommendations for delete-heavy workloads based on IBM Storage Ceph documentation

#### 4. ObjectNamingAgent (`object_naming_agent.py`)
- Validates object naming conventions
- Detects problematic characters (spaces, non-ASCII, reserved characters)
- Provides naming best practices recommendations

## Installation

### Requirements
- Python 3.7+
- boto3
- Standard library modules (json, logging, datetime, re, etc.)

### Setup

```bash
# Install dependencies
pip install boto3

# Or use the requirements file from parent directory
pip install -r ../requirements.txt
```

## Usage

### Running the Orchestrator

The main orchestrator script runs all agents and generates an HTML dashboard:

```bash
cd rgw/standalone
python ripplesense_orchestrator.py \
    --endpoint http://10.1.172.232:5000 \
    --access-key s3cmduser \
    --secret-key s3cmduser \
    --output ripplesense_dashboard.html
```

### Command Line Options

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
print(results)
```

## Output

### HTML Dashboard
The orchestrator generates a comprehensive HTML dashboard that includes:
- Summary cards with alert/warning/recommendation counts
- Bucket versioning status table
- Agent navigation links
- Detailed warnings and recommendations organized by agent

### JSON Results
Optional JSON output provides structured data for programmatic processing:
```json
{
  "timestamp": "2024-01-01T12:00:00",
  "endpoint": "http://10.1.172.232:5000",
  "connection_status": "success",
  "agents": {
    "versioning_lifecycle": { ... },
    "storage_class": { ... },
    "gc_config": { ... },
    "object_naming": { ... }
  },
  "summary": {
    "total_alerts": 10,
    "total_warnings": 25,
    "total_recommendations": 5
  }
}
```

## Features

### Connection Verification
- Verifies connection to S3 endpoint before running agents
- Provides clear error messages if connection fails
- Prevents wasted time on unreachable endpoints

### Agent-Specific Analysis

#### Versioning & Lifecycle Agent
- Detects versioned buckets without lifecycle policies
- Identifies suspended versioning buckets
- Provides lifecycle policy templates

#### Storage Class Optimization Agent
- Focuses on Object Lock enabled objects (compliance objects)
- Recommends cold storage for old objects (>90 days)
- Suggests erasure coding for large objects (>100MB)

#### GC Config Agent
- Analyzes bucket object counts
- Provides GC tuning recommendations for delete-heavy workloads
- References IBM Storage Ceph documentation for optimal settings

#### Object Naming Agent
- Validates S3 object naming conventions
- Detects problematic characters and patterns
- Provides remediation recommendations

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

## License

See LICENSE.md in the repository root.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## References

- [IBM Storage Ceph - Adjusting Garbage Collection for Delete-Heavy Workloads](https://www.ibm.com/docs/en/storage-ceph/8.1.0?topic=collection-adjusting-garbage-delete-heavy-workloads)
- Ceph RGW documentation
- S3 lifecycle policy best practices
