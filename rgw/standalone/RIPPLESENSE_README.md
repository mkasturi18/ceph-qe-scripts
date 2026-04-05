# Ripplesense AI Agent Framework

🌊 **Ripplesense** is an AI-powered framework for analyzing and optimizing Ceph RGW (Rados Gateway) deployments. It provides automated detection and recommendations for common cost management and optimization challenges.

## Quick Start

```bash
cd rgw/standalone
python ripplesense_orchestrator.py \
    --endpoint http://10.1.172.232:5000 \
    --access-key YOUR_ACCESS_KEY \
    --secret-key YOUR_SECRET_KEY \
    --output ripplesense_dashboard.html
```

## What Ripplesense Does

Ripplesense consists of 4 specialized agents that analyze your Ceph RGW deployment:

1. **Versioning & Lifecycle Agent** - Detects versioned buckets without lifecycle policies
2. **Storage Class Optimization Agent** - Identifies optimization opportunities for Object Lock enabled objects
3. **Garbage Collection Config Agent** - Provides GC configuration recommendations for delete-heavy workloads
4. **Object Naming Validation Agent** - Validates object naming conventions

## Features

- ✅ **Connection Verification** - Verifies endpoint connectivity before analysis
- ✅ **Comprehensive Dashboard** - Beautiful HTML dashboard with agent-organized results
- ✅ **Bucket Versioning Status** - Complete view of versioned vs non-versioned buckets
- ✅ **Actionable Recommendations** - Specific steps to optimize your deployment
- ✅ **Cost Impact Analysis** - Estimates potential savings from optimizations

## Documentation

- [Quick Start Guide](RIPPLESENSE_QUICKSTART.md)
- [Agent Documentation](ripplesense_agents/README.md)

## Requirements

- Python 3.7+
- boto3

## License

See LICENSE.md in the repository root.

