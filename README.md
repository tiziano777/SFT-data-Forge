# Dataset Management Platform for LLM Training

A comprehensive platform for managing the complete lifecycle of datasets used in Large Language Model training, from ingestion to final mapped datasets ready for fine-tuning.

## Overview

This platform implements a **3-layer architecture** for dataset management, providing a systematic approach to ETL (Extract, Transform, Load) operations for ML/NLP workflows. It's designed to be storage-agnostic, working seamlessly with local disks, NFS mounts, or distributed storage systems.

## Architecture

### Three-Layer Dataset Structure
📁 RAW →             📁 PROCESSED →         📁 MAPPED
│                   │                       │
├─ Web scrapings    ├─ raw + core_metadata  ├─ Custom schema template 
├─ API downloads    ├─ standard_format      └─ Trainable datasets
├─ HF datasets      ├─ *Deduplicated    
└─ Custom imports   └─ *Filtered        


## Key Features

### 📊 Dataset Management
- **Multi-source ingestion**: Track and manage datasets from web, APIs, Hugging Face, and custom sources
- **Dataset Cards**: Rich metadata tracking for each dataset (source, version, preprocessing steps, etc.)
- **Intelligent Metadata Management**: Search, filter, and organize datasets by various attributes
- **ETL Pipeline Support**: Structured workflows for RAW → PROCESSED → MAPPED transformations

### 🔍 Data Analysis & Virtualization
- **DuckDB Integration**: Query datasets directly with SQL-like syntax
- **Advanced Statistics**: 
  - Low-level stats (token counts, vocabulary size, sequence lengths)
  - Chat template analysis (conversation structure, turns per dialogue, role distribution)
- **Cross-dataset Joins**: Combine statistics and metadata for comprehensive analysis

### 📐 Schema, System Prompt & Template Management
- **Chat Template Definitions**: Define and store conversation schemas in database
- **Custom Mappers**: Python-based mapping functions with UDF support
- **Format Standardization**: Transform diverse dataset formats into unified structures
- **System Prompt Management**: Centralized control over instruction templates

### 🔗 Data Lineage
- **Graph-based Provenance**: Track dataset/distribution transformations and relationships, also for recipes and system prompt derivations
- **Recipe System**: Create reproducible training datasets by combining multiple mapped sources
- **Version Control**: Monitor dataset evolution through the pipeline

### 🍳 Recipe Creation

**🎨 Data Studio** section in which you have the possibility to build your own dataset for a ML model training scope.

## Quick Start

### Local Installation
```bash
# Install dependencies
pip install -r requirements.txt

# Install Playwright browsers
playwright install

# Launch the Streamlit dashboard
streamlit run dashboard.py
```

### Docker Deployment
Development Environment
```bash
# Build and start development containers
make dev-build
make dev-up
```
### Production Environment
You have to modify docker/prod/ files in order to allign with your requirements, then run:
```bash
# Build and start production containers
make prod-build
make prod-up
```
### Storage Configuration
The platform is designed to be storage-agnostic. Configure your storage backend via environment variables:

```bash
# Base paths (container internal)
BASE_PATH=/app/nfs
# LAYER PATHS = BASE_PATH + LAYER_NAME
RAW_DATA_DIR={/app/nfs/}data-download
PROCESSED_DATA_DIR={/app/nfs/}processed-data
MAPPED_DATA_DIR={/app/nfs/}mapped-data
STATS_DATA_DIR={/app/nfs/}stats-data

# Bind mounts (host/external paths)
BINDED_BASE_PATH=example/users/name/repo/local_folder_binded_to_base_path
RAW_DATA_DIR=...
PROCESSED_DATA_DIR=...
MAPPED_DATA_DIR=...
STATS_DATA_DIR=...
```

### Supported storage backends:

Local filesystem: Direct mounting for development

NFS: Network-mounted storage for cluster deployments

Distributed FS: Ceph, GlusterFS, BeeGFS for scale-out scenarios

Cloud storage: Via FUSE mounts (s3fs, goofys)


