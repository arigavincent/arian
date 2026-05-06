#!/usr/bin/env python3
"""
ArianFun MovieBox Sync Orchestrator
"""

import os
import sys
import logging

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'services'))

from discovery import discover
from enrichment import enrich_worker
from collection_builder import collections_worker
from shared import get_db, log_sync, complete_sync

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("🚀 Starting ArianFun MovieBox sync")
    
    run_id = log_sync("moviebox_sync", mode="full", status="running")
    
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        logger.info("✅ Database connection successful")
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        sys.exit(1)
    
    logger.info("📡 Step 1: Discovering new titles...")
    try:
        discover(mode="full")
        logger.info(f"✅ Discovery completed")
    except Exception as e:
        logger.error(f"❌ Discovery failed: {e}")
        complete_sync(run_id, notes=f"Discovery failed: {e}")
        sys.exit(1)
    
    logger.info("🔍 Step 2: Enriching titles...")
    try:
        enrich_worker()
        logger.info(f"✅ Enrichment completed")
    except Exception as e:
        logger.error(f"❌ Enrichment failed: {e}")
        complete_sync(run_id, notes=f"Enrichment failed: {e}")
        sys.exit(1)
    
    logger.info("📚 Step 3: Rebuilding collections...")
    try:
        collections_worker()
        logger.info(f"✅ Collections rebuilt")
    except Exception as e:
        logger.error(f"❌ Collection rebuild failed: {e}")
        complete_sync(run_id, notes=f"Collection rebuild failed: {e}")
        sys.exit(1)
    
    complete_sync(run_id, status="complete", notes="Full sync finished successfully")
    logger.info("🎉 Full sync completed successfully!")

if __name__ == "__main__":
    main()
