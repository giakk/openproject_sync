#!/usr/bin/env python3

import logging
import sys
from datetime import datetime
from typing import Dict, List, Any
from .config import ConfigManager
from .servicies.sync import SyncService

from .utils.logger import setup_logging

logger = logging.getLogger(__name__)

def main():

    global_settings = ConfigManager()

    setup_logging(
        level=global_settings.logger.level,
        filename=global_settings.logger.filename
    )

    try:

        logger.info("=== AVVIO SINCRONIZZAZIONE PROGETTI ===")

        # syncer = SyncService(global_settings)
        # syncer.run_full_sync()

    except KeyboardInterrupt:
        logger.info("Cyncronization process interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    
    main()