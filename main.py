#!/usr/bin/env python3

import logging
import sys
from datetime import datetime
from typing import Dict, List, Any
from app.config.ConfigManager import ConfigManager
from app.servicies.sync import SyncService

from app.utils.logger import setup_logging

logger = logging.getLogger(__name__)

def main():

    global_settings = ConfigManager()

    log_file = setup_logging(
        level=global_settings.logger.level,
        log_directory=global_settings.logger.log_directory,
        max_log_files=global_settings.logger.max_log_files
    )

    try:

        logger.info("========= AVVIATA SINCRONIZZAZIONE =========")
        logger.info(f"Log file: {log_file}")

        syncer = SyncService(global_settings)
        syncer.run_full_sync()

        logger.info("========= TERMINATA SINCRONIZZAZIONE =========")


    except KeyboardInterrupt:
        logger.info("Syncronization process interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    
    main()