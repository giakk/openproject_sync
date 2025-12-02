#!/bin/bash
#
# Backup a Postgresql database into a daily file.
#

BACKUP_DIR=../backups
LOG_FILE=../backup.log
DAYS_TO_KEEP=5
FILE_SUFFIX=_cached_database_backup.sql
DATABASE=sync_cache_db
USER=postgres

# Set working directory to BACKUP_DIR
cd $BACKUP_DIR || exit

FILE=$(date +"%Y%m%d%H%M")${FILE_SUFFIX}
OUTPUT_FILE=${BACKUP_DIR}/${FILE}

{
  echo "Starting backup: $(date)"

  # do the database backup (dump)
  pg_dump -U ${USER} ${DATABASE} -F p -f ${OUTPUT_FILE}
  if [ $? -eq 0 ]; then
    echo "Database backup successful"
  else
    echo "Database backup failed"
  fi

  # gzip the postgres database dump file
  gzip $OUTPUT_FILE
  if [ $? -eq 0 ]; then
    echo "Gzip successful"
  else
    echo "Gzip failed"
  fi

  # show the user the result
  echo "${OUTPUT_FILE}.gz was created:"
  ls -l ${OUTPUT_FILE}.gz

  # prune old backups
  find $BACKUP_DIR -maxdepth 1 -mtime +$DAYS_TO_KEEP -name "*${FILE_SUFFIX}.gz" -exec rm -rf '{}' ';'
  if [ $? -eq 0 ]; then
    echo "Old backups pruned successfully"
  else
    echo "Failed to prune old backups"
  fi

  echo "Backup completed: $(date)"
} >> ${LOG_FILE} 2>&1
