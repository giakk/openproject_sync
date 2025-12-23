# src/utils/logger.py
import logging
import logging.handlers
import os
import sys
import glob
from typing import Optional
from datetime import datetime
from pathlib import Path

def cleanup_old_logs(log_directory: str, max_files: int = 10) -> None:
    """
    Pulisce i vecchi file di log mantenendo solo gli ultimi max_files giorni.

    Args:
        log_directory: Directory contenente i file di log
        max_files: Numero massimo di file giornalieri da mantenere
    """
    try:
        # Pattern per trovare tutti i file di log giornalieri
        log_pattern = os.path.join(log_directory, "sync_*.log")
        log_files = glob.glob(log_pattern)

        if len(log_files) <= max_files:
            return  # Non c'è nulla da pulire

        # Ordina per data di modifica (più vecchi prima)
        log_files.sort(key=os.path.getmtime)

        # Calcola quanti file eliminare
        files_to_delete = len(log_files) - max_files

        # Elimina i file più vecchi
        deleted_count = 0
        for log_file in log_files[:files_to_delete]:
            try:
                os.remove(log_file)
                deleted_count += 1
            except Exception as e:
                print(f"Error removing log file {log_file}: {e}")

        if deleted_count > 0:
            print(f"Removed {deleted_count} old log file(s), keeping last {max_files} days")

    except Exception as e:
        print(f"Error during log cleanup: {e}")


def setup_logging(
    level: str = "INFO",
    format_string: Optional[str] = None,
    log_directory: Optional[str] = None,
    console: bool = False,
    max_log_files: int = 10
) -> str:
    """
    Configura il sistema di logging con file giornaliero e pulizia automatica.

    Args:
        level: Livello di logging (DEBUG, INFO, WARNING, ERROR)
        format_string: Formato personalizzato per i log
        log_directory: Directory dove salvare i file di log
        console: Se loggare anche su console
        max_log_files: Numero massimo di giorni di log da mantenere

    Returns:
        str: Path del file di log per la giornata corrente
    """

    # Formato di default
    if not format_string:
        # format_string = "%(asctime)s - %(levelname)s - func:%(funcName)s - %(message)s"
        format_string = "%(asctime)s - %(levelname)s - %(message)s"

    # Crea directory se non esiste
    if log_directory and not os.path.exists(log_directory):
        os.makedirs(log_directory, exist_ok=True)

    # Pulisci vecchi file di log prima di aprire quello di oggi
    if log_directory:
        cleanup_old_logs(log_directory, max_log_files)

    # Crea nome file con timestamp giornaliero (solo data, non ora)
    if log_directory:
        # Genera timestamp giornaliero (YYYYMMDD)
        date_str = datetime.now().strftime("%Y%m%d")

        # Nuovo nome file giornaliero: sync_20231215.log
        timestamped_filename = os.path.join(log_directory, f"sync_{date_str}.log")
    else:
        timestamped_filename = None

    # Configurazione base
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))

    # Rimuovi handler esistenti
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    formatter = logging.Formatter(format_string)

    # Handler per console
    if console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, level.upper()))
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)

    # Handler per file con timestamp giornaliero (append mode)
    if timestamped_filename:
        file_handler = logging.FileHandler(
            filename=timestamped_filename,
            mode='a',  # Append mode per scrivere più esecuzioni nello stesso file
            encoding='utf-8'
        )
        file_handler.setLevel(getattr(logging, level.upper()))
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    # Configura logger per librerie esterne
    # _configure_external_loggers(level)

    # Log iniziale con separatore per distinguere le esecuzioni
    logger = logging.getLogger(__name__)
    execution_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info("=" * 80)
    logger.info(f"Nuova esecuzione iniziata alle {execution_time}")
    logger.info(f"Logging configurato - Livello: {level}, File: {timestamped_filename}")
    logger.info("=" * 80)

    return timestamped_filename

def _configure_external_loggers(level: str) -> None:
    """Configura livelli di logging per librerie esterne"""
    
    # Riduci verbosità di librerie esterne
    external_loggers = [
        'requests',
        'urllib3',
        'psycopg2',
        'asyncio'
    ]
    
    for logger_name in external_loggers:
        external_logger = logging.getLogger(logger_name)
        # Imposta livello più alto per ridurre rumore
        if level == "DEBUG":
            external_logger.setLevel(logging.INFO)
        else:
            external_logger.setLevel(logging.WARNING)

class SyncLogger:
    """Logger specializzato per operazioni di sincronizzazione"""
    
    def __init__(self, name: str):
        self.logger = logging.getLogger(name)
        self.sync_context = {}
    
    def set_sync_context(self, sync_id: str, sync_type: str, entity_count: int = 0):
        """Imposta contesto per sessione di sincronizzazione"""
        self.sync_context = {
            'sync_id': sync_id,
            'sync_type': sync_type,
            'entity_count': entity_count,
            'start_time': datetime.now()
        }
    
    def clear_sync_context(self):
        """Pulisce contesto sincronizzazione"""
        self.sync_context = {}
    
    def _format_message(self, message: str) -> str:
        """Formatta messaggio con contesto sincronizzazione"""
        if not self.sync_context:
            return message
        
        context_str = f"[{self.sync_context['sync_type']}:{self.sync_context['sync_id'][:8]}]"
        return f"{context_str} {message}"
    
    def debug(self, message: str, **kwargs):
        """Log debug con contesto"""
        self.logger.debug(self._format_message(message), extra=kwargs)
    
    def info(self, message: str, **kwargs):
        """Log info con contesto"""
        self.logger.info(self._format_message(message), extra=kwargs)
    
    def warning(self, message: str, **kwargs):
        """Log warning con contesto"""
        self.logger.warning(self._format_message(message), extra=kwargs)
    
    def error(self, message: str, **kwargs):
        """Log error con contesto"""
        self.logger.error(self._format_message(message), extra=kwargs)
    
    def critical(self, message: str, **kwargs):
        """Log critical con contesto"""
        self.logger.critical(self._format_message(message), extra=kwargs)
    
    def sync_start(self, entity_count: int):
        """Log inizio sincronizzazione"""
        self.info(f"Avvio sincronizzazione di {entity_count} entità")
    
    def sync_progress(self, processed: int, successful: int, failed: int):
        """Log progresso sincronizzazione"""
        total = self.sync_context.get('entity_count', 0)
        percentage = (processed / total * 100) if total > 0 else 0
        
        self.info(f"Progresso: {processed}/{total} ({percentage:.1f}%) - "
                 f"Successi: {successful}, Errori: {failed}")
    
    def sync_complete(self, total_processed: int, successful: int, failed: int, duration: float):
        """Log completamento sincronizzazione"""
        success_rate = (successful / total_processed * 100) if total_processed > 0 else 0
        
        self.info(f"Sincronizzazione completata in {duration:.2f}s - "
                 f"Processati: {total_processed}, Successi: {successful} ({success_rate:.1f}%), "
                 f"Errori: {failed}")
    
    def operation_success(self, operation_type: str, entity_id: str, execution_time: float = None):
        """Log successo operazione"""
        time_str = f" in {execution_time:.3f}s" if execution_time else ""
        self.debug(f"Successo {operation_type} entità {entity_id}{time_str}")
    
    def operation_error(self, operation_type: str, entity_id: str, error: str, retry_count: int = 0):
        """Log errore operazione"""
        retry_str = f" (tentativo {retry_count})" if retry_count > 0 else ""
        self.error(f"Errore {operation_type} entità {entity_id}{retry_str}: {error}")
    
    def validation_error(self, entity_id: str, errors: list):
        """Log errori di validazione"""
        error_str = ", ".join(errors)
        self.warning(f"Errori validazione entità {entity_id}: {error_str}")
    
    def auto_correction(self, entity_id: str, corrections: list):
        """Log correzioni automatiche"""
        correction_str = ", ".join(corrections)
        self.info(f"Auto-correzioni applicate entità {entity_id}: {correction_str}")

def get_sync_logger(name: str) -> SyncLogger:
    """
    Factory per creare logger di sincronizzazione
    
    Args:
        name: Nome del logger
        
    Returns:
        SyncLogger: Logger configurato
    """
    return SyncLogger(name)