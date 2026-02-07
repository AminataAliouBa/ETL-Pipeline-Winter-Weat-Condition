# -*- coding: utf-8 -*-
import logging


def log(log_name, error_filename, info_filename):
    """
    Configure and return a logger with multiple handlers.
    
    Args:
        log_name: Name identifier for the logger
        error_filename: Path to error log file
        info_filename: Path to info/metrics log file
        
    Returns:
        Configured logger instance with console, error, and metrics handlers
    """
    logger = logging.getLogger(log_name)
    logger.setLevel(logging.DEBUG) 

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # Handler métriques (INFO)
    metrics_handler = logging.FileHandler(info_filename, encoding='utf-8')
    metrics_handler.setLevel(logging.INFO)
    metrics_handler.setFormatter(formatter)

    # Handler erreurs (ERROR)
    error_handler = logging.FileHandler(error_filename, encoding='utf-8')
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)

    # Handler console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # Ajout des handlers
    logger.addHandler(metrics_handler)
    logger.addHandler(error_handler)
    logger.addHandler(console_handler)
    
    return logger
