import logging
import os
from logging.handlers import RotatingFileHandler
from datetime import datetime
import sys

class ColoredFormatter(logging.Formatter):
    """Add colors to logs in console"""
    grey = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format_str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    FORMATS = {
        logging.DEBUG: grey + format_str + reset,
        logging.INFO: grey + format_str + reset,
        logging.WARNING: yellow + format_str + reset,
        logging.ERROR: red + format_str + reset,
        logging.CRITICAL: bold_red + format_str + reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)

def setup_logger():
    """Settings logger"""
    logger = logging.getLogger("RoAutomation")
    logger.setLevel(logging.DEBUG)
    
    if logger.handlers:
        return logger

    os.makedirs("logs", exist_ok=True)
    
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(ColoredFormatter())
    
    log_file = f"logs/automation_{datetime.now().strftime('%Y%m%d')}.log"
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=5*1024*1024,  # 5MB
        backupCount=3,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    
    error_handler = RotatingFileHandler(
        f"logs/errors_{datetime.now().strftime('%Y%m%d')}.log",
        maxBytes=2*1024*1024,  # 2MB
        backupCount=1,
        encoding='utf-8'
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    logger.addHandler(error_handler)
    
    return logger

class LoggerWebDriverManager:
    """Manager for WebDriver logging"""
    def __init__(self, logger=None):
        self.logger = logger or setup_logger()
    
    def register_logger(self, driver):
        driver.logger = self.logger
        
    def log_step(self, message, level="info"):
        log_method = getattr(self.logger, level.lower(), self.logger.info)
        log_method(message)
        
    def log_error_with_screenshot(self, driver, message):
        self.logger.error(message)
        try:
            if hasattr(driver, 'get_screenshot_as_file'):
                screenshot_name = f"error_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
                screenshot_path = os.path.join("logs", screenshot_name)
                driver.get_screenshot_as_file(screenshot_path)
                self.logger.error(f"Screenshot salvo em: {screenshot_path}")
        except Exception as e:
            self.logger.error(f"Falha ao capturar screenshot: {str(e)}")