import logging
import os
from logging.handlers import RotatingFileHandler
from datetime import datetime
import sys

class ColoredFormatter(logging.Formatter):
    """Adiciona cores aos logs no console"""
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
    """Configura um logger robusto para automação"""
    logger = logging.getLogger("RoAutomation")
    logger.setLevel(logging.DEBUG)  # Define o nível mais alto no logger
    
    # Evita logs duplicados se o logger já foi configurado
    if logger.handlers:
        return logger

    # Cria diretório de logs se não existir
    os.makedirs("logs", exist_ok=True)
    
    # Formato comum para os logs
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Handler para console (com cores)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)  # Nível diferente para console
    console_handler.setFormatter(ColoredFormatter())
    
    # Handler para arquivo com rotação
    log_file = f"logs/automation_{datetime.now().strftime('%Y%m%d')}.log"
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=5*1024*1024,  # 5MB
        backupCount=3,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    
    # Handler para erros (arquivo separado)
    error_handler = RotatingFileHandler(
        f"logs/errors_{datetime.now().strftime('%Y%m%d')}.log",
        maxBytes=2*1024*1024,  # 2MB
        backupCount=1,
        encoding='utf-8'
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    
    # Adiciona todos os handlers
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    logger.addHandler(error_handler)
    
    return logger

class LoggerWebDriverManager:
    """Gerenciador de logger para WebDriver"""
    def __init__(self, logger=None):
        self.logger = logger or setup_logger()
    
    def register_logger(self, driver):
        """Registra o logger no driver"""
        driver.logger = self.logger
        
    def log_step(self, message, level="info"):
        """Registra um passo da automação com nível configurável"""
        log_method = getattr(self.logger, level.lower(), self.logger.info)
        log_method(message)
        
    def log_error_with_screenshot(self, driver, message):
        """Registra erro e tenta capturar screenshot"""
        self.logger.error(message)
        try:
            if hasattr(driver, 'get_screenshot_as_file'):
                screenshot_name = f"error_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
                screenshot_path = os.path.join("logs", screenshot_name)
                driver.get_screenshot_as_file(screenshot_path)
                self.logger.error(f"Screenshot salvo em: {screenshot_path}")
        except Exception as e:
            self.logger.error(f"Falha ao capturar screenshot: {str(e)}")

# # Exemplo de uso
# if __name__ == "__main__":
#     logger = setup_logger()
#     logger.debug("Mensagem de debug")
#     logger.info("Mensagem informativa")
#     logger.warning("Aviso importante")
#     logger.error("Erro ocorreu")
#     logger.critical("Erro crítico!")
    
#     # Exemplo com WebDriver
#     driver_manager = LoggerWebDriverManager(logger)
#     # Simulando um driver (em um caso real, seria um Selenium WebDriver)
#     class MockDriver:
#         pass
#     driver = MockDriver()
#     driver_manager.register_logger(driver)
#     driver.logger.info("Logger registrado no driver com sucesso!")