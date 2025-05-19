# src/core/scraper.py

import os 
import asyncio

from dotenv import load_dotenv
from selenium.webdriver import Chrome, ChromeOptions
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    WebDriverException,
)

from src.log.logger import setup_logger, LoggerWebDriverManager


load_dotenv()


URL_RO = os.getenv("URL_RO")
URL_CONSULT = os.getenv("URL_CONSULT")
USERNAME_RO = os.getenv("USERNAME_RO")
PASSWORD_RO = os.getenv("PASSWORD_RO")


logger = setup_logger()
driver_logger =  LoggerWebDriverManager(logger=logger)


class WebDriverManager:
    def __init__(self):
        options = Options()
        options.add_argument("--start-maximized")
        options.add_argument("--disable-infobars")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--ignore-certificate-errors")
        options.add_argument("--disable-dev-shm-usage")
        # options.add_argument("--headless")  # Descomentar para produção

        self.driver = Chrome(options=options)
        self.driver.set_page_load_timeout(30)
        self.driver.implicitly_wait(15)
        driver_logger.register_logger(driver=self.driver) # register logs
    
    
    
class PageObject(WebDriverManager):
    def __init__(self):
        super().__init__()
    
    
    # helpers
    def _wait_and_find(self, by: str, value: str, timeout=15):
        try:
            element = WebDriverManager(self.driver, timeout).until(
                EC.visibility_of_element_located((by, value))
            )
            
            WebDriverWait(self.driver, timeout).until(
                EC.element_to_be_clickable((by, value))
            )
            return element
        except TimeoutException:
            driver_logger.logger.error(f"Element not found: {by}={value}")
            raise
        
        except Exception as e:
            driver_logger.logger.error(f"Error finding element {by}={value}: {str(e)}")
            raise
    
    def login_gov(self):
        try:
            driver_logger.logger.info("Start login")
            user_field = self._wait_and_find(By.NAME, "usuario")
            user_field.send_keys(USERNAME_RO)
            
            password_field = self._wait_and_find(By.NAME, "senha")
            password_field.send_keys(PASSWORD_RO)
            password_field.send_keys(Keys.ENTER)
            
            driver_logger.logger.info("Login sucefully")

        except Exception as e:
            driver_logger.logger.error(f"Erro no login: {str(e)}")
            raise


if __name__ == "__main__":
    exe = PageObject()
    exe.login_gov()