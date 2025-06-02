# src/core/scraper_ms.py

import os
import tempfile
import uuid
import time
from typing import Dict

from dotenv import load_dotenv
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException, ElementClickInterceptedException
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.keys import Keys
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from src.database.schemas import ResultSearchRo
from src.log.logger import LoggerWebDriverManager, setup_logger
from src.models.models import ServidorSchema
from src.utils.helpers import WaitHelper

load_dotenv()

USERNAME_MS = os.getenv("USERNAME_MS")
PASSWORD_MS = os.getenv("PASSWORD_MS")
PAGE_MS = os.getenv("URL_FIRS_LOGIN")

logger = setup_logger(name= "MS_AUTOMATION")
driver_logger = LoggerWebDriverManager(logger=logger)


class WebDriverManager:
    
    def __init__(self):
        user_data_dir = tempfile.mkdtemp(prefix=f"selenium_{uuid.uuid4()}_")
        options = Options()
        options.add_argument("--start-maximized")
        options.add_argument("--disable-infobars")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-gpu")
        options.add_argument(f"--user-data-dir={user_data_dir}")
        options.add_argument("--ignore-certificate-errors")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        # options.add_argument("--headless")  # Descomentar em produção

        self.driver = Chrome(options=options)
        # service=ChromeService("/usr/local/bin/chromedriver")
        self.driver.set_page_load_timeout(30)
        self.driver.implicitly_wait(15)
        driver_logger.register_logger(driver=self.driver)


class PageObject(WebDriverManager):
    def __init__(self, username: str, password: str):
        super().__init__()
        self.username = username
        self.passowrd = password
    
    
    def login(self):
        try:
            driver_logger.logger.info("Login started MS")
            self.driver.get(f"{PAGE_MS}")
            username = WaitHelper.wait_for_element(
                self.driver, By.NAME, locator="username", timeout=10
            )
            username.send_keys(self.username)
            button_next = WaitHelper.wait_for_element(
                self.driver, 
                By.XPATH, 
                locator="//button[contains(@class, 'btn-primary') and contains(., 'Próxima')]", 
                timeout=10
            )
            button_next.click()
            password = WaitHelper.wait_for_element(
                self.driver, By.NAME, locator="senha", timeout=10
            )
            password.send_keys(self.passowrd)
            time.sleep(10)

        except Exception as e:
            driver_logger.logger.error(f"Error login: {str(e)}")
            raise
        

if __name__ == "__main__":
    
    PageObject(username=USERNAME_MS, password=PASSWORD_MS).login()