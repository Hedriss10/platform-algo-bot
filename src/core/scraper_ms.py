# src/core/scraper_ms.py

import os
import tempfile
import uuid
import time
import numpy as np
import cv2
import requests
import pytesseract
import io
from io import BytesIO
from PIL import Image, ImageEnhance, ImageFilter, ImageOps
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

    def _convert_captcha(self):
        try:
            driver_logger.logger.info("Processing captcha (white background + precise OCR)...")
            
            # 1. Capture element screenshot
            captcha_element = WaitHelper.wait_for_element(
                self.driver,
                By.XPATH,
                locator="//img[@name='captcha_img']",
                timeout=10,
            )
            captcha_screenshot = captcha_element.screenshot_as_png
            
            # 2. Convert to OpenCV format
            img_array = np.frombuffer(captcha_screenshot, np.uint8)
            img = cv2.imdecode(img_array, cv2.IMREAD_COLOR)
            
            # 3. Advanced cleaning with OpenCV
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
            
            # Remove noise while preserving edges
            denoised = cv2.fastNlMeansDenoising(gray, None, h=10, templateWindowSize=7, searchWindowSize=21)
            
            # Adaptive thresholding to handle lighting variations
            thresh = cv2.adaptiveThreshold(denoised, 255, 
                                        cv2.ADAPTIVE_THRESH_GAUSSIAN_C, 
                                        cv2.THRESH_BINARY_INV, 11, 2)
            
            # Morphological operations to remove lines/dots
            kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (2,2))
            cleaned = cv2.morphologyEx(thresh, cv2.MORPH_CLOSE, kernel)
            
            # Invert back to white background with black text
            final_img = cv2.bitwise_not(cleaned)
            
            # 4. Save processed image for debugging
            cv2.imwrite("captcha_processed_opencv.png", final_img)
            
            # 5. OCR with optimized configuration
            texto_captcha = pytesseract.image_to_string(
                final_img,
                config='--psm 8 --oem 3 -c tessedit_char_whitelist=ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
            ).strip()
            
            texto_captcha = ''.join(e for e in texto_captcha if e.isalnum()).upper()
            print("Extracted captcha text:", texto_captcha)
            
            # 6. Fill the captcha field
            campo_captcha = WaitHelper.wait_for_element(
                self.driver,
                By.NAME,
                locator="captcha",
                timeout=10
            )
            campo_captcha.clear()
            campo_captcha.send_keys(texto_captcha)
            
            driver_logger.logger.info("Captcha solved successfully!")
            
        except Exception as e:
            driver_logger.logger.error(f"Failed to process captcha: {str(e)}")
            raise
                
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
            time.sleep(5)
            self._convert_captcha()
            time.sleep(10)

        except Exception as e:
            driver_logger.logger.error(f"Error login: {str(e)}")
            raise
        

if __name__ == "__main__":
    
    PageObject(username=USERNAME_MS, password=PASSWORD_MS).login()