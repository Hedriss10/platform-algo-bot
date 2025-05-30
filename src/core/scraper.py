# src/core/scraper.py

import os
import tempfile
import uuid
import time
from typing import Dict

from dotenv import load_dotenv
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
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


URL_RO = os.getenv("URL_RO")
URL_CONSULT = os.getenv("URL_CONSULT")

logger = setup_logger()
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
        options.add_argument("--headless")  # Descomentar em produção

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
    
    def extract_server_data(self, card) -> Dict:
        try:
            card = WaitHelper.wait_for_element(
                self.driver,
                By.CSS_SELECTOR,
                "div.q-card__section.q-card__section--vert",
                timeout=5,
            )

            data = {
                "nome": self._clean_text(
                    self._get_element_text(
                        card, "div.col-xs-12.text-bold span.text-weight-bold"
                    )
                ),
                "matricula": self._clean_text(
                    self._get_text_after_label(card, "Matrícula:")
                ),
                "cpf": self._clean_text(
                    self._get_text_after_label(card, "CPF:")
                ),
                "cargo": self._clean_text(
                    self._get_text_after_label(card, "Cargo:")
                ),
                "lotacao": self._clean_text(
                    self._get_text_after_label(card, "Lotação:")
                ),
                "classificacao": self._clean_text(
                    self._get_text_after_label(card, "Classificação:")
                ),
                "margem_disponivel": self._clean_text(
                    self._get_badge_value(card, "Margem Disponível:")
                ),
                "margem_cartao": self._clean_text(
                    self._get_badge_value(card, "Margem Cartão:")
                ),
                "margem_cartao_beneficio": self._clean_text(
                    self._get_badge_value(card, "Margem Cartão Benefício:")
                ),
            }
            driver_logger.logger.info("Data extract success")
            return data

        except Exception as e:
            driver_logger.logger.error(
                f"Error is processing extract_server_data: {str(e)}"
            )
            raise

    def _get_element_text(self, parent, css_selector: str) -> str:
        try:
            element = parent.find_element(By.CSS_SELECTOR, css_selector)
            return element.text
        except:
            return ""

    def _get_text_after_label(self, parent, label_text: str) -> str:
        try:
            xpath = f".//span[contains(@class, 'text-bold') and contains(text(), '{label_text}')]/following-sibling::text()"
            text_node = parent.find_element(By.XPATH, xpath)
            return text_node
        except:
            return ""

    def _get_badge_value(self, parent, label_text: str) -> str:
        try:
            xpath = f".//span[contains(@class, 'text-bold') and contains(text(), '{label_text}')]/following-sibling::div[contains(@class, 'q-badge')]"
            badge = parent.find_element(By.XPATH, xpath)
            return badge.text
        except:
            return ""

    def _clean_text(self, text: str) -> str:
        if not text:
            return ""
        return " ".join(text.strip().split())
        
        
    def _slow_time(self, seconds: int):
        time.sleep(seconds)

    def login_gov(self):
        try:
            driver_logger.logger.info("Login started")
            self.driver.get(URL_RO)
            user = WaitHelper.wait_for_element(
                self.driver, By.NAME, locator="usuario", timeout=10
            )
            user.send_keys(self.username)
            self._slow_time(5)
            password = WaitHelper.wait_for_element(
                self.driver, by=By.NAME, locator="senha", timeout=10
            )
            password.send_keys(self.passowrd)
            password.send_keys(Keys.ENTER)
            driver_logger.logger.info("Login sucefully")
            self._slow_time(5)
        except Exception as e:
            driver_logger.logger.error(f"Erro no login: {str(e)}")
            raise

    def click_search_employe(self):
        try:
            button = WaitHelper.wait_for_element(
                self.driver,
                By.XPATH,
                '//button[.//span[contains(., "Buscar Servidor")]]',
                clickable=True,
                timeout=15,  # Increased timeout
            )
            WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, '//button[.//span[contains(., "Buscar Servidor")]]')),
                "Search button not clickable"
            )
            self._slow_time(10)
            button.click()
            driver_logger.logger.info("Click button search employe successful")
        except TimeoutException as te:
            driver_logger.logger.error(f"Timeout error clicking search button: {str(te)}")
            raise
        except WebDriverException as wde:
            driver_logger.logger.error(f"WebDriver error clicking search button: {str(wde)}")
            raise
        except Exception as e:
            driver_logger.logger.error(f"Unexpected error clicking search button: {str(e)}")
            raise

    def search_table(self, db_session: Session):
        try:
            driver_logger.logger.info("Start collect data in table")

            card = WaitHelper.wait_for_element(
                self.driver,
                By.CSS_SELECTOR,
                "div.q-card__section.q-card__section--vert",
                timeout=10,
            )
            self._slow_time(10)
            raw_data = self.extract_server_data(card)
            validated_data = ServidorSchema(**raw_data).model_dump()
            self._slow_time(10)
            db_record = ResultSearchRo(
                nome=validated_data["nome"],
                matricula=validated_data["matricula"],
                cpf=validated_data["cpf"],
                cargo=validated_data["cargo"],
                lotacao=validated_data["lotacao"],
                classificacao=validated_data["classificacao"],
                margem_disponivel=validated_data["margem_disponivel"],
                margem_cartao=validated_data["margem_cartao"],
                margem_cartao_beneficio=validated_data[
                    "margem_cartao_beneficio"
                ],
            )
            try:
                db_session.add(db_record)
                db_session.commit()
                driver_logger.logger.info(
                    f"CPF insert success: ID {db_record.id}"
                )
                return db_record
            except SQLAlchemyError as e:
                db_session.rollback()
                driver_logger.logger.error(f"Error save in database: {str(e)}")
                raise

        except Exception as e:
            driver_logger.logger.error(f"Error search_table: {str(e)}")
            raise

    def fill_form_fields(self, cpf: str, matricula: str = "", employee_pensioner: str = "N") -> bool:
        try:
            driver_logger.logger.info(f"Iniciando preenchimento para CPF: {cpf}")
            self.driver.get(URL_CONSULT)
            self._slow_time(10)
            WebDriverWait(self.driver, 20).until(
                lambda d: d.execute_script("return document.readyState") == "complete",
                f"Page {URL_CONSULT} did not load completely for CPF {cpf}"
            )

            # Wait for the CPF input field to be visible and clickable
            cpf_field = WaitHelper.wait_for_element(
                self.driver,
                By.CSS_SELECTOR,
                'input[name="cpf"]',
                visible=True,
                clickable=True,
                timeout=20,  # Increased timeout
            )
            self._slow_time(10)
            # Ensure the field is interactable before clearing and sending keys
            WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, 'input[name="cpf"]')),
                f"CPF field not clickable for CPF {cpf}"
            )
            self._slow_time(10)
            self.driver.execute_script(
                "arguments[0].scrollIntoView({block: 'center'});", cpf_field
            )
            self._slow_time(10)
            cpf_field.clear()
            cpf_field.send_keys(cpf)
            driver_logger.logger.info(f"CPF {cpf} inserido no formulário")
            self._slow_time(10)
            # Ensure the search button is clickable before clicking
            self.click_search_employe()
            self._slow_time(10)
            # Wait for either a notification or the modal/table to appear
            try:
                notification = WaitHelper.wait_for_element(
                    self.driver,
                    By.XPATH,
                    '//div[contains(@class, "q-notification__message") and contains(., "Nenhum servidor encontrado")]',
                    timeout=10,
                )
                driver_logger.logger.warning(f"CPF {cpf} not found")
                return False

            except TimeoutException:
                # Wait for the loading spinner to disappear, if present
                WaitHelper.wait_for_element_disappear(
                    self.driver,
                    By.CSS_SELECTOR,
                    "div.loading-spinner",
                    timeout=15,
                )
                self._slow_time(10)
                modal_process = self.modal_exists_table()
                if modal_process or not modal_process:
                    driver_logger.logger.info("Continue with form filling")
                    return True

                driver_logger.logger.info(f"CPF {cpf} insert forms")

                if matricula:
                    matricula_field = WaitHelper.wait_for_element(
                        self.driver,
                        By.CSS_SELECTOR,
                        'input[name="matricula"]',
                        visible=True,
                        timeout=10,
                    )
                    self._slow_time(10)
                    WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, 'input[name="matricula"]')),
                        f"Matricula field not clickable for CPF {cpf}"
                    )
                    matricula_field.clear()
                    self._slow_time(10)
                    matricula_field.send_keys(matricula)
                    driver_logger.logger.info(f"Matrícula {matricula} inserida")

                if employee_pensioner == "S":
                    pensionista_checkbox = WaitHelper.wait_for_element(
                        self.driver,
                        By.CSS_SELECTOR,
                        'input[name="pensionista"]',
                        clickable=True,
                        timeout=10,
                    )
                    self._slow_time(10)
                    WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, 'input[name="pensionista"]')),
                        f"Pensionista checkbox not clickable for CPF {cpf}"
                    )
                    self._slow_time(10)
                    pensionista_checkbox.click()
                    driver_logger.logger.info("Options selected `Pensionista`")

                return True

        except TimeoutException as te:
            driver_logger.logger.error(f"Timeout error processing CPF {cpf}: {str(te)}")
            raise
        except WebDriverException as wde:
            driver_logger.logger.error(f"WebDriver error processing CPF {cpf}: {str(wde)}")
            raise
        except Exception as e:
            driver_logger.logger.error(f"Unexpected error processing CPF {cpf}: {str(e)}")
            raise

    def modal_exists_table(self) -> bool:
        try:
            driver_logger.logger.info(
                "Verificando se modal de seleção aparece"
            )

            try:
                modal = WaitHelper.wait_for_element(
                    self.driver, By.CSS_SELECTOR, "div.q-dialog", timeout=10
                )
                if not modal:
                    driver_logger.logger.info(
                        "Modal não encontrada - CPF único"
                    )
                    return False
            except TimeoutException:
                driver_logger.logger.info("Modal não encontrada - CPF único")
                return False

            driver_logger.logger.info(
                "Modal de seleção encontrada - processando..."
            )

            rows = WaitHelper.wait_for_elements(
                self.driver,
                By.CSS_SELECTOR,
                "div.q-dialog table tbody tr:not([style*='display: none'])",
                timeout=10,
            )

            for row in rows:
                try:
                    margem_disponivel = row.find_element(
                        By.XPATH, ".//td[6]"
                    ).text.strip()
                    margem_cartao = row.find_element(
                        By.XPATH, ".//td[7]"
                    ).text.strip()

                    if (
                        margem_disponivel.replace(",", "")
                        .replace(".", "")
                        .isdigit()
                        and margem_cartao.replace(",", "")
                        .replace(".", "")
                        .isdigit()
                    ):
                        svg = row.find_element(
                            By.CSS_SELECTOR, "svg.q-radio__bg"
                        )
                        self.driver.execute_script(
                            """
                            arguments[0].dispatchEvent(new MouseEvent('click', {
                                view: window,
                                bubbles: true,
                                cancelable: true
                            }));
                        """,
                            svg,
                        )

                        driver_logger.logger.info(
                            f"Servidor selecionado - Margem: {margem_disponivel}, Cartão: {margem_cartao}"
                        )

                        confirm_button = WaitHelper.wait_for_element(
                            self.driver,
                            By.XPATH,
                            '//button[.//span[contains(@class, "block") and contains(text(), "Confirmar")]]',
                            clickable=True,
                            timeout=10,
                        )

                        # Método alternativo de clique que funciona melhor com elementos Vue/Quasar
                        self.driver.execute_script(
                            """
                            var event = new MouseEvent('click', {
                                'view': window,
                                'bubbles': true,
                                'cancelable': true
                            });
                            arguments[0].dispatchEvent(event);
                        """,
                            confirm_button,
                        )

                        driver_logger.logger.info(
                            "Button clicked successfully"
                        )

                        WaitHelper.wait_for_element_disappear(
                            self.driver,
                            By.CSS_SELECTOR,
                            "div.q-dialog",
                            timeout=10,
                        )

                        return True

                except Exception as e:
                    driver_logger.logger.warning(
                        f"Erro ao processar linha: {str(e)}"
                    )
                    continue

            driver_logger.logger.warning("Not server with margin valid found")
            return False

        except Exception as e:
            driver_logger.logger.error(f"Erro ao processar modal: {str(e)}")
            raise

    def clean_input_cpf(self):
        try:
            driver_logger.logger.info("Clean input CPF")
            input_cpf = WaitHelper.wait_for_element(
                by=By.CSS_SELECTOR, locator='input[name="cpf"]'
            )
            self.click_search_employe()
            input_cpf.clear()
            driver_logger.logger.info("Input CPF clean success")
        except Exception as e:
            driver_logger.logger.error(f"Error clean_input_cpf: {str(e)}")
            raise
