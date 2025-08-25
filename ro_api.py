import os
import tempfile
import uuid
import time
import traceback
import requests
import json

from typing import List
from dotenv import load_dotenv
from selenium.common.exceptions import (
    TimeoutException,
    WebDriverException,
    NoSuchElementException,
    ElementClickInterceptedException,
)

from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.keys import Keys
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities


from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from src.database.schemas import ResultSearchRo
from src.log.logger import LoggerWebDriverManager, setup_logger
from src.models.ro import ServidorSchema
from src.utils.helpers import WaitHelper
from src.database.schemas import ResultSearchRo, SearchRo, SessionLocal
from sqlalchemy import select, update


load_dotenv()

URL_RO = os.getenv("URL_RO")


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
        # Configuração para capturar logs de rede
        options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
        # options.add_argument("--headless")  # Descomentar em produção

        self.driver = Chrome(options=options)
        self.driver.set_page_load_timeout(10)
        self.driver.implicitly_wait(15)
        driver_logger.register_logger(driver=self.driver)


class PageObject(WebDriverManager):
    def __init__(self, username: str, password: str):
        super().__init__()
        self.username = username
        self.password = password

    def _slow_time(self, seconds: int):
        time.sleep(seconds)

    def login_gov(self):
        try:
            driver_logger.logger.info("Login started")
            self.driver.get(URL_RO)

            # Preenche usuário e senha
            user = WaitHelper.wait_for_element(
                self.driver, By.NAME, "usuario", timeout=6, visible=True
            )
            user.send_keys(self.username)
            self._slow_time(3)

            password = WaitHelper.wait_for_element(
                self.driver, By.NAME, "senha", timeout=6, visible=True
            )
            password.send_keys(self.password)
            password.send_keys(Keys.ENTER)
            driver_logger.logger.info("Login successful")
            self._slow_time(
                5
            )  # Aumentei o tempo para garantir que a requisição do token seja feita

            # Capturar logs de rede (Network)
            logs = self.driver.get_log("performance")

            token_data = None
            for log in logs:
                try:
                    message = json.loads(log["message"])["message"]
                    if message.get("method") == "Network.responseReceived":
                        url = (
                            message.get("params", {})
                            .get("response", {})
                            .get("url", "")
                        )
                        if "oauth/token" in url:
                            request_id = message["params"]["requestId"]
                            # Pegar o corpo da resposta
                            response_body = self.driver.execute_cdp_cmd(
                                "Network.getResponseBody",
                                {"requestId": request_id},
                            )
                            token_data = json.loads(response_body["body"])
                            break
                except:
                    continue

            if token_data:
                print("\n✅ Token capturado com sucesso:")
                print(json.dumps(token_data, indent=4, ensure_ascii=False))

                with open("token_response.json", "w", encoding="utf-8") as f:
                    json.dump(token_data, f, indent=4, ensure_ascii=False)

                return token_data
            else:
                print(
                    "\n❌ Não foi possível capturar o token. Verifique se a requisição foi feita."
                )
                return None

        except Exception as e:
            driver_logger.logger.error(
                f"Erro no login: {e.__class__.__name__}: {str(e)}"
            )
            driver_logger.logger.debug(traceback.format_exc())
            raise
        finally:
            if hasattr(self, "driver"):
                self.driver.quit()
                print("Navegador fechado.")


class ScrapePoolExecute:
    def __init__(self, username: str, password: str, *args, **kwargs):
        self.page_objects = PageObject(username=username, password=password)
        driver_logger.register_logger(driver=self.page_objects.driver)

    def run(self):
        try:
            self.page_objects.login_gov()
        except Exception as e:
            driver_logger.logger.error(
                f"Error running scraping pool: {str(e)}"
            )
            raise

# Carregar variáveis do .env
load_dotenv()

class ExtractTransformLoad:
    def __init__(self):
        self.base_url = os.getenv("ROUTE_RO")  # Ex.: "https://consignacao.sistemas.ro.gov.br/servidor/buscarPorMatriculaCpfSequencia?numCpf={cpf}"
        self.token = None

    def load_token(self) -> str:
        token_path = "token_response.json"

        if not os.path.exists(token_path):
            raise FileNotFoundError(f"Token file not found: {token_path}")

        with open(token_path, "r", encoding="utf-8") as f:
            token_data = json.load(f)

        if "access_token" not in token_data:
            raise KeyError("Token file does not contain 'access_token'")

        self.token = token_data["access_token"]
        return self.token

    def cpfs_database(self):
        try:
            db = SessionLocal()
            stmt = select(SearchRo.cpf)
            cpfs = db.scalars(stmt).all()
            db.close()
            return cpfs
        except Exception as e:
            driver_logger.logger.error(f"Error cpfs_database: {str(e)}")
            raise
    
    def update_has_filter_cpf(self, cpf: str):
        try:
            db = SessionLocal()
            cpf_str = cpf.replace(".", "").replace("-", "")
            stmt = (
                update(SearchRo)
                .where(SearchRo.cpf == cpf_str)
                .values(has_filter=True)
            )
            db.execute(stmt)
            db.commit()
            driver_logger.logger.info(f"CPF {cpf} has filter updated")
        except Exception as e:
            driver_logger.logger.error(f"Error update_has_filter_cpf with cpf {cpf}: {str(e)}")
            raise
        finally:
            db.close()

    def get_request(self, cpf: str):
        """Faz requisição para a API usando CPF"""
        if not self.token:
            raise ValueError("Token not loaded. Call 'load_token()' first.")

        url = self.base_url.format(cpf=cpf)  # Substitui {cpf} na rota
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
                "Authorization": f"Bearer {self.token}",
            }
            response = requests.get(url, headers=headers, timeout=10)

            if response.status_code == 200:
                data = response.json()
                print(f"\n✅ Dados do CPF {cpf} capturados com sucesso:")
                print(json.dumps(data, indent=4, ensure_ascii=False))
                self.update_has_filter_cpf(cpf)
                return data

            elif response.status_code == 401:
                username = os.getenv("USERNAME_RO")
                password = os.getenv("PASSWORD_RO")
                scrape_pool = ScrapePoolExecute(username=username, password=password)
                scrape_pool.run()

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for CPF {cpf}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error for CPF {cpf}: {e}")
            raise


if __name__ == "__main__":
    a = ExtractTransformLoad()
    a.load_token()

    cpfs = a.cpfs_database()
    for cpf in cpfs:
        time.sleep(5)
        a.get_request(cpf)
