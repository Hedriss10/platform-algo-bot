from datetime import datetime
from selenium.webdriver import Chrome, ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    WebDriverException,
)
import asyncio
import logging
import os
from dotenv import load_dotenv

load_dotenv()

class WebDriverManagerRo:
    def __init__(self, browser="chrome", username=None, password=None):
        self.logger = logging.getLogger("RoAutomation")
        self.username = username or os.getenv("USERNAME_RO")
        self.password = password or os.getenv("PASSWORD_RO")
        self.driver = None
        self.initialize_driver(browser)

    def initialize_driver(self, browser):
        try:
            options = ChromeOptions()
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
            self.logger.info("WebDriver inicializado com sucesso.")
        except Exception as e:
            self.logger.error(f"Erro ao inicializar WebDriver: {str(e)}")
            raise

    async def navigate_to(self, url):
        try:
            self.driver.get(url)
            self.logger.info(f"Navegado para: {url}")
            await asyncio.sleep(1)  # Pequena espera para estabilizar
        except Exception as e:
            self.logger.error(f"Erro ao navegar para {url}: {str(e)}")
            raise

    async def login_gov(self):
        try:
            self.logger.info("Efetuando login...")
            usuario_field = await self.wait_and_find(By.NAME, "usuario")
            usuario_field.send_keys(self.username)

            senha_field = await self.wait_and_find(By.NAME, "senha")
            senha_field.send_keys(self.password)
            senha_field.send_keys(Keys.ENTER)

            self.logger.info("Login realizado com sucesso")
            await asyncio.sleep(2)
        except Exception as e:
            self.logger.error(f"Erro no login: {str(e)}")
            raise

    async def wait_and_find(self, by, value, timeout=15):
        try:
            element = WebDriverWait(self.driver, timeout).until(
                EC.visibility_of_element_located((by, value))
            )
            WebDriverWait(self.driver, timeout).until(
                EC.element_to_be_clickable((by, value))
            )
            return element
        except TimeoutException:
            self.logger.error(f"Elemento não encontrado: {by}={value}")
            raise
        except Exception as e:
            self.logger.error(f"Erro ao encontrar elemento {by}={value}: {str(e)}")
            raise

    async def clear_field(self, field):
        try:
            self.driver.execute_script(
                "arguments[0].scrollIntoView({block: 'center'});", field
            )
            await asyncio.sleep(0.1)
            field.clear()
            value = field.get_attribute("value")
            if value:
                self.logger.warning(f"Campo não limpo. Valor atual: {value}")
                field.send_keys(Keys.CONTROL + "a")
                field.send_keys(Keys.DELETE)
                await asyncio.sleep(0.1)
            self.logger.info("Campo limpo com sucesso.")
        except Exception as e:
            self.logger.error(f"Erro ao limpar campo: {str(e)}")
            raise

    async def close_modal_if_present(self):
        try:
            self.logger.info("Verificando se há modal aberto...")
            backdrops = self.driver.find_elements(By.CSS_SELECTOR, "div.q-dialog__backdrop")
            if backdrops and backdrops[0].is_displayed():
                self.logger.info("Modal encontrado. Tentando fechar...")
                try:
                    confirm_button = self.driver.find_element(
                        By.XPATH,
                        '//button[contains(@class, "q-btn") and .//span[contains(text(), "Confirmar")]]',
                    )
                    self.driver.execute_script(
                        "arguments[0].scrollIntoView({block: 'center'});", confirm_button
                    )
                    confirm_button.click()
                    self.logger.info("Modal fechado com botão Confirmar.")
                except NoSuchElementException:
                    self.logger.info("Botão Confirmar não encontrado. Tentando fechar...")
                    try:
                        close_button = self.driver.find_element(
                            By.XPATH,
                            '//button[contains(@class, "q-btn") and (.//i[contains(@class, "material-icons") and text()="close"] or .//span[contains(text(), "Fechar")])]',
                        )
                        close_button.click()
                        self.logger.info("Modal fechado com botão de fechar.")
                    except NoSuchElementException:
                        self.logger.info("Botão de fechar não encontrado. Removendo modal...")
                        self.driver.execute_script(
                            """
                            const dialog = document.querySelector('div.q-dialog');
                            const backdrop = document.querySelector('div.q-dialog__backdrop');
                            if (dialog) dialog.remove();
                            if (backdrop) backdrop.remove();
                            """
                        )
                        self.logger.info("Modal removido via JavaScript.")
                await asyncio.sleep(1.5)
            else:
                self.logger.info("Nenhum modal encontrado.")
        except Exception as e:
            self.logger.error(f"Erro ao fechar modal: {str(e)}")

    async def fill_form_fields(self, cpf, matricula="", pensionista="N"):
        try:
            self.logger.info("Preenchendo formulário...")
            await self.close_modal_if_present()

            cpf_field = await self.wait_and_find(By.CSS_SELECTOR, 'input[name="cpf"]')
            self.driver.execute_script(
                "arguments[0].scrollIntoView({block: 'center'});", cpf_field
            )
            await self.clear_field(cpf_field)
            cpf_field.send_keys(cpf)
            await asyncio.sleep(0.5)

            if matricula:
                matricula_field = await self.wait_and_find(
                    By.CSS_SELECTOR, 'input[name="matricula"]'
                )
                await self.clear_field(matricula_field)
                matricula_field.send_keys(matricula)
                await asyncio.sleep(0.2)

            await self.select_pensionista(pensionista)
            await self.click_buscar_servidor()
        except Exception as e:
            self.logger.error(f"Erro no preenchimento do formulário: {str(e)}")
            raise

    async def select_pensionista(self, option="N"):
        try:
            option = option.upper()
            if option not in ["S", "N"]:
                raise ValueError("Opção inválida para pensionista. Use 'S' ou 'N'.")
            
            label = "Sim" if option == "S" else "Não"
            self.logger.info(f"Selecionando pensionista: {label}")
            selector = f'div.q-radio[aria-label="{label}"]'
            pensionista_option = await self.wait_and_find(By.CSS_SELECTOR, selector)
            pensionista_option.click()
            self.logger.info(f"Pensionista '{label}' selecionado com sucesso")
        except Exception as e:
            self.logger.error(f"Erro ao selecionar pensionista: {str(e)}")
            raise

    async def click_buscar_servidor(self):
        try:
            self.logger.info('Clicando no botão "Buscar Servidor"...')
            try:
                buscar_btn = await self.wait_and_find(
                    By.XPATH, '//button[.//span[contains(., "Buscar Servidor")]]'
                )
            except TimeoutException:
                buscar_btn = await self.wait_and_find(
                    By.CSS_SELECTOR, "button.q-btn.bg-primary.text-white"
                )
            buscar_btn.click()
            self.logger.info('Botão "Buscar Servidor" clicado com sucesso')
            await asyncio.sleep(10)
        except Exception as e:
            self.logger.error(f"Erro ao clicar no botão Buscar Servidor: {str(e)}")
            raise

    async def extract_table_data(self):
        try:
            self.logger.info("Extraindo dados da tabela...")
            modal = await self.wait_and_find(By.CSS_SELECTOR, "div.q-dialog", timeout=5)
            try:
                table = modal.find_element(
                    By.CSS_SELECTOR, 'table[cellspacing="0"][cellpadding="0"]'
                )
                WebDriverWait(self.driver, 5).until(EC.visibility_of(table))
            except NoSuchElementException:
                self.logger.info("Tabela não encontrada ou não carregada no modal.")
                return {"headers": [], "rows": []}

            headers = []
            header_elements = table.find_elements(
                By.CSS_SELECTOR, 'thead th:not([style*="display: none"])'
            )
            for header in header_elements:
                text = header.text.strip()
                if text:
                    headers.append(text)

            rows = []
            row_containers = table.find_elements(
                By.XPATH,
                './/tbody//tr[not(contains(@style,"display: none")) and not(.//td[contains(text(), "Nenhum registro encontrado")])]',
            )
            for row in row_containers:
                row_data = {}
                cells = row.find_elements(By.CSS_SELECTOR, 'td:not([style*="display: none"])')
                for i, cell in enumerate(cells):
                    if i < len(headers):
                        try:
                            row_data[headers[i]] = cell.text.strip()
                        except Exception as e:
                            self.logger.warning(f"Erro ao ler célula {i}: {str(e)}")
                            row_data[headers[i]] = ""
                
                if row_data.get("Margem disponível") == "Sem Margem" or row_data.get("Margem Cartão") == "Sem Margem":
                    self.logger.info(f"Ignorando linha com matrícula {row_data.get('Matricula')} por conter 'Sem Margem'")
                    continue
                
                if row_data:
                    rows.append(row_data)

            self.logger.info(f"Encontrados {len(rows)} registros válidos na tabela")
            if rows:
                self.logger.info(f"Dados extraídos: {rows}")
            return {"headers": headers, "rows": rows}
        except Exception as e:
            self.logger.error(f"Erro ao extrair dados da tabela: {str(e)}")
            return {"headers": [], "rows": []}

    async def scrape_margins(self):
        try:
            self.logger.info("Coletando dados das margens...")
            margem_disponivel = await self.wait_and_find(By.CSS_SELECTOR, 'div.q-badge.bg-blue')
            margem_cartao = await self.wait_and_find(By.CSS_SELECTOR, 'div.q-badge.bg-red:nth-of-type(1)')
            margem_cartao_beneficio = await self.wait_and_find(By.CSS_SELECTOR, 'div.q-badge.bg-red:nth-of-type(2)')
            
            nome = await self.wait_and_find(By.CSS_SELECTOR, 'span.text-weight-bold')
            cpf = await self.wait_and_find(By.XPATH, '//span[contains(text(), "CPF:")]/following-sibling::text()')
            matricula = await self.wait_and_find(By.XPATH, '//span[contains(text(), "Matrícula:")]/following-sibling::text()')

            margins_data = {
                "nome": nome.text.strip(),
                "cpf": cpf.text.strip(),
                "matricula": matricula.text.strip(),
                "margemDisponivel": margem_disponivel.text.strip(),
                "margemCartao": margem_cartao.text.strip(),
                "margemCartaoBeneficio": margem_cartao_beneficio.text.strip(),
                "timestamp": datetime.now().isoformat(),
            }

            if any(
                margem in ["Sem Margem", ""]
                for margem in [
                    margins_data["margemDisponivel"],
                    margins_data["margemCartao"],
                    margins_data["margemCartaoBeneficio"],
                ]
            ):
                self.logger.info("Ignorando dados com 'Sem Margem' na página de detalhes.")
                return None

            return margins_data
        except Exception as e:
            self.logger.error(f"Erro ao raspar dados das margens: {str(e)}")
            return None

    async def check_exists_url_ahead_search(self):
        try:
            current_url = self.driver.current_url
            self.logger.info(f"URL atual: {current_url}")

            if os.getenv("URL_RO_CHECK") in current_url:
                self.logger.info("Redirecionado para página de resultado. Coletando dados...")
                margins_data = await self.scrape_margins()
                
                await self.navigate_to(os.getenv("URL_CONSULT"))
                await asyncio.sleep(3)
                self.logger.info(f"Retornou para: {self.driver.current_url}")
                await self.close_modal_if_present()
                return margins_data
            else:
                self.logger.info("Não foi necessário voltar. URL atual é adequada.")
                await self.close_modal_if_present()
                return None
        except Exception as e:
            self.logger.error(f"Erro ao verificar ou redirecionar URL: {str(e)}")
            return None

    async def handle_modal_with_margins(self):
        try:
            self.logger.info("Processando modal com margens...")
            table_data = await self.extract_table_data()

            if table_data["rows"]:
                self.logger.info(f"Dados da tabela encontrados: {table_data}")
                valid_rows = table_data["rows"]
                if valid_rows:
                    first_row_matricula = valid_rows[0]["Matricula"]
                    self.logger.info(f"Selecionando linha com matrícula {first_row_matricula}...")
                    radio_button = await self.wait_and_find(
                        By.XPATH,
                        f'//tr[td[contains(text(), "{first_row_matricula}")]]//div[@role="radio"]',
                    )
                    radio_button.click()
                    await asyncio.sleep(2)

                    margins_data = await self.check_exists_url_ahead_search()
                    return {"margins": margins_data} if margins_data else table_data

            self.logger.info("Nenhuma tabela válida encontrada, verificando URL para margens...")
            margins_data = await self.check_exists_url_ahead_search()
            await self.close_modal_if_present()
            return {"margins": margins_data} if margins_data else {"headers": [], "rows": []}
        except Exception as e:
            self.logger.error(f"Erro ao processar modal: {str(e)}")
            return {"headers": [], "rows": []}

    async def quit(self):
        try:
            if self.driver:
                self.driver.quit()
                self.logger.info("Navegador fechado com sucesso")
        except Exception as e:
            self.logger.error(f"Erro ao fechar o navegador: {str(e)}")
            raise