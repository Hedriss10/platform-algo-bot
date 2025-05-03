import asyncio
import os
from dotenv import load_dotenv
from src.web.webdrivermanager import WebDriverManagerRo
from src.database.database import DatabaseManagerPostgreSQL
from src.log.logger import setup_logger
import re
from datetime import datetime

load_dotenv()

logger = setup_logger()

URL_GOV = os.getenv("URL_RO")
URL_CONSULT = os.getenv("URL_CONSULT")
USERNAME_RO = os.getenv("USERNAME_RO")
PASSWORD_RO = os.getenv("PASSWORD_RO")

CONFIG = {
    "batch_size": 100,
    "retry_attempts": 3,
    "retry_delay_ms": 2000,
}

async def execute_ro():
    manager = None
    db = None

    async def initialize_manager():
        nonlocal manager
        if manager:
            await manager.quit()
        manager = WebDriverManagerRo("chrome", USERNAME_RO, PASSWORD_RO)
        await manager.navigate_to(URL_GOV)
        await manager.login_gov()
        await manager.navigate_to(URL_CONSULT)
        await asyncio.sleep(1)

    try:
        logger.info("Iniciando automação...")

        missing_vars = []
        if not URL_GOV:
            missing_vars.append("URL_RO")
        if not URL_CONSULT:
            missing_vars.append("URL_CONSULT")
        if not USERNAME_RO:
            missing_vars.append("USERNAME_RO")
        if not PASSWORD_RO:
            missing_vars.append("PASSWORD_RO")

        if missing_vars:
            raise ValueError(
                f"Variáveis de ambiente faltando: {', '.join(missing_vars)}. Verifique o arquivo .env"
            )

        db = DatabaseManagerPostgreSQL()
        await db.connect()

        pending_count = await db.get_pending_count()
        logger.info(f"Total de CPFs pendentes: {pending_count}")

        await initialize_manager()

        batch_number = 0
        while True:
            batch_number += 1
            logger.info(f"Iniciando lote {batch_number}...")
            cpfs_from_db = await db.select_ro_data(CONFIG["batch_size"])
            if not cpfs_from_db:
                logger.info("Nenhum CPF pendente encontrado no banco.")
                break

            logger.info(f"Processando lote de {len(cpfs_from_db)} CPFs...")

            for index, cpf_data in enumerate(cpfs_from_db):
                raw_cpf = re.sub(r"\D", "", cpf_data["cpf_formatado"])
                logger.info(
                    f"Processando {index + 1}/{len(cpfs_from_db)} (lote {batch_number}): CPF {cpf_data['cpf_formatado']}"
                )

                attempt = 0
                success = False

                while attempt < CONFIG["retry_attempts"] and not success:
                    try:
                        current_url = manager.driver.current_url
                        if URL_CONSULT not in current_url:
                            logger.info(
                                f"URL atual ({current_url}) não é a de consulta. Navegando para {URL_CONSULT}..."
                            )
                            await manager.navigate_to(URL_CONSULT)
                            await asyncio.sleep(1)

                        await manager.fill_form_fields(cpf=raw_cpf)
                        result_data = await manager.handle_modal_with_margins()

                        if result_data.get("margins"):
                            margins = result_data["margins"]
                            logger.info(f"Dados coletados para CPF {margins['cpf']}: {margins}")
                            await db.insert_result_search_ro(
                                margins.get("nome"),
                                raw_cpf,
                                margins.get("margemDisponivel"),
                                margins.get("margemCartao"),
                                margins.get("margemCartaoBeneficio"),
                            )
                        elif result_data.get("rows"):
                            logger.info(f"Dados da tabela não esperados: {result_data['rows']}")
                            await db.insert_result_search_ro(None, raw_cpf, None, None, None)
                        else:
                            logger.info(
                                f"Nenhum dado válido encontrado para o CPF {cpf_data['cpf_formatado']}"
                            )
                            await db.insert_result_search_ro(None, raw_cpf, None, None, None)

                        await db.insert_has_filter(cpf_data["cpf_raw"])
                        success = True
                    except Exception as e:
                        attempt += 1
                        logger.error(
                            f"Erro ao processar CPF {cpf_data['cpf_formatado']} (tentativa {attempt}/{CONFIG['retry_attempts']}): {str(e)}"
                        )
                        if attempt < CONFIG["retry_attempts"]:
                            if "no such window" in str(e).lower():
                                logger.info("Navegador fechado inesperadamente. Reiniciando...")
                                await initialize_manager()
                            else:
                                await asyncio.sleep(CONFIG["retry_delay_ms"] / 1000)
                                await manager.navigate_to(URL_CONSULT)
                                await asyncio.sleep(1)
                        else:
                            logger.info(
                                f"Falha após {CONFIG['retry_attempts']} tentativas para CPF {cpf_data['cpf_formatado']}. Salvando como erro."
                            )
                            await db.insert_result_search_ro(None, raw_cpf, None, None, None)
                            await db.insert_has_filter(cpf_data["cpf_raw"])

    except Exception as e:
        logger.error(f"Falha na execução da automação: {str(e)}")
        raise
    finally:
        if manager:
            await manager.quit()
        if db:
            await db.disconnect()
        logger.info("Automação finalizada.")

if __name__ == "__main__":
    asyncio.run(execute_ro())