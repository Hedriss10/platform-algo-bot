# src/core/executer.py

import time
import os
from typing import List, Tuple

from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import case, func, select, update, text
from src.core.scraper_ro import PageObject
from src.database.schemas import SearchRo, SessionLocal
from src.log.logger import LoggerWebDriverManager, setup_logger

load_dotenv()

URL_CONSULT = os.getenv("URL_CONSULT")


logger = setup_logger()
driver_logger = LoggerWebDriverManager(logger=logger)


MAX_LENGTH = 11
MAX_CPF_TO_PROCESS = 8

class ScrapePoolExecute:
    def __init__(self, username: str, password: str, cpfs_to_process: List[str], *args, **kwargs):
        self.page_objects = PageObject(username=username, password=password)
        driver_logger.register_logger(driver=self.page_objects.driver)
        self.cpfs_to_process = cpfs_to_process

    def _format_cpf(self, cpf: str) -> str:
        return f"{cpf[:3]}.{cpf[3:6]}.{cpf[6:9]}-{cpf[9:]}"

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

    def scrpaer_pool(self):
        db_session = SessionLocal()
        try:
            for i, cpf_raw in enumerate(self.cpfs_to_process):
                cpf = self._format_cpf(cpf_raw)
                driver_logger.logger.info(f"Processing CPF {i+1}/{len(self.cpfs_to_process)}: {cpf}")
                if self.page_objects.fill_form_fields(cpf):
                    time.sleep(1)
                    self.page_objects.search_table(db_session)
                    time.sleep(1)
                    self.update_has_filter_cpf(cpf)
                    time.sleep(1)
                    self.page_objects.driver.refresh()
                    time.sleep(1)
                    self.page_objects.driver.get(URL_CONSULT)
                else:
                    self.page_objects.driver.refresh()

            driver_logger.logger.info("Scraping completed")
        except Exception as e:
            driver_logger.logger.error(f"Error scrpaer_pool with cpfs: {str(e)}")
            raise
        finally:
            db_session.close()

    def run(self):
        try:
            self.page_objects.login_gov()
            self.scrpaer_pool()
        except Exception as e:
            driver_logger.logger.error(f"Error running scraping pool: {str(e)}")
            raise


if __name__ == "__main__":

    def parse_cpfs_and_password(file_path: str):
        try:
            with open(file_path, "r") as f:
                lines = [line.strip() for line in f.readlines() if line.strip()]

            header = lines[0]
            cpf_lines = lines[1:]

            # Extrai a senha única
            password = header.split('PASSWORD:')[1].strip().strip('"')

            # Usuários (usernames) no arquivo são linhas de CPFs no TXT que logam no CRM
            usernames = list({cpf.strip() for cpf in cpf_lines if cpf.strip()})

            return password, usernames

        except Exception as e:
            raise ValueError(f"Erro ao ler o arquivo: {str(e)}")

    def chunk_list(lst: List[str], n: int):
        # Divide a lista lst em pedaços de tamanho n (exceto o último que pode ser menor)
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def execute_scraping(username: str, password: str, cpfs_to_process: List[str]):
        try:
            print(f"[START] Execute scraping for user: {username} | CPFs to process: {len(cpfs_to_process)}")
            pool = ScrapePoolExecute(username=username, password=password, cpfs_to_process=cpfs_to_process)
            pool.run()
            print(f"[OK] Finished user: {username}")
        except Exception as e:
            print(f"[ERRO] failed for user {username}: {str(e)}")

    def interface(file: str, max_workers: int = 8):
        try:
            password, usernames = parse_cpfs_and_password(file)

            print(f"🔐 Password unique for users: {password}")
            print(f"👥 Total of users: {len(usernames)} | Threads: {max_workers}")

            # Pega todos os CPFs pendentes no banco (has_filter = False)
            db = SessionLocal()
            stmt = select(SearchRo.cpf).where(~SearchRo.has_filter)
            all_cpfs = [row[0] for row in db.execute(stmt).fetchall()]
            db.close()

            print(f"📋 Total CPFs to process: {len(all_cpfs)}")

            # Divide os CPFs entre os usuários (agentes)
            chunk_size = len(all_cpfs) // len(usernames) + 1
            cpfs_chunks = list(chunk_list(all_cpfs, chunk_size))

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []
                for i, username in enumerate(usernames):
                    cpfs_for_agent = cpfs_chunks[i] if i < len(cpfs_chunks) else []
                    futures.append(executor.submit(execute_scraping, username, password, cpfs_for_agent))

                for future in as_completed(futures):
                    _ = future.result()

        except Exception as e:
            print(f"[Failed interface] {str(e)}")

    interface(file="cpfs.txt", max_workers=8)