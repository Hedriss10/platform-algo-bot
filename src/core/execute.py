# src/core/executer.py

import time
import os
from typing import List, Tuple
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import case, func, select, update
from src.core.scraper import PageObject
from src.database.schemas import SearchRo, SessionLocal
from src.log.logger import LoggerWebDriverManager, setup_logger

load_dotenv()

URL_CONSULT = os.getenv("URL_CONSULT")


logger = setup_logger()
driver_logger = LoggerWebDriverManager(logger=logger)


MAX_LENGTH = 11

class ScrapePoolExecute:
    def __init__(self, username: str, password: str, *args, **kwargs):
        self.page_objects = PageObject(username=username, password=password)
        driver_logger.register_logger(driver=self.page_objects.driver)
        self.search_ro = SearchRo

    def colect_cpfs(self):
        try:
            db = SessionLocal()
            driver_logger.logger.info("Start collecting CPFs from database")

            cpf_col = self.search_ro.cpf

            formatted_cpf = case(
                (
                    func.length(cpf_col) == MAX_LENGTH,
                    func.concat(
                        func.substr(cpf_col, 1, 3),
                        ".",
                        func.substr(cpf_col, 4, 3),
                        ".",
                        func.substr(cpf_col, 7, 3),
                        "-",
                        func.substr(cpf_col, 10, 2),
                    ),
                ),
                else_=None,
            ).label("cpf")

            stmt = select(formatted_cpf).where(~self.search_ro.has_filter)
            result_raw = db.execute(stmt).fetchall()

            cpfs = [row[0] for row in result_raw if row[0]]
            cpfs_str = ";".join(cpfs)
            return cpfs_str

        except Exception as e:
            driver_logger.logger.error(f"Error collecting CPFs: {str(e)}")
            raise

    def update_has_filter_cpf(self, cpf):
        try:
            db = SessionLocal()
            cpf_str = cpf.replace(".", "").replace("-", "")
            stmt = (
                update(self.search_ro)
                .where(self.search_ro.cpf == cpf_str)
                .values(has_filter=True)
            )
            db.execute(stmt)
            db.commit()
            driver_logger.logger.info(f"CPF {cpf} has filter updated")
        except Exception as e:
            driver_logger.logger.error(
                f"Error update_has_filter_cpf with cpfs: {str(e)}"
            )
            raise
        finally:
            db.close()

    def scrpaer_pool(self):
        try:
            db_session = SessionLocal()
            cpfs = self.colect_cpfs().split(";")
            for i, cpf in enumerate(cpfs):
                driver_logger.logger.info(f"Processing CPF {i+1}/{len(cpfs)}: {cpf}")
                if self.page_objects.fill_form_fields(cpf):
                    time.sleep(2)
                    self.page_objects.search_table(db_session)
                    time.sleep(2)
                    self.update_has_filter_cpf(cpf)
                    time.sleep(2)
                    self.page_objects.driver.refresh()
                    time.sleep(2)
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
            driver_logger.logger.error(
                f"Error scrpaer_pool with cpfs: {str(e)}"
            )
            raise
        

if __name__ == "__main__":
    
    def parse_cpfs_and_password(file_path: str) -> Tuple[str, List[str]]:
        try:
            with open(file_path, "r") as f:
                lines = [line.strip() for line in f.readlines() if line.strip()]

            header = lines[0]
            cpf_lines = lines[1:]

            # Extrai a senha
            password = header.split('PASSWORD:')[1].strip().strip('"')

            # Junta todas as linhas de CPFs, remove duplicados/vazios
            raw_cpfs = ",".join(cpf_lines).split(",")
            cpfs = list({cpf.strip() for cpf in raw_cpfs if cpf.strip()})

            return password, cpfs

        except Exception as e:
            raise ValueError(f"Erro ao ler o arquivo: {str(e)}")


    def execute_scraping(cpf: str, password: str):
        try:
            print(f"[START] Execute scraping for CPF: {cpf}")
            pool = ScrapePoolExecute(username=cpf, password=password)
            pool.run()
            print(f"[OK] Finished CPF: {cpf}")
        except Exception as e:
            print(f"[ERRO] falied for CPF {cpf}: {str(e)}")


    def interface(file: str, max_workers: int = 8):
        try:
            password, cpfs_list = parse_cpfs_and_password(file)

            print(f"🔐 password unique for cpfs: {password}")
            print(f"👥 total of CPFs: {len(cpfs_list)} | Threads: {max_workers}")

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [
                    executor.submit(execute_scraping, cpf, password)
                    for cpf in cpfs_list
                ]
                for future in as_completed(futures):
                    _ = future.result()

        except Exception as e:
            print(f"[Faied interface] {str(e)}")
            
            
    interface(file="cpfs.txt", max_workers=8)