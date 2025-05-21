# src/core/executer.py

import time
import os
from dotenv import load_dotenv
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
    def __init__(self):
        self.page_objects = PageObject()
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
                    self.update_has_filter_cpf(cpf)
                    self.page_objects.driver.refresh()
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
    pool = ScrapePoolExecute()
    pool.run()
