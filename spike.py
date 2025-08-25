import os
import json
import requests
import threading
import logging

# Configuração do logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from decimal import Decimal
from typing import List, Optional
from src.database.schemas import SearchRo  
from src.database.schemas import ResultSearchRo, SessionLocal 
from src.core.scraper_token import ScrapePoolExecute
from sqlalchemy import select, update
from sqlalchemy.orm import Session
from concurrent.futures import ThreadPoolExecutor, as_completed

class ExtractTransformLoad:
    def __init__(self):
        self.base_url = os.getenv("ROUTE_RO")  # Ex.: "https://consignacao.sistemas.ro.gov.br/..."
        self.token = None
        self.token_lock = threading.Lock()  # Lock para renovação segura do token


    def save_result(self, data: list, cpf: str):
        """Salva o resultado da consulta no banco"""
        db: Session = SessionLocal()
        try:
            for item in data:
                margem_disponivel = str(item.get("margemDisponivel", 0.0))
                margem_cartao = str(item.get("margemCartaoDisponivel", 0.0))
                margem_cartao_beneficio = str(item.get("margemCartaoBeneficio", 0.0))

                result = ResultSearchRo(
                    nome=item.get("nomFuncionario", "").strip(),
                    matricula=str(item.get("numMatricula", "")),
                    cpf=cpf,
                    cargo=item.get("nomCargo", "").strip(),
                    lotacao=item.get("nomLotacao", "").strip(),
                    classificacao=item.get("nomClassificacao", "").strip(),
                    margem_disponivel=margem_disponivel,
                    margem_cartao=margem_cartao,
                    margem_cartao_beneficio=margem_cartao_beneficio,
                    nome_cargo=item.get("nomCargo", "").strip(),
                    situacao=item.get("situacao", "").strip(),
                    is_pensionista=item.get("isPensionista", ""),
                    list_status=True if any([
                        Decimal(margem_disponivel) > 0,
                        Decimal(margem_cartao) > 0,
                        Decimal(margem_cartao_beneficio) > 0
                    ]) else False
                )
                db.add(result)

            db.commit()
            logger.info(f"✅ Resultados do CPF {cpf} salvos com sucesso")

        except Exception as e:
            db.rollback()
            logger.error(f"❌ Erro ao salvar resultados do CPF {cpf}: {str(e)}")
            raise
        finally:
            db.close()


    def load_token(self) -> str:
        """Carrega o token de autenticação do arquivo JSON."""
        token_path = "token_response.json"
        try:
            if not os.path.exists(token_path):
                raise FileNotFoundError(f"Token file not found: {token_path}")
            with open(token_path, "r", encoding="utf-8") as f:
                token_data = json.load(f)
            if "access_token" not in token_data:
                raise KeyError("Token file does not contain 'access_token'")
            self.token = token_data["access_token"]
            logger.info("Token loaded successfully")
            return self.token
        except Exception as e:
            logger.error(f"Failed to load token: {str(e)}")
            raise

    def renew_token(self) -> None:
        """Renova o token em caso de erro 401."""
        with self.token_lock:  # Garante que apenas uma thread renove o token
            if self.token is not None:  # Verifica se o token já foi renovado por outra thread
                return
            try:
                username = os.getenv("USERNAME_RO")
                password = os.getenv("PASSWORD_RO")
                scrape_pool = ScrapePoolExecute(username=username, password=password)
                scrape_pool.run()
                self.load_token()  # Recarrega o token após renovação
                logger.info("Token renewed successfully")
            except Exception as e:
                logger.error(f"Failed to renew token: {str(e)}")
                raise

    def cpfs_database(self) -> List[str]:
        """Extrai CPFs do banco de dados."""
        db = SessionLocal()
        try:
            stmt = select(SearchRo.cpf).where(SearchRo.has_filter.__eq__(False))
            cpfs = db.scalars(stmt).all()
            logger.info(f"Retrieved {len(cpfs)} CPFs from database")
            return cpfs
        except Exception as e:
            logger.error(f"Error retrieving CPFs from database: {str(e)}")
            raise
        finally:
            db.close()

    def update_has_filter_cpf(self, cpf: str, db: Session) -> None:
        """Atualiza o campo has_filter no banco para um CPF."""
        try:
            cpf_str = cpf.replace(".", "").replace("-", "")
            stmt = (
                update(SearchRo)
                .where(SearchRo.cpf == cpf_str)
                .values(has_filter=True)
            )
            db.execute(stmt)
            db.commit()
            logger.info(f"CPF {cpf} has filter updated")
        except Exception as e:
            logger.error(f"Error updating has_filter for CPF {cpf}: {str(e)}")
            raise

    def get_request(self, cpf: str) -> Optional[dict]:
        """Faz requisição para a API usando CPF."""
        if not self.token:
            raise ValueError("Token not loaded. Call 'load_token()' first.")

        url = self.base_url.format(cpf=cpf)
        db = SessionLocal()  # Cada thread tem sua própria sessão
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
                "Authorization": f"Bearer {self.token}",
            }
            response = requests.get(url, headers=headers, timeout=10)

            if response.status_code == 200:
                data = response.json()
                logger.info(f"✅ Dados do CPF {cpf} capturados com sucesso")
                self.update_has_filter_cpf(cpf, db)
                
                if isinstance(data, list) and len(data) > 0:
                    self.save_result(data, cpf)
                    
            elif response.status_code == 401:
                logger.warning(f"Token expired for CPF {cpf}, attempting to renew")
                self.renew_token()
                headers["Authorization"] = f"Bearer {self.token}"
                response = requests.get(url, headers=headers, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    logger.info(f"✅ Dados do CPF {cpf} capturados com sucesso após renovação do token")
                    self.update_has_filter_cpf(cpf, db)
                    return data
                else:
                    logger.error(f"Request failed for CPF {cpf} after token renewal: {response.status_code}")
            else:
                logger.error(f"Request failed for CPF {cpf}: {response.status_code}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for CPF {cpf}: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error for CPF {cpf}: {str(e)}")
        finally:
            db.close()
        return None

    def run_etl(self, max_workers: int = 5) -> None:
        """Executa o processo ETL com múltiplas threads."""
        self.load_token()
        cpfs = self.cpfs_database()
        logger.info(f"Starting ETL process for {len(cpfs)} CPFs with {max_workers} threads")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_cpf = {executor.submit(self.get_request, cpf): cpf for cpf in cpfs}
            for future in as_completed(future_to_cpf):
                cpf = future_to_cpf[future]
                try:
                    result = future.result()
                    if result:
                        logger.info(f"Processed CPF {cpf} successfully")
                    else:
                        logger.warning(f"No data returned for CPF {cpf}")
                except Exception as e:
                    logger.error(f"Error processing CPF {cpf}: {str(e)}")

if __name__ == "__main__":
    etl = ExtractTransformLoad()
    etl.run_etl(max_workers=5)