
import os
import json
import requests
from sqlalchemy.orm import Session
from decimal import Decimal
from sqlalchemy import select, update
from sqlalchemy import select, update
from dotenv import load_dotenv

from src.core.scraper_token import ScrapePoolExecute
from src.log.logger import LoggerWebDriverManager, setup_logger
from src.database.schemas import ResultSearchRo, SessionLocal, SearchRo



# Carregar variáveis do .env
load_dotenv()

logger = setup_logger()
driver_logger = LoggerWebDriverManager(logger=logger)

class ExtractTransformLoad:
    def __init__(self):
        self.base_url = os.getenv("ROUTE_RO")
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
    
    def _format_cpf(self, cpf: str) -> str:
        """Garante 11 dígitos e formata CPF para 000.000.000-00"""
        cpf = str(cpf).zfill(11)
        new_cpf = f"{cpf[:3]}.{cpf[3:6]}.{cpf[6:9]}-{cpf[9:]}"
        return new_cpf.replace(".", "").replace("-", "")

    def cpfs_database(self):
        try:
            db = SessionLocal()
            stmt = select(SearchRo.cpf).where(SearchRo.has_filter.__eq__(False))
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
            driver_logger.logger.info(f"✅ Resultados do CPF {cpf} salvos com sucesso")

        except Exception as e:
            db.rollback()
            driver_logger.logger.error(f"❌ Erro ao salvar resultados do CPF {cpf}: {str(e)}")
            raise
        finally:
            db.close()

    def get_request(self, cpf: str):
        if not self.token:
            raise ValueError("Token not loaded. Call 'load_token()' first.")

        url = self.base_url.format(cpf=self._format_cpf(cpf))
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
                "Authorization": f"Bearer {self.token}",
            }
            response = requests.get(url, headers=headers, timeout=10)

            if response.status_code == 200:
                data = response.json()
                print(f"\n✅ Dados do CPF {cpf} capturados com sucesso:")
                # print(json.dumps(data, indent=4, ensure_ascii=False))
                self.update_has_filter_cpf(cpf)

                if isinstance(data, list) and len(data) > 0:
                    self.save_result(data, cpf)

                return data

            elif response.status_code == 401:
                logger.warning("⚠️ Token expirado, iniciando renovação com Selenium...")

                username = os.getenv("USERNAME_RO")
                password = os.getenv("PASSWORD_RO")
                scrape_pool = ScrapePoolExecute(username=username, password=password)

                new_token = scrape_pool.run()
                if not new_token:
                    raise RuntimeError("Falha ao renovar token com Selenium.")

                self.token = new_token  # atualiza token
                logger.info("🔄 Token atualizado com sucesso, repetindo requisição...")
                return self.get_request(cpf)  # repete a requisição do mesmo CPF

            else:
                logger.error(f"❌ Erro {response.status_code} para CPF {cpf}")
                return None

        except requests.exceptions.RequestException as e:
            logger.error(f"Request falhou para CPF {cpf}: {e}")
            return None
        except Exception as e:
            logger.error(f"Erro inesperado para CPF {cpf}: {e}")
            return None
