import asyncpg
from dotenv import load_dotenv
import os
import logging

load_dotenv()

class DatabaseManagerPostgreSQL:
    # TODO - verificar o cpf_raw, e ve oque está acontecendo por que não ta inserindo a lista de resultados
    def __init__(self):
        self.logger = logging.getLogger("RoAutomation")
        self.pool = None
        self.database_url = os.getenv("DATABASE_URL")

    async def connect(self):
        try:
            self.pool = await asyncpg.create_pool(self.database_url)
            self.logger.info("Conexão com o banco de dados estabelecida.")
        except Exception as e:
            self.logger.error(f"Erro ao conectar ao banco: {str(e)}")
            raise

    async def disconnect(self):
        if self.pool:
            await self.pool.close()
            self.logger.info("Conexão com o banco de dados fechada.")

    async def select_ro_data(self, batch_size):
        try:
            async with self.pool.acquire() as connection:
                query = """
                SELECT 
                    regexp_replace(cpf, '(\\d{3})(\\d{3})(\\d{3})(\\d{2})', '\\1.\\2.\\3-\\4') AS cpf_formatado,
                    cpf AS cpf_raw
                FROM 
                    spreed.ro 
                WHERE 
                    has_filter = FALSE
                LIMIT $1;
                """
                rows = await connection.fetch(query, batch_size)
                return [
                    {"cpf_formatado": row["cpf_formatado"], "cpf_raw": row["cpf_raw"]}
                    for row in rows
                ]
        except Exception as e:
            self.logger.error(f"Erro ao selecionar dados RO: {str(e)}")
            raise

    async def insert_result_search_ro(
        self, nome, cpf, margem_disponivel, margem_cartao, margem_cartao_beneficio
    ):
        try:
            async with self.pool.acquire() as connection:
                query = """
                    INSERT INTO spreed.result_search_ro (
                        name, cpf_search, margem_disponivel, margem_cartao, margem_cartao_beneficio, created_at
                    ) VALUES ($1, $2, $3, $4, $5, NOW())
                """
                await connection.execute(
                    query,
                    nome,
                    cpf,
                    margem_disponivel,
                    margem_cartao,
                    margem_cartao_beneficio,
                )
                self.logger.info(f"Dados inseridos/atualizados para CPF {cpf}")
        except Exception as e:
            self.logger.error(f"Erro ao inserir resultado para CPF {cpf}: {str(e)}")
            raise

    async def insert_has_filter(self, cpf_raw):
        try:
            async with self.pool.acquire() as connection:
                query = """
                    UPDATE spreed.ro
                    SET has_filter = TRUE
                    WHERE cpf_raw = $1;
                """
                await connection.execute(query, cpf_raw)
                self.logger.info(f"has_filter atualizado para CPF {cpf_raw}")
        except Exception as e:
            self.logger.error(f"Erro ao atualizar has_filter para CPF {cpf_raw}: {str(e)}")
            raise

    async def get_pending_count(self):
        try:
            async with self.pool.acquire() as connection:
                query = """
                    SELECT COUNT(*) AS count
                    FROM spreed.ro
                    WHERE has_filter = FALSE OR has_filter IS NULL;
                """
                result = await connection.fetchrow(query)
                return result["count"]
        except Exception as e:
            self.logger.error(f"Erro ao contar CPFs pendentes: {str(e)}")
            raise