# src/models/models.py
from pydantic import BaseModel


class ServidorSchema(BaseModel):
    nome: str
    matricula: str
    cpf: str
    cargo: str
    lotacao: str
    classificacao: str
    margem_disponivel: str
    margem_cartao: str
    margem_cartao_beneficio: str