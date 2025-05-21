import os

from dotenv import load_dotenv
from sqlalchemy.orm import mapped_column, Mapped, DeclarativeBase
from sqlalchemy import Column, Integer, String, Boolean, create_engine
from sqlalchemy.orm import sessionmaker


load_dotenv()

DATABASE_URL = os.getenv("SQLALCHEMY_DATABASE_URI")


class Base(DeclarativeBase):
    pass


engine = create_engine(
    DATABASE_URL, connect_args={"options": "-csearch_path=spreed"}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class SearchRo(Base):
    __tablename__ = "ro"
    __table_args__ = {"schema": "spreed"}

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    nome: Mapped[str] = mapped_column(String(100))
    cpf: Mapped[str] = mapped_column(String(11))
    sexo: Mapped[str] = mapped_column(String(10))
    endereco: Mapped[str] = mapped_column(String(255))
    numero: Mapped[int] = mapped_column(Integer)
    complemento: Mapped[str] = mapped_column(String(255))
    bairro: Mapped[str] = mapped_column(String(255))
    cidade: Mapped[str] = mapped_column(String(255))
    uf: Mapped[str] = mapped_column(String(2))
    cep: Mapped[str] = mapped_column(String(10))
    celular1: Mapped[str] = mapped_column(String(20))
    whatsapp1: Mapped[str] = mapped_column(String(20))
    celular2: Mapped[str] = mapped_column(String(20))
    whatsapp2: Mapped[str] = mapped_column(String(20))
    celular3: Mapped[str] = mapped_column(String(20))
    whatsapp3: Mapped[str] = mapped_column(String(20))
    fixo1: Mapped[str] = mapped_column(String(20))
    fixo2: Mapped[str] = mapped_column(String(20))
    fixo3: Mapped[str] = mapped_column(String(20))
    data_nascimento: Mapped[str] = mapped_column(String(10))
    idade: Mapped[int] = mapped_column(Integer)
    email1: Mapped[str] = mapped_column(String(255))
    email2: Mapped[str] = mapped_column(String(255))
    email3: Mapped[str] = mapped_column(String(255))
    renda: Mapped[str] = mapped_column(String(100))
    nome_mae: Mapped[str] = mapped_column(String(255))
    nomenclatura_escolaridade: Mapped[str] = mapped_column(String(100))
    has_filter: Mapped[bool] = mapped_column(Boolean, default=False)

    def __repr__(self):
        return f"Registred sucessfully: {self.id}"


class ResultSearchRo(Base):
    __tablename__ = "result_search_ro"
    __table_args__ = {"schema": "spreed"}

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    nome: Mapped[str] = mapped_column(String(225))
    matricula: Mapped[str] = mapped_column(String(100))
    cpf: Mapped[str] = mapped_column(String(30))
    cargo: Mapped[str] = mapped_column(String(225))
    lotacao: Mapped[str] = mapped_column(String(255))
    classificacao: Mapped[str] = mapped_column(String(255))
    margem_disponivel: Mapped[str] = mapped_column(String(30))
    margem_cartao: Mapped[str] = mapped_column(String(30))
    margem_cartao_beneficio: Mapped[str] = mapped_column(String(30))

    def __repr__(self):
        return f"Registred result search ro sucessfully: {self.id}"
