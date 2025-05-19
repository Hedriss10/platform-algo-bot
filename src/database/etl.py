import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, insert
from sqlalchemy.orm import sessionmaker
from src.database.schemas import ResultSearchRo

load_dotenv()

DATABASE_URL = os.getenv("SQLALCHEMY_DATABASE_URI")
engine = create_engine(DATABASE_URL, connect_args={"options": "-csearch_path=spreed"})
Session = sessionmaker(bind=engine)

class InjectDataBaseManager:
    
    def __init__(self, file: str) -> None:
        self.file = file
    
    def inject_data_base(self):
        print("Lendo arquivo CSV...")
        df = pd.read_csv(self.file, sep=";", dtype=str, encoding="latin-1")
        
        df = df.where(pd.notnull(df), None)

        df["DATA_NASCIMENTO"] = pd.to_datetime(
            df["DATA_NASCIMENTO"], errors="coerce", dayfirst=True
        )
        df["DATA_NASCIMENTO"] = df["DATA_NASCIMENTO"].apply(
            lambda x: x.date() if pd.notnull(x) else None
        )

        df["IDADE"] = pd.to_numeric(df["IDADE"], errors="coerce")

        rename_map = {
            "CPF": "cpf",
            "NOME": "nome",
            "SEXO": "sexo",
            "ENDERECO": "endereco",
            "NUMERO": "numero",
            "COMPLEMENTO": "complemento",
            "BAIRRO": "bairro",
            "CIDADE": "cidade",
            "UF": "uf",
            "CEP": "cep",
            "CELULAR1": "celular1",
            "WHATSAPP1": "whatsapp1",
            "CELULAR2": "celular2",
            "WHATSAPP2": "whatsapp2",
            "CELULAR3": "celular3",
            "WHATSAPP3": "whatsapp3",
            "FIXO1": "fixo1",
            "FIXO2": "fixo2",
            "FIXO3": "fixo3",
            "DATA_NASCIMENTO": "data_nascimento",
            "IDADE": "idade",
            "EMAIL1": "email1",
            "EMAIL2": "email2",
            "EMAIL3": "email3",
            "RENDA": "renda",
            "NOME_MAE": "nome_mae",
            "NOMENCLATURA_ESCOLARIDADE": "nomenclatura_escolaridade"
        }

        df.rename(columns=rename_map, inplace=True)

        valid_columns = set(c.name for c in ResultSearchRo.__table__.columns if c.name != "id")
        records = [
            {k: v for k, v in row.items() if k in valid_columns}
            for row in df.to_dict(orient="records")
        ]
        try:
            with engine.begin() as conn:
                conn.execute(insert(ResultSearchRo), records)
            print(f"{len(records)} registros inseridos com sucesso.")
        except Exception as e:
            print("Erro ao inserir dados:", e)

if __name__ == "__main__":
    filepath = "" # file path to csv
    manager = InjectDataBaseManager(filepath)
    manager.inject_data_base()
