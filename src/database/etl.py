import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
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
        df = pd.read_csv(self.file, sep=";", dtype=str, encoding="latin-1")
        df = df.where(pd.notnull(df), None)  # Converte NaN para None

        df["DATA_NASCIMENTO"] = pd.to_datetime(
            df["DATA_NASCIMENTO"], errors="coerce", dayfirst=True
        )
        df["DATA_NASCIMENTO"] = df["DATA_NASCIMENTO"].apply(lambda x: x.date() if pd.notnull(x) else None)

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
        if "idade" in df.columns:
            df["idade"] = df["idade"].astype(float)

        records = [ResultSearchRo(**row) for row in df.to_dict(orient="records")]

        session = Session()
        try:
            session.bulk_save_objects(records)
            session.commit()
            print(f"{len(records)} registros inseridos com sucesso.")
        except Exception as e:
            session.rollback()
            print("Erro ao inserir dados:", e)
        finally:
            session.close()


if __name__ == "__main__":
    filepath = "/Users/hedrispereira/temp/platform-algo-bot/data/data.csv"
    manager = InjectDataBaseManager(filepath)
    manager.inject_data_base()
