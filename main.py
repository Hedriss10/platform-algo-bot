import time
from src.api import ExtractTransformLoad
from src.log.logger import setup_logger

logger = setup_logger()


def main():
    a = ExtractTransformLoad()
    a.load_token()  # token inicial
    cpfs = a.cpfs_database()

    i = 0
    while i < len(cpfs):  # loop contínuo
        cpf = cpfs[i]
        try:
            time.sleep(5)
            data = a.get_request(cpf)

            # Só avança se processou o CPF sem erro
            if data is not None:
                i += 1

        except ValueError as e:
            # Caso "Token not loaded"
            logger.warning(f"Token ausente. Tentando recarregar: {e}")
            a.load_token()

        except Exception as e:
            logger.error(f"Erro inesperado no CPF {cpf}: {e}")
            i += 1  # pula esse CPF problemático e segue em frente


if __name__ == "__main__":
    main()