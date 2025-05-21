# manage.py

# TODO - manipular o resultado da buscar e salvar dentro do banco de dados...
# TODO - criar o request manipulando atráves do fastapi, um get para buscar os dados, post enviando o arquivo...
# TODO - publicar isso no servidor...

import uvicorn
from src import app


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
