import pandas as pd
from sqlalchemy import create_engine

# Configuração de conexão com banco de dados
db_connection_str = "sqlite:///agencia_turismo.db"
db_connection = create_engine(db_connection_str)

# Etapa de Extração
def extract_data():
    # Substitua isso pelo seu método de extração de dados, por exemplo, consultando um banco de dados de clientes, reservas, etc.
    clientes_data = pd.read_csv("clientes.csv")
    reservas_data = pd.read_csv("reservas.csv")
    destinos_data = pd.read_csv("destinos.csv")
    return clientes_data, reservas_data, destinos_data

# Etapa de Transformação
def transform_data(clientes_data, reservas_data, destinos_data):
    # Substitua isso pela lógica de transformação que você deseja aplicar aos dados,
    # como mesclar informações, calcular preços, filtrar reservas, etc.
    # Vamos apenas criar um exemplo simples de mesclagem de dados aqui.
    reservas_clientes = pd.merge(reservas_data, clientes_data, on='cliente_id')
    reservas_destinos = pd.merge(reservas_clientes, destinos_data, on='destino_id')
    return reservas_destinos

# Etapa de Carga
def load_data(data):
    # Substitua isso pelo método de carregamento de dados em seu destino, como um banco de dados.
    data.to_sql('reservas_destinos', db_connection, if_exists='replace', index=False)

if __name__ == "__main__":
    # Etapa de Extração
    clientes, reservas, destinos = extract_data()

    # Etapa de Transformação
    reservas_destinos = transform_data(clientes, reservas, destinos)

    # Etapa de Carga
    load_data(reservas_destinos)

    print("Pipeline ETL para agência de turismo concluído com sucesso!")
