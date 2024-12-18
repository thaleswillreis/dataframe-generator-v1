import pandas as pd
from faker import Faker
import pyarrow as pa
import pyarrow.parquet as pq
import uuid
import random
import os
from tqdm import tqdm
from typing import List, Dict

# Configurações
fake = Faker('pt_BR')

def gerar_dados_cadastrais(n_linhas: int) -> pd.DataFrame:
    """
    Gera um DataFrame com dados fictícios de cadastros de pessoas.

    Args:
        n_linhas (int): Número de registros a serem gerados.

    Returns:
        pd.DataFrame: DataFrame contendo os dados gerados.

    Raises:
        RuntimeError: Se ocorrer um erro durante a geração dos dados.
    """
    try:
        data: List[Dict[str, str]] = []
        print("Gerando dados de cadastros. Aguarde...")
        for _ in tqdm(range(n_linhas), desc="Gerando cadastros:"):
            genero = random.choice(['M', 'F'])
            nome = fake.name_male() if genero == 'M' else fake.name_female()
            data.append({
                'id': str(uuid.uuid4()),
                'nome': nome,
                'cpf': fake.unique.cpf(),
                'data_nascimento': fake.date_of_birth(minimum_age=18, maximum_age=85).strftime('%Y-%m-%d'),
                'email': fake.email(),
                'telefone': fake.phone_number(),
                'rua': fake.street_name(),
                'numero': str(random.randint(1, 9999)),
                'bairro': fake.neighborhood(),
                'cidade': fake.city(),
                'estado': fake.state(),
                'pais': 'Brasil',
                'cep': fake.postcode(),
                'genero': genero,
                'profissao': fake.job(),
                'data_cadastro': fake.date_between(start_date='-2y', end_date='today').strftime('%Y-%m-%d')
            })
        return pd.DataFrame(data)
    except Exception as e:
        raise RuntimeError(f"Erro ao gerar dados de cadastro: {e}")

def gerar_dados_vendas(cadastros_df: pd.DataFrame, n_pedidos: int) -> pd.DataFrame:
    """
    Gera um DataFrame com dados fictícios de vendas, utilizando os dados de cadastro gerados.

    Args:
        cadastros_df (pd.DataFrame): DataFrame com dados de cadastros de clientes.
        n_pedidos (int): Número de pedidos a serem gerados.

    Returns:
        pd.DataFrame: DataFrame contendo os dados de vendas gerados.

    Raises:
        RuntimeError: Se ocorrer um erro durante a geração dos dados.
    """
    try:
        rua_map = cadastros_df.set_index('cpf')['rua'].to_dict()
        numero_map = cadastros_df.set_index('cpf')['numero'].to_dict()
        bairro_map = cadastros_df.set_index('cpf')['bairro'].to_dict()
        cidade_map = cadastros_df.set_index('cpf')['cidade'].to_dict()
        estado_map = cadastros_df.set_index('cpf')['estado'].to_dict()

        data: List[Dict[str, str]] = []
        print("Gerando dados de vendas. Aguarde...")
        for _ in tqdm(range(n_pedidos), desc="Gerando vendas:"):
            cpf_cliente = random.choice(cadastros_df['cpf'])
            valor_pedido = round(random.uniform(30, 5000), 2)
            valor_frete = round(random.uniform(10, 150), 2)
            valor_desconto = random.choice([0, round(random.uniform(10, 20), 2)])
            cupom = fake.word() if valor_desconto > 0 else None

            status_pedido = random.choices(
                ['Entregue', 'Aguardando envio', 'Aguardando pagamento', 'Cancelado'],
                weights=[80, 10, 7, 3],
                k=1
            )[0]

            forma_pagamento = random.choices(
                ['Cartão de crédito', 'Boleto', 'Cartão de débito', 'Pix'],
                weights=[50, 20, 10, 20],
                k=1
            )[0]

            endereco_entrega = f"{rua_map[cpf_cliente]}, {numero_map[cpf_cliente]}, {bairro_map[cpf_cliente]}"

            data.append({
                'id_pedido': str(uuid.uuid4()),
                'cpf': cpf_cliente,
                'valor_pedido': valor_pedido,
                'valor_frete': valor_frete,
                'valor_desconto': valor_desconto,
                'cupom': cupom,
                'forma_pagamento': forma_pagamento,
                'endereco_entrega': endereco_entrega,
                'cidade_entrega': cidade_map[cpf_cliente],
                'estado_entrega': estado_map[cpf_cliente],
                'pais_entrega': 'Brasil',
                'status_pedido': status_pedido,
                'data_pedido': fake.date_between(start_date='-2y', end_date='today').strftime('%Y-%m-%d')
            })
        return pd.DataFrame(data)
    except KeyError as e:
        raise RuntimeError(f"Erro ao mapear CPF para endereço: {e}")
    except Exception as e:
        raise RuntimeError(f"Erro ao gerar dados de vendas: {e}")

def processar_dados(n_linhas_cadastros: int, n_pedidos: int, caminho_base: str = './data/') -> None:
    """
    Processa a geração de dados fictícios de cadastros e vendas, salvando em arquivos Parquet.

    Args:
        n_linhas_cadastros (int): Número de registros de cadastros a serem gerados.
        n_pedidos (int): Número de registros de vendas a serem gerados.
        caminho_base (str): Caminho base para salvar os arquivos gerados.

    Raises:
        RuntimeError: Se ocorrer um erro durante o processamento dos dados.
    """
    try:
        # Diretórios de saída
        caminho_cadastros = os.path.join(caminho_base, 'cadastros/')
        caminho_pedidos = os.path.join(caminho_base, 'pedidos/')

        # Criar os diretórios, se não existirem
        os.makedirs(caminho_cadastros, exist_ok=True)
        os.makedirs(caminho_pedidos, exist_ok=True)

        print("Iniciando a geração de dados...")
        # Gerar os DataFrames
        cadastros_df = gerar_dados_cadastrais(n_linhas=n_linhas_cadastros)
        pedidos_df = gerar_dados_vendas(cadastros_df, n_pedidos=n_pedidos)

        # Caminhos dos arquivos Parquet
        cadastros_parquet_path = os.path.join(caminho_cadastros, 'cadastros.parquet')
        pedidos_parquet_path = os.path.join(caminho_pedidos, 'pedidos.parquet')

        print("Salvando os dados...")
        # Salvar os DataFrames em arquivos Parquet
        cadastros_df.to_parquet(cadastros_parquet_path, engine='pyarrow', index=False)
        pedidos_df.to_parquet(pedidos_parquet_path, engine='pyarrow', index=False)

        print(f"Dados de cadastros salvos em: {cadastros_parquet_path}")
        print(f"Dados de pedidos salvos em: {pedidos_parquet_path}")
    except Exception as e:
        raise RuntimeError(f"Erro durante o processamento dos dados: {e}")

# Chamada do método principal
if __name__ == "__main__":
    try:
        processar_dados(n_linhas_cadastros=50000, n_pedidos=50000)
    except RuntimeError as e:
        print(e)
