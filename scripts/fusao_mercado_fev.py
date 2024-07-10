import json
import csv
from typing import Literal


def read_json(path_json: str) -> list[dict]:
    with open(path_json, 'r') as file:
        return json.load(file)


def read_csv(path_csv: str) -> list[dict]:
    data_csv = []
    with open(path_csv, 'r') as file:
        spamreader = csv.DictReader(file, delimiter=',')
        for row in spamreader:
            data_csv.append(row)

        return data_csv


def read_data(path: str, tipo_arquivo: Literal['csv', 'json']) -> list[dict]:
    data = []
    if tipo_arquivo == 'csv':
        data = read_csv(path)

    elif tipo_arquivo == 'json':
        data = read_json(path)

    return data


def get_columns(data: list[dict]) -> list[str]:
    return list(data[-1].keys())


def rename_columns(data: list[dict], key_mapping: dict) -> list[dict]:
    return [
        {key_mapping[old_key]: value for old_key, value in old_dict.items()}
        for old_dict in data
    ]


def join(data_a: list[dict], data_b: list[dict]) -> list[dict]:
    combined_list = []
    combined_list.extend(data_a)
    combined_list.extend(data_b)
    return combined_list


def transform_data_table(data: list[dict], column_names: list[str]) -> list[str]:
    combined_data_table = [column_names]
    combined_data_table.extend([
        [row.get(column, 'Indisponivel') for column in column_names] for row in data
    ])
    return combined_data_table


def save_data(data: list[str], path: str) -> None:
    with open(path, 'w') as file:
        writer = csv.writer(file)
        writer.writerows(data)
    print(f'Save data successful in "{path}"')


# Start reading data
path_json = 'data_raw/dados_empresaA.json'
path_csv = 'data_raw/dados_empresaB.csv'

data_json = read_data(path_json, 'json')
data_csv = read_data(path_csv, 'csv')

# column_names_json = get_columns(data_json)
# column_names_csv = get_columns(data_csv)

# Transform data
key_mapping = {
    'Nome do Item': 'Nome do Produto',
    'Classificação do Produto': 'Categoria do Produto',
    'Valor em Reais (R$)': 'Preço do Produto (R$)',
    'Quantidade em Estoque': 'Quantidade em Estoque',
    'Nome da Loja': 'Filial',
    'Data da Venda': 'Data da Venda'
}

data_csv = rename_columns(data_csv, key_mapping)

fusion_data = join(data_json, data_csv)
# fusion_data = join(data_csv, data_json)
fusion_data_column_names = get_columns(fusion_data)

fusion_data_table = transform_data_table(fusion_data, fusion_data_column_names)

# Saving data
path_fusion_data = 'data_processed/dados_combinados.csv'
save_data(fusion_data_table, path_fusion_data)
