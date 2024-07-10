from data_processing import Data

path_json = 'data_raw/dados_empresaA.json'
path_csv = 'data_raw/dados_empresaB.csv'

# Extract
enterprise_a = Data.read_data(path_json, 'json')
enterprise_b = Data.read_data(path_csv, 'csv')
print('### Empresa A ###')
print(f'Colunas: {enterprise_a.column_names}')
print(f'Quantidade de dados: {enterprise_a.count_lines}')
print('### Empresa B ###')
print(f'Colunas: {enterprise_b.column_names}')
print(f'Quantidade de dados: {enterprise_b.count_lines}')


# Transform
key_mapping = {
    'Nome do Item': 'Nome do Produto',
    'Classificação do Produto': 'Categoria do Produto',
    'Valor em Reais (R$)': 'Preço do Produto (R$)',
    'Quantidade em Estoque': 'Quantidade em Estoque',
    'Nome da Loja': 'Filial',
    'Data da Venda': 'Data da Venda'
}
enterprise_b.rename_columns(key_mapping)
print(f'Colunas renomeadas da empresa B: {enterprise_b.column_names}')

fusion_data = Data.join(enterprise_a, enterprise_b)
print('### Dados da fusão ###')
print(f'Colunas: {fusion_data.column_names}')
print(f'Quantidade de dados: {fusion_data.count_lines}')


# Load
path_fusion_data = 'data_processed/dados_combinados.csv'
fusion_data.save_data(path_fusion_data)
