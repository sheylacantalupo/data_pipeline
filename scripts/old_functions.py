import json
import csv

def leitura_json(path_json):
    with open(path_json, 'r') as file:
        dados = json.load(file)
    raw_lines = dados['payload']['blob']['rawLines']
    raw_lines = raw_lines[1:-1]
    return [json.loads(line[:-1]) for line in raw_lines]

def leitura_csv(path_csv):
    dados_csv = []
    with open(path_csv, 'r') as file:
        spamReader = csv.DictReader(file, delimiter=',')
        for row in spamReader:
            dados_csv.append(row)
    return dados_csv

def leitura_dados(path, tipo_arquivo):
    if tipo_arquivo == 'csv':
        return leitura_csv(path)
    elif tipo_arquivo == 'json':
        return leitura_json(path)

def get_columns(dados):
    nomes_colunas = []
    nomes_colunas.extend(dados[-1].keys())
    return nomes_colunas

def rename_columns(dados,key_mapping):
    new_dados_csv = []

    for old_dict in dados:
        dict_temp = {}
        for old_key, value in old_dict.items():
            dict_temp[key_mapping[old_key]] = value
        new_dados_csv.append(dict_temp)
    return new_dados_csv

def size_data(dados):
    return len(dados)

def join(dadosA, dadosB):
    combined_list = []
    combined_list.extend(dadosA)
    combined_list.extend(dadosB)
    return combined_list

def dados_tabela(dados, colunas):

    dados_combinados_tabela = [colunas]
    for row in dados:
        linha = []
        for coluna in colunas:
            linha.append(row.get(coluna, 'indisponivel'))
        dados_combinados_tabela.append(linha)
    return dados_combinados_tabela

def salvando_dados(path, dados):
    with open(path, 'w') as file:
        writer = csv.writer(file)
        writer.writerows(dados)


# # iniciando a leitura
# dados_json = leitura_dados(path_json, 'json')
# nome_colunas_json = get_columns(dados_json)

# dados_csv = leitura_dados(path_csv, 'csv')
# nome_colunas_csv = get_columns(dados_csv)

# key_mapping = {'Nome do Item': 'Nome do Produto',
#                 'Classificação do Produto': 'Categoria do Produto',
#                 'Valor em Reais (R$)': 'Preço do Produto (R$)',
#                 'Quantidade em Estoque': 'Quantidade em Estoque',
#                 'Nome da Loja': 'Filial',
#                 'Data da Venda': 'Data da Venda'}


# # transformacao dos dados

# dados_csv = rename_columns(dados_csv, key_mapping)
# nome_colunas_csv = get_columns(dados_csv)
# print(nome_colunas_csv)

# dados_fusao = join(dados_json,dados_csv)
# nomes_colunas_fusao = get_columns(dados_fusao)
# print(nomes_colunas_fusao)

# # Salvando dados
# dados_fusao_tabela = dados_tabela(dados_fusao, nomes_colunas_fusao)
# print(dados_fusao_tabela[-1])

# path_dados_combinados = 'data_processed/dados_combinados.csv'
# salvando_dados(path_dados_combinados, dados_fusao_tabela)
