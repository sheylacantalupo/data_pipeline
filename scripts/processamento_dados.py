import json
import csv

class Dados:
    def __init__(self, path, tipo_dados):
        self.path = path
        self.tipo_dados = tipo_dados
        self.dados = self.leitura_dados()
        self.nomes_colunas = self.get_columns()
        self.qtd_linhas = self.size_data()

    # métodos da classe
    def leitura_json(self):

        with open(self.path, 'r') as file:
            dados = json.load(file)
        raw_lines = dados['payload']['blob']['rawLines']
        raw_lines = raw_lines[1:-1]
        return [json.loads(line[:-1]) for line in raw_lines]

    def leitura_csv(self):

        dados_csv = []
        with open(self.path, 'r') as file:
            spamReader = csv.DictReader(file, delimiter=',')
            for row in spamReader:
                dados_csv.append(row)
        return dados_csv

    def leitura_dados(self):
        dados = []
        if self.tipo_dados == 'csv':
            dados = self.leitura_csv()
        elif self.tipo_dados == 'json':
            dados = self.leitura_json()
        elif self.tipo_dados == 'list':
            dados = self.path
            self.path = 'lista em memoria'
        
        return dados    
        
    def get_columns(self):
        return list(self.dados[-1].keys())
    
    def rename_columns(self,key_mapping):
        new_dados = []

        for old_dict in self.dados:
            dict_temp = {}
            for old_key, value in old_dict.items():
                dict_temp[key_mapping[old_key]] = value
            new_dados.append(dict_temp)
        self.dados = new_dados
        self.nomes_colunas = self.get_columns()

    def size_data(self):
        return len(self.dados)
    
    def join(dadosA, dadosB):
        combined_list = []
        combined_list.extend(dadosA.dados)
        combined_list.extend(dadosB.dados)
        return Dados(combined_list, 'list')

    def dados_tabela(self):
        dados_combinados_tabela = [self.nomes_colunas]
        for row in self.dados:
            linha = []
            for coluna in self.nomes_colunas:
                linha.append(row.get(coluna, 'indisponivel'))
            dados_combinados_tabela.append(linha)
        return dados_combinados_tabela
    
    def salvando_dados(self, path):

        dados_combinados_tabela = self.dados_tabela()

        with open(path, 'w') as file:
            writer = csv.writer(file)
            writer.writerows(dados_combinados_tabela)