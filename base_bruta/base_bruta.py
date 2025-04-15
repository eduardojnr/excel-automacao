import os
import json
import pandas as pd
from database import get_connection

def conectar_bd():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    return conn, cursor

def limpar_diretorios():
    diretorio = ['1 Pre Enriquecimento', '2 Enriquecimento Inicial', '3 Enriquecimento Final','4 Arquivo Discadora']
    for i in range(len(diretorio)):
        arquivo_base = [f for f in os.listdir(str(diretorio[i])) if f.endswith('.csv') or f.endswith('.xlsx')]
        for arquivo in arquivo_base:
            caminho_completo = os.path.join(diretorio[i], arquivo)
            os.remove(caminho_completo)

def ler_filtros():
    with open('../filtros/filtros_base_robo copy.json', 'r') as f:
        filtros = json.load(f)
    print('\nFiltros carregados')
    return filtros

def ler_df():
    diretorio = './0 Base'
    arquivo_base = [f for f in os.listdir(diretorio) if f.endswith('.csv')]
    df_bases = []

    for arquivo in arquivo_base:
        caminho_completo = os.path.join(diretorio, arquivo)
        df = pd.read_csv(f'{caminho_completo}', sep=';')
        df_bases.append(df)
    df = pd.concat(df_bases, ignore_index=True)
    df = df.drop_duplicates(subset=['cpf'])
    df['cpf'] = df['cpf'].astype(str).str.zfill(11) # Padrão de 11 caracteres
    df = df.rename(columns={'cpf': 'CPF'})
    df['TaxaCalculada'] = df['TaxaCalculada'].astype(str).str.replace(",", ".").astype(float)

    return df

def obter_banco(df, filtros):
    banco_map = {info['cod_banco']: nome for nome, info in filtros['bancos'].items()}
    df['nome_banco'] = df['cod_banco'].map(banco_map)

    return df
    
def separar(df): # Separa um dataframe para cada banco presente
    df['nome_banco'] = df['nome_banco']
    bancos = df.drop_duplicates(subset=['nome_banco'])
    df_dic = {}
    df_list = []

    for banco in bancos['nome_banco']:
        nome_df = f'df_{str(banco)}' # Nome dinâmico para cada banco
        df_dic[nome_df] = df[df['nome_banco'] == banco] # Criação do DataFrame filtrado
        df_list.append(df_dic[nome_df])
    
    return df_list

def percorrer_bases(df_list, filtros):
    lista = []
    for i in range(len(df_list)):
        banco = df_list[i]['nome_banco'].iloc[1] # Busca a qual banco o DataFrame pertence

        if (banco in filtros['bancos']):
            print(f'-> {banco} | Iniciando filtros -> {df_list[i].shape}')
            df_list = filtro_especie(df_list, filtros, banco, i)
            print(f'-> {banco} | Filtro Espécie -> {df_list[i].shape}')
            df_list = filtro_valor_parcela(df_list, filtros, banco, i)
            print(f'-> {banco} | Filtro Valor Parcela -> {df_list[i].shape}')
            df_list = filtro_qtd_parcelas(df_list, filtros, banco, i)
            print(f'-> {banco} | Filtro Quantidade Parcelas -> {df_list[i].shape}')
            df_list = filtro_parcelas_pagas(df_list, filtros, banco, i)
            print(f'-> {banco} | Filtro Parcelas Pagas -> {df_list[i].shape}')
            df_list = filtro_taxa(df_list, filtros, banco, i)
            print(f'-> {banco} | Filtro Taxa -> {df_list[i].shape}')
            df_list = salvar_planilhas(df_list, data, banco, i)
            lista.append(df_list[i])

    juncao_df = pd.concat(lista, ignore_index=True)
    enriquecimento(dias)

    entrada = int(input('Continuar? Digite 1\n'))

    if (entrada == 1):
        diretorio = './3 Enriquecimento Final' # Trocar para o 3 Enriquecimento   
        arquivos_enriquecidos = [f for f in os.listdir(diretorio) if f.endswith('.csv')]
        for arquivo in arquivos_enriquecidos:
            caminho_completo = os.path.join(diretorio, arquivo)
            nomenclatura = arquivo[:-16]
            banco = arquivo[:-21]
            try:
                if not (caminho_completo.endswith('Não Encontrados.csv')):
                    if (banco in filtros['bancos']):
                        df = pd.read_csv(caminho_completo, sep=';', encoding='windows-1252')
                        df['CPF'] = df['CPF'].astype(str).str.zfill(11) # Padrão de 11 caracteres                
                        df = obter_base_bruta(df, juncao_df)
                        print(f'-> {banco} | Iniciando Filtros -> {df.shape}')
                        df = filtro_idade(df, filtros)
                        print(f'-> {banco} | Filtro Idade -> {df.shape}')
                        df = tratar_nulos_telefones(df)
                        df = formato_discadora(df)
                        df_aju, df_est = separar_unidades(df)
                        df_aju = df_aju.copy()
                        df_est = df_est.copy()
                        print(f'-> {banco} | ARACAJU -> {df_aju.shape}')
                        print(f'-> {banco} | ESTANCIA -> {df_est.shape}')
                        df_aju['MEMO1'] = f'{nomenclatura} CL AJU'
                        df_est['MEMO1'] = f'{nomenclatura} CL EST'
                        df_aju = df_aju.drop_duplicates(subset=['CPF'])
                        df_est = df_est.drop_duplicates(subset=['CPF'])
                        df_aju.to_csv(f'./4 Arquivo Discadora/{nomenclatura} CL AJU.csv', index=False, sep=';')
                        df_est.to_csv(f'./4 Arquivo Discadora/{nomenclatura} CL EST.csv', index=False, sep=';')
                        print('\nArquivos gerados!')            

            except Exception as e:
                print(f"Erro ao carregar {arquivo}: {repr(e)}")

    return df_list

def filtro_especie(df_list, filtros, banco, i): # Aplica filtro para espécies inválidas
    especie_invalida = filtros['bancos'][banco]['especie_invalida']
    df_list[i] = df_list[i][~df_list[i]['especie'].isin(especie_invalida)]
    return df_list

def filtro_valor_parcela(df_list, filtros, banco, i): # Aplica filtro para minimo Valor_Parcela
    df_list[i] = df_list[i][df_list[i]['vl_parcela'] >= filtros['bancos'][banco]['vl_min_parcela']]
    return df_list

def filtro_qtd_parcelas(df_list, filtros, banco, i): # Aplica filtro para qtd_parcelas
    qtd_parcelas = [filtros['bancos'][banco]['qtd_min_parcelas'], filtros['bancos'][banco]['qtd_max_parcelas']]
    df_list[i] = df_list[i][df_list[i]['prazo'].isin(qtd_parcelas)]
    return df_list

def filtro_parcelas_pagas(df_list, filtros, banco, i): # Aplica filtro para minimo parcelas restantes
    df_1 = df_list[i][df_list[i]['parcelas_pagas'] >= filtros['bancos'][banco]['qtd_min_parcelas_pagas']]
    df_2 = df_1[df_1['parcelas_pagas'] <= filtros['bancos'][banco]['qtd_max_parcelas_pagas']]
    df_list[i] = df_2
    return df_list

def filtro_taxa(df_list, filtros, banco, i): # Aplica filtro para as taxas
    df_list[i] = df_list[i][(df_list[i]['TaxaCalculada'] >= filtros['bancos'][banco]['taxa_min'])]
    df_list[i] = df_list[i][(df_list[i]['TaxaCalculada'] <= filtros['bancos'][banco]['taxa_max'])]
    return df_list

def salvar_planilhas(df_list, data, banco, i):      
    df_list[i].to_excel(f"./1 Pre Enriquecimento/{banco} {data} Pre Enriquecimento.xlsx", index=False)
    return df_list

def enriquecimento(dias): # Consultar o bd para o primeiro enriquecimento
    diretorio = './1 Pre Enriquecimento'   
    arquivos_pre_enriquecidos = [f for f in os.listdir(diretorio) if f.endswith('.xlsx')]
    print(f'\n')
    print(f'--' * 40)
    print('\nEtapa de enriquecimento\n')
    for arquivo in arquivos_pre_enriquecidos:
        caminho_completo = os.path.join(diretorio, arquivo)
        encontrados = []
        n_encontrados = []
        try:
            conn, cursor = conectar_bd()
            df = pd.read_excel(caminho_completo, sheet_name='Sheet1')
            cpfs = list(set(df["CPF"].dropna().astype(str))) # Remove duplicatas
            print(f"\nArquivo {arquivo} carregado com sucesso!")

            sql=f"""
            SELECT e.*, TIMESTAMPDIFF(YEAR, e.NASC, CURDATE()) AS IDADE_CALCULADA 
            FROM enriquecimento e 
            WHERE e.CPF IN ({','.join(['%s'] * len(cpfs))}) 
                AND e.DATA_INSERCAO >= CURDATE() - INTERVAL %s DAY
            ORDER BY e.DATA_INSERCAO DESC
            """

            cursor.execute(sql, cpfs + [dias])
            rows = cursor.fetchall()

            # Criando um dicionário para armazenar os CPFs encontrados
            resultados_dict = {}
            for row in rows:
                cpf = row["CPF"]
                if cpf not in resultados_dict:
                    resultados_dict[cpf] = row  

            # Criando listas separadas
            encontrados = list(resultados_dict.values())
            nao_encontrados = [{"CPF": cpf} for cpf in cpfs if cpf not in resultados_dict]

            cursor.close()
            conn.close()

            # Criando DataFrames
            df_encontrados = pd.DataFrame(encontrados)
            df_nao_encontrados = pd.DataFrame(nao_encontrados)

            # Criando um arquivos CSV para os ENCONTRADOS
            df_encontrados.to_csv(f'./2 Enriquecimento Inicial/{arquivo[:-24]} Encontrados.csv', index=False, sep=';')
            print(f'Planilha de ENCONTRADOS: {df_encontrados.shape} -> {arquivo[:-24]} Encontrados.csv')

            # Criando um arquivos CSV para os NÃO ENCONTRADOS
            df_nao_encontrados.to_csv(f'./2 Enriquecimento Inicial/{arquivo[:-24]} Não Encontrados.csv', index=False, sep=';')
            print(f'Planilha de NÃO ENCONTRADOS: {df_nao_encontrados.shape} -> {arquivo[:-24]} Não Encontrados.csv\n')
            
        except Exception as e:
            print(f"Erro ao carregar {arquivo}: {e}")

def obter_base_bruta(df, juncao_df): # Traz a qtd de contratos, vl do beneficio e especie
    df = df.merge(juncao_df[['CPF', 'total_contratos', 'vl_beneficio', 'nome_banco', 'especie']], on='CPF', how='left')
    df = df.rename(columns={'total_contratos': 'CONTRATOS', 'vl_beneficio': 'VL-BENEFICIO', 'especie': 'ESPECIE'})
    return df

def filtro_idade(df, filtros):  
    banco = df['nome_banco'].iloc[1]
    if (banco in filtros['bancos']):
        df = df[
            (df['IDADE'] >= filtros['bancos'][banco]['idade_min']) &
            (df['IDADE'] <= filtros['bancos'][banco]['idade_max']) &
            (df['IDADE_CALCULADA'] >= filtros['bancos'][banco]['idade_min']) &
            (df['IDADE_CALCULADA'] <= filtros['bancos'][banco]['idade_max'])]
    return df

def separar_unidades(df):
    df_aju = [] 
    df_est = [] 
    df_aju = df[(df['CONTRATOS'] == 1) | ((df['CONTRATOS'] > 1) & (df['VL-BENEFICIO'] >= 3000))]
    df_est = df[(df['CONTRATOS'] > 1) & (df['VL-BENEFICIO'] < 3000)]
    return df_aju, df_est

def tratar_nulos_telefones(df):
    for i in range(1, 3):
        df[f'DDDCEL{i}'] = df[f'DDDCEL{i}'].replace(["na", "nan", "NA", "NaN"], "", regex=True)
        df[f'DDDCEL{i}'] = df[f'DDDCEL{i}'].fillna("")
        df[f'CEL{i}'] = df[f'CEL{i}'].replace(["na", "nan", "NA", "NaN"], "", regex=True)
        df[f'CEL{i}'] = df[f'CEL{i}'].fillna("")
        #Convertendo para string
        df[f'DDDCEL{i}'] = df[f'DDDCEL{i}'].astype(str)
        df[f'DDDCEL{i}'] = df[f'DDDCEL{i}'].astype(str)
        df[f'CEL{i}'] = df[f'CEL{i}'].astype(str)
        df[f'CEL{i}'] = df[f'CEL{i}'].astype(str)
    return df

def formato_discadora(df):
    colunas = list(df.columns)
    colunas.remove('CONTRATOS')
    colunas.remove('VL-BENEFICIO')
    colunas.insert(2, 'CONTRATOS')
    colunas.insert(3, 'VL-BENEFICIO')
    df = df[colunas]

    df = df.drop(columns=['DATA_INSERCAO', 'NASC', 'RENDA', 'ESPECIE', 'IDADE_CALCULADA', 'nome_banco'])
    df['TEL1'] = df['DDDCEL1'] + df['CEL1']
    df['TEL2'] = df['DDDCEL2'] + df['CEL2']
    df['MEMO1'] = None
    df['MEMO2'] = df['CPF']
    for i in range(1,3): # Tratando nulos e telefones
        df[f'DDDCEL{i}'] = df[f'DDDCEL{i}'].astype(str).replace('nan', '').str.rstrip('.0')
        df[f'CEL{i}'] = df[f'CEL{i}'].astype(str).replace('nan', '').str.rstrip('.0')
        df[f'TEL{i}'] = df[f'TEL{i}'].astype(str).replace('nan', '').str.rstrip('.0')
    return df

limpar_diretorios()
data = str(input('Insira a data (ddmm): '))
dias = int(input("Insira o número de dias para o enriquecimento: "))
filtros = ler_filtros()
df = ler_df()
df = obter_banco(df, filtros)
df_list = separar(df)
percorrer_bases(df_list, filtros)
