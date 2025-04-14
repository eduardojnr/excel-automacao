import pandas as pd
import json
import os
from database import get_connection

# Toda a etapa do robô até o primeiro enriquecimento do bd

# Ajustar os paths - fazer mais automático
# Ajustar caminho/nome das saídas
# Colocar a estatística de entrada e saída a cada filtro

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
    arquivo_base = [f for f in os.listdir(diretorio) if f.endswith('.xlsx')]
    df_bases = []

    for arquivo in arquivo_base:
        caminho_completo = os.path.join(diretorio, arquivo)
        df = pd.read_excel(f'{caminho_completo}', sheet_name='Refin')
        df_bases.append(df)
    df = pd.concat(df_bases, ignore_index=True)

    df['CPF'] = df['CPF'].astype(str).str.zfill(11) # Padrão de 11 caracteres
    df['Taxa_Contrato'] = df['Taxa_Contrato'].astype(str).str.replace(",", ".").astype(float) # Altera o decimal de , para .
    df['Status'] = df['Status'].fillna('') # Trata nulos
    print(f'{df.shape} -> Base carregada!\n')
    print('--------------------------------------------------------------------------------')
    return df

def separar(df): # Cria um dataframe para cada banco presente
    df['Banco'] = df['Banco']
    bancos = df.drop_duplicates(subset=['Banco'])
    df_dic = {}
    df_list = []
    for banco in bancos['Banco']:
        nome_df = f'df_{str(banco)}' # Nome dinâmico para cada banco
        df_dic[nome_df] = df[df['Banco'] == banco] # Criação do DataFrame filtrado
        df_list.append(df_dic[nome_df]) # Retorna uma lista com o DataFrame de cada banco presente
    return df_list

def percorrer_bases(df_list, filtros):
    for i in range(len(df_list)):
        banco = df_list[i]['Banco'].iloc[1] # Busca a qual banco o DataFrame pertence
        if (banco in filtros['bancos']):
            print(f'\n-> {banco} | Iniciando filtros -> {df_list[i].shape}')
            df_list = filtro_valor_parcela(df_list, filtros, banco, i)
            print(f'-> {banco} | Filtro Valor Parcela -> {df_list[i].shape}')
            df_list = filtro_qtd_parcelas(df_list, filtros, banco, i)
            print(f'-> {banco} | Filtro Quantidade Parcelas -> {df_list[i].shape}')
            df_list = filtro_status(df_list, filtros, banco, i)
            print(f'-> {banco} | Filtro Status -> {df_list[i].shape}')
            df_list = filtro_parcelas_pagas(df_list, filtros, banco, i)
            print(f'-> {banco} | Filtro Parcelas Pagas -> {df_list[i].shape}')
            df_list = filtro_taxa(df_list, filtros, banco, i)
            print(f'-> {banco} | Filtro Taxa -> {df_list[i].shape}')
            df_list = calcular_troco(df_list, filtros, banco, i)
            print(f'-> {banco} | Filtro Troco -> {df_list[i].shape}\n')
            salvar_planilhas(df_list, data, banco, i)
    
    print('\nFiltros iniciais realizados - Planilhas salvas')
    return df_list

def filtro_valor_parcela(df_list, filtros, banco, i): # Aplica filtro para minimo Valor_Parcela
    df_list[i] = df_list[i][df_list[i]['Valor_Parcela'] >= filtros['bancos'][banco]['vl_min_parcela']]
    return df_list

def filtro_qtd_parcelas(df_list, filtros, banco, i): # Aplica filtro para qtd_parcelas
    qtd_parcelas = [filtros['bancos'][banco]['qtd_min_parcelas'], filtros['bancos'][banco]['qtd_max_parcelas']]
    df_list[i] = df_list[i][df_list[i]['Parcelas_Contrato'].isin(qtd_parcelas)]
    return df_list

def filtro_status(df_list, filtros, banco, i): # Aplica filtro para tirar os que tem status
    df_list[i] = df_list[i][df_list[i]['Status'] == filtros['bancos'][banco]['status']]
    return df_list

def filtro_parcelas_pagas(df_list, filtros, banco, i): # Aplica filtro para minimo parcelas restantes
    df_list[i]['Parcelas_Pagas'] = df_list[i]['Parcelas_Contrato'] - df_list[i]['Parcelas_Aberto']
    df_1 = df_list[i][df_list[i]['Parcelas_Pagas'] >= filtros['bancos'][banco]['qtd_min_parcelas_pagas']]
    df_2 = df_1[df_1['Parcelas_Pagas'] <= filtros['bancos'][banco]['qtd_max_parcelas_pagas']]
    df_list[i] = df_2
    return df_list
    
def filtro_taxa(df_list, filtros, banco, i): # Aplica filtro para as taxas
    df_list[i] = df_list[i][(df_list[i]['Taxa_Contrato'] >= filtros['bancos'][banco]['taxa_min'])]
    df_list[i] = df_list[i][(df_list[i]['Taxa_Contrato'] <= filtros['bancos'][banco]['taxa_max'])]
    return df_list

def calcular_troco(df_list, filtros, banco, i):
    df_list[i]['VALOR_RESTANTE'] = df_list[i]['Parcelas_Aberto'] * df_list[i]['Valor_Parcela']
    df_list[i]['TROCO_BRUTO'] = df_list[i]['Valor_Parcela'] / filtros['bancos'][banco]['coeficiente_troco']
    df_list[i]['TROCO_LIQUIDO'] = df_list[i]['TROCO_BRUTO'] - df_list[i]['Saldo_Refin']
    return df_list

def filtrar_troco(df_list, filtros, banco, i):
    df_list[i] = df_list[i][(df_list[i]['TROCO_LIQUIDO'] >= filtros['bancos'][banco]['troco_min'])]

def salvar_planilhas(df_list, data, banco, i):      
    df_list[i].to_excel(f"./1 Pre Enriquecimento/{banco} {data} Pre Enriquecimento.xlsx", index=False)
    return df_list

def enriquecimento(dias): # Consultar o bd para o primeiro enriquecimento
    diretorio = './1 Pre Enriquecimento'   
    arquivos_pre_enriquecidos = [f for f in os.listdir(diretorio) if f.endswith('.xlsx')]
    print(f'\n')
    print(f'--' * 40)
    print('\n\nEtapa de enriquecimento\n')
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

    print('--' * 40)
    print('\n')

# Provisório para fazer as etapas seguintes
def nova_vida():
    print('')

# Ler base bruta
def ler_base_bruta():
    diretorio = './0 Base/Base bruta'  
    arquivo_base_bruta = [f for f in os.listdir(diretorio) if f.endswith('.csv')]
    base_bruta = []
    print(f'\n')
    print(f'--' * 40)
    for arquivo in arquivo_base_bruta:
        caminho_completo = os.path.join(diretorio, arquivo)
        df_base_bruta = pd.read_csv(caminho_completo, sep=';', encoding='windows-1252')
        df_base_bruta = df_base_bruta[['cpf', 'total_contratos', 'vl_beneficio', 'especie']]
        df_base_bruta = df_base_bruta.rename(columns={'cpf': 'CPF'})
        base_bruta.append(df_base_bruta)

    df_base_bruta = pd.concat(base_bruta, ignore_index=True)
    return df_base_bruta

def obter_base_bruta(df, df_base_bruta): # Traz a qtd de contratos, vl do beneficio e especie
    df = df.merge(df_base_bruta[['CPF', 'total_contratos', 'vl_beneficio', 'especie']], on='CPF', how='left')
    df = df.rename(columns={'total_contratos': 'CONTRATOS', 'vl_beneficio': 'VL-BENEFICIO', 'especie': 'ESPECIE'})
    return df

def filtro_especie(df, banco):
    df = df[~df['ESPECIE'].isin(filtros['bancos'][banco]['especie_invalida'])]
    return df

def filtro_idade(df, banco):  
    df = df[
        (df['IDADE'] >= filtros['bancos'][banco]['idade_min']) &
        (df['IDADE'] <= filtros['bancos'][banco]['idade_max']) &
        (df['IDADE_CALCULADA'] >= filtros['bancos'][banco]['idade_min']) &
        (df['IDADE_CALCULADA'] <= filtros['bancos'][banco]['idade_max'])]
    return df

def separar_unidades(df):
    df
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

def formato_discadora(df): # Exclui colunas, reordena e trata nulos
    colunas = list(df.columns)
    colunas.remove('CONTRATOS')
    colunas.remove('VL-BENEFICIO')
    colunas.insert(2, 'CONTRATOS')
    colunas.insert(3, 'VL-BENEFICIO')
    df = df[colunas]

    df = df.drop(columns=['DATA_INSERCAO', 'NASC', 'RENDA', 'ESPECIE', 'IDADE_CALCULADA'])
    df['TEL1'] = df['DDDCEL1'] + df['CEL1']
    df['TEL2'] = df['DDDCEL2'] + df['CEL2']
    df['MEMO1'] = None
    df['MEMO2'] = df['CPF']
    for i in range(1,3): # Tratando nulos e telefones
        df[f'DDDCEL{i}'] = df[f'DDDCEL{i}'].astype(str).replace('nan', '').str.rstrip('.0')
        df[f'CEL{i}'] = df[f'CEL{i}'].astype(str).replace('nan', '').str.rstrip('.0')
        df[f'TEL{i}'] = df[f'TEL{i}'].astype(str).replace('nan', '').str.rstrip('.0')
    return df

def remover_duplicidades(df_aju, df_est, cpfs_usados): # Garante que um CPF só existirá em no máximo um banco/unidade
    df_aju = df_aju[~df_aju["CPF"].isin(cpfs_usados)]
    df_est = df_est[~df_est["CPF"].isin(cpfs_usados)]
    cpfs_usados.update(df_aju['CPF'].tolist())  # Usa .update() para adicionar valores únicos ao conjunto
    cpfs_usados.update(df_est['CPF'].tolist())

    return df_aju, df_est

def ler_enriquecimento():
    diretorio = './3 Enriquecimento Final'
    cpfs_usados = set()
    arquivos_enriquecidos = [f for f in os.listdir(diretorio) if f.endswith('.csv')] # Lê os arquivos enriquecidos
    for arquivo in arquivos_enriquecidos:
        caminho_completo = os.path.join(diretorio, arquivo)
        nomenclatura = arquivo[:-16]
        banco = arquivo[:-21]
        try:
            if not (caminho_completo.endswith('Não Encontrados.csv')):
                df = pd.read_csv(caminho_completo, sep=';', encoding='windows-1252')
                df_base_bruta = ler_base_bruta()
                df = obter_base_bruta(df, df_base_bruta)
                print(f'-> {banco} | Iniciando filtros -> {df.shape}')
                df = filtro_especie(df, banco)
                print(f'-> {banco} | Filtro Espécie -> {df.shape}')
                df = filtro_idade(df, banco)
                print(f'-> {banco} | Filtro Idade -> {df.shape}')
                df = tratar_nulos_telefones(df)
                df = formato_discadora(df)
                df_aju, df_est = separar_unidades(df)
                df_aju, df_est = remover_duplicidades(df_aju, df_est, cpfs_usados)
                df_aju = df_aju.copy()
                df_est = df_est.copy()
                print(f'-> {banco} | ARACAJU -> {df_aju.shape}')
                print(f'-> {banco} | ESTANCIA -> {df_est.shape}')
                df_aju['MEMO1'] = f'{nomenclatura} ROBO AJU'
                df_est['MEMO1'] = f'{nomenclatura} ROBO EST'
                df_aju = df_aju.drop_duplicates(subset=['CPF'])
                df_est = df_est.drop_duplicates(subset=['CPF'])
                df_aju.to_csv(f'./4 Arquivo Discadora/{nomenclatura} ROBO AJU.csv', index=False, sep=';')
                df_est.to_csv(f'./4 Arquivo Discadora/{nomenclatura} ROBO EST.csv', index=False, sep=';')
                print('\nArquivos gerados!')

        except Exception as e:
            print(f"Erro ao carregar {arquivo}: {repr(e)}")

# Execução
data = str(input("Insira a data (ddmm): "))
dias = int(input("Insira o número de dias para o enriquecimento: "))
limpar_diretorios()
filtros = ler_filtros()
df = ler_df()
df_list = separar(df)
df_list = percorrer_bases(df_list, filtros)
enriquecimento(dias)

entrada = int(input('Deseja continuar? Tecle 1\n'))
if (entrada == 1):
    ler_enriquecimento()
