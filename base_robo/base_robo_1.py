import pandas as pd
import json


# Ajustar os paths - fazer mais automático
# Ajustar caminho/nome das saídas
# Colocar a estatística de entrada e saída a cada filtro


def ler_df():
    df = pd.read_excel('./0 Base/ROBO 1003.xlsx', sheet_name='Refin')
    df['CPF'] = df['CPF'].astype(str).str.zfill(11) # Padrão de 11 caracteres
    df["Taxa_Contrato"] = df["Taxa_Contrato"].astype(str).str.replace(",", ".").astype(float)
    df['Status'] = df['Status'].fillna('')
    print('Base carregada!')
    return df

def ler_filtros():
    with open('../filtros/filtros_base_robo.json', 'r') as f:
        filtros = json.load(f)
    print('Filtros carregados')
    return filtros

def filtro_Valor_Parcela(df, filtros): # Aplica filtro para minimo Valor_Parcela
    df = df[df['Valor_Parcela'] >= filtros['ambos']['vl_min_parcela']]
    print('Filtrando valor parcela')
    return df

def filtro_qtd_parcelas(df, filtros): # Aplica filtro para qtd_parcelas
    df = df[df['Parcelas_Contrato'].isin([84, 96])]
    print('Filtrando quantidade parcelas')
    return df

def filtro_parcelas_aberto(df, filtros): # Aplica filtro para minimo parcelas restantes
    df['Parcelas_Restantes'] = df['Parcelas_Contrato'] - df['Parcelas_Aberto']
    df_1 = df[df['Parcelas_Restantes'] >= filtros['ambos']['qtd_min_parcelas_restantes']]
    df_2 = df_1[df_1['Parcelas_Restantes'] <= filtros['ambos']['qtd_max_parcelas_restantes']]
    df = df_2
    print('Filtrando parcelas em aberto')
    return df

def filtro_status(df, filtros): # Aplica filtro para tirar os que tem status
    status = filtros['ambos']['status']
    filtro_status = [status]
    df = df[df['Status'].isin(filtro_status)]
    print('Filtrando status')
    return df
    
def filtro_taxa(df, filtros): # Aplica filtro para as taxas
    df_1 = df[(df['Banco'] == 'C6') & (df['Taxa_Contrato'] >= filtros['c6']['taxa_min'])]
    df_2 = df[(df['Banco'] == 'DIGIO') & (df['Taxa_Contrato'] >= filtros['digio']['taxa_min'])]
    aux = [df_1, df_2]
    df = pd.concat(aux, ignore_index=True)
    print('Filtrando taxa')
    return df

def salvar_planilhas(df):
    df_c6 = df[(df['Banco']) == 'C6']
    df_digio = df[(df['Banco']) == 'DIGIO']
    df_c6.to_excel(f"./1 Pre Enriquecimento/C6 Pre Enriquecimento.xlsx", index=False)
    df_digio.to_excel(f"./1 Pre Enriquecimento/DIGIO Pre Enriquecimento.xlsx", index=False)
    print('Planilhas salvas')

# Execução
df = ler_df()
filtros = ler_filtros()
df = filtro_Valor_Parcela(df, filtros)
df = filtro_qtd_parcelas(df, filtros)
df = filtro_parcelas_aberto(df, filtros)
df = filtro_status(df, filtros)
df = filtro_taxa(df, filtros)
salvar_planilhas(df)
