import os
import pandas as pd

def limpar_diretorios(): # Garante que terá apenas o resultado em <base_nova_filtrada>
    diretorio = ['base_nova_filtrada']
    for i in range(len(diretorio)):
        arquivo_base = [f for f in os.listdir(str(diretorio[i])) if f.endswith('.csv') or f.endswith('.xlsx')]
        for arquivo in arquivo_base:
            caminho_completo = os.path.join(diretorio[i], arquivo)
            os.remove(caminho_completo)

# Função para ler os arquivos da discadora
def ler_discadora():
    diretorio = './discadora_arquivos'
    
    arquivos_discadora = [f for f in os.listdir(diretorio) if f.endswith('.csv')]
    clientes_discadora = []
    
    for arquivo in arquivos_discadora:
        caminho_completo = os.path.join(diretorio, arquivo)
        try:
            df = pd.read_csv(caminho_completo, sep=';', encoding="windows-1252")
            df['CPF'] = df['CPF'].astype(str).str.zfill(11)  # Padrão de 11 caracteres
            clientes_discadora.append(df)
            print(f"Discadora {arquivo} carregado com sucesso!")
        except Exception as e:
            print(f"Erro ao carregar {arquivo}: {e}")
    
    clientes_discadora = pd.concat(clientes_discadora, ignore_index=True)
    #clientes_discadora.to_excel('./discadora_arquivos/Compilado arquivos.xlsx', index=False)
    print('\nDiscadora concatenada!')
    
    return clientes_discadora

def ler_clientes_casa(): # Lê os clientes da casa
    diretorio = './discadora_arquivos'
    clientes_casa = []

    try:
        df = pd.read_excel('./clientes_casa/CPF.xlsx', sheet_name='Plan1')
        df['CPF'] = df['CPF'].astype(str).str.zfill(11)  # Padrão de 11 caracteres
        clientes_casa.append(df)
        print(f"Clientes da casa carregado com sucesso!")
    except Exception as e:
        print(f"Erro ao carregar clientes da casa: {e}")
    
    clientes_casa = pd.concat(clientes_casa, ignore_index=True)
    
    return clientes_casa

# Função para ler e filtrar a base nova
def filtrar_base_nova(clientes_discadora, clientes_casa):
    diretorio_base_nova = './base_nova'

    arquivos_base_nova = [b for b in os.listdir(diretorio_base_nova) if b.endswith('.csv')]

    for arquivo in arquivos_base_nova:
        caminho_completo = os.path.join(diretorio_base_nova, arquivo)
        try:
            df = pd.read_csv(caminho_completo, sep=';', encoding="windows-1252")
            df['CPF'] = df['CPF'].astype(str).str.zfill(11)  # Padrão de 11 caracteres
            df['MEMO2'] = df['MEMO2'].astype(str).str.zfill(11)  # Padrão de 11 caracteres
            
            for i in range(1,3): # Tratando nulos e telefones
                df[f'DDDCEL{i}'] = df[f'DDDCEL{i}'].astype(str).replace('nan', '').str.rstrip('.0')
                df[f'CEL{i}'] = df[f'CEL{i}'].astype(str).replace('nan', '').str.rstrip('.0')
                df[f'TEL{i}'] = df[f'TEL{i}'].astype(str).replace('nan', '').str.rstrip('.0')
                      
            qtd_cpfs_origem = df.shape[0]

            # Removendo CPFs que já estão na discadora ou que são da casa
            df = df[~df['CPF'].isin(clientes_discadora['CPF'])]  
            df = df[~df['CPF'].isin(clientes_casa['CPF'])]  
            qtd_cpfs_fim = df.shape[0]
            print(f"\n{arquivo}\nQuantidade de CPFs na entrada: {qtd_cpfs_origem} \nQuantidade de CPFs na saída: {qtd_cpfs_fim}\n\n")
            df.to_csv(f"./base_nova_filtrada/{arquivo}", index=False, sep=';')

        except Exception as e:
            print(f"Erro ao carregar {arquivo}: {e}")


# Executando as funções
limpar_diretorios()
clientes_discadora = ler_discadora()
clientes_casa = ler_clientes_casa()
filtrar_base_nova(clientes_discadora, clientes_casa)
