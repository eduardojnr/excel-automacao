Regras para o funcionamento:

1° - <./base_nova> É a base que se deseja filtrar antes de enviar a discadora, deve conter o formato .csv com cabeçalho e separador ';', além de seguir o modelo de colunas (CPF deve estar em UPPERCASE):
    CPF, Contratos, vl-beneficio, NOME, SEXO, IDADE, POSSIVEL_PROFISSAO, CIDADE, UF, DDDCEL1, CEL1, DDDCEL2, CEL2, TEL1, TEL2, MEMO1, MEMO2

2° - <./clientes_casa> deve conter o formato .xlsx com o cabeçalho CPF (UPPERCASE) e os CPFs devem estar na planilha 'Plan1'

3° - <./discadora_arquivos> É a base que já está na discadora e contém clientes que não devem ser repetidos, deve conter o formato .csv com cabeçalho e separador ';', além de seguir o modelo de colunas (CPF deve estar em UPPERCASE):
    CPF, Contratos, vl-beneficio, NOME, SEXO, IDADE, POSSIVEL_PROFISSAO, CIDADE, UF, DDDCEL1, CEL1, DDDCEL2, CEL2, TEL1, TEL2, MEMO1, MEMO2

4° - <./base_nova_filtrada> É o destino das bases devidamente filtradas, prontas para serem enviadas à discadora

5° - `python ./remover_discadora.py`