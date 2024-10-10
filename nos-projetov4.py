import pandas as pd
import requests
from mysql.connector import connect, Error

MAX_REQUEST = 30
req_num = 0

# Leitura do ficheiro CSV
df = pd.read_csv('codigos_postais.csv')

# Ensure the columns are of string type
df['concelho'] = df['concelho'].astype(str)
df['distrito'] = df['distrito'].astype(str)

# Função para obter concelho e distrito a partir do código postal usando web scraping
def obter_informacoes_webscraping(cp4, cp3):
    global req_num
    concelho = ''
    distrito = ''
    linkcp = f'https://www.cttcodigopostal.pt/api/v1/85d55477664d47d589fde4b1451bc9fc/{cp4}-{cp3}'
    print(linkcp)
    if req_num >= MAX_REQUEST:
        print("Limite Chegou 30 pedidos")
        return '', ''
    else:
        response = requests.get(linkcp)
        response.raise_for_status()
        if response.status_code == 200:
            req_num += 1
            data = response.json()
            for entry in data:
                concelho = entry['concelho']
                distrito = entry['distrito']
            print(concelho, distrito)
            return concelho, distrito
        else:
            print(f"{response.status_code}")
            return '', ''

# Verifique se a coluna 'cp7' existe no dataframe
if 'cp7' in df.columns:
    # Dividir o código postal em cp4 e cp3
    df[['cp4', 'cp3']] = df['cp7'].str.split('-', expand=True)
    for i, row in df.iterrows():
        concelho, distrito = obter_informacoes_webscraping(row['cp4'], row['cp3'])
        df.at[i, 'concelho'] = concelho if concelho else 'Desconhecido'
        df.at[i, 'distrito'] = distrito if distrito else 'Desconhecido'
    print(df)

    # Conectar ao banco de dados MySQL
    try:
        conn = connect(
            host='localhost',
            user='root',
            password='5KaI|1l<&8Z~',
            database='new_schema'
        )
        if conn.is_connected():
            cursor = conn.cursor()

            # Criar a tabela se não existir
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS codigos_postais (
                    codigo_postal VARCHAR(10) PRIMARY KEY,
                    concelho VARCHAR(100),
                    distrito VARCHAR(100)
                )
            """)

            # Inserir ou atualizar os dados na tabela
            for index, row in df.iterrows():
                sql_select = "SELECT * FROM codigos_postais WHERE codigo_postal = %s"
                cursor.execute(sql_select, (row['cp7'],))
                result = cursor.fetchone()

                if result:
                    sql_update = "UPDATE codigos_postais SET concelho = %s, distrito = %s WHERE codigo_postal = %s"
                    cursor.execute(sql_update, (row['concelho'], row['distrito'], row['cp7']))
                else:
                    sql_insert = "INSERT INTO codigos_postais (codigo_postal, concelho, distrito) VALUES (%s, %s, %s)"
                    cursor.execute(sql_insert, (row['cp7'], row['concelho'], row['distrito']))

            # Commit e fechar a conexão
            conn.commit()
            cursor.close()
            conn.close()
            print("Dados inseridos/atualizados com sucesso no banco de dados.")
        else:
            print("Falha na conexão com o banco de dados.")
    except Error as e:
        print(f"Erro ao conectar ao banco de dados: {e}")

    # Exportar o dataframe enriquecido para um novo ficheiro CSV
    df.to_csv('codigos_postais_enriquecidos.csv', index=False)
    print("Dados processados com sucesso.")
else:
    print("A coluna 'cp7' não foi encontrada no ficheiro CSV.")


