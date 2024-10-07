import pandas as pd
import requests
import time
from tqdm import tqdm
import mysql.connector
from mysql.connector import Error
from flask import Flask, jsonify, request

app = Flask(__name__)

# Função para buscar informações do código postal com tratamento de erro
def get_postal_info(postal_code):
    try:
        url = f"https://api.ctt.pt/v1/codigos-postais/{postal_code}"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            return data.get('concelho', 'N/A'), data.get('distrito', 'N/A')
        else:
            return 'N/A', 'N/A'
    except requests.exceptions.RequestException as e:
        print(f"Erro ao buscar o código postal {postal_code}: {e}")
        return 'N/A', 'N/A'

# Carregar o arquivo CSV
df = pd.read_csv('C:\\Users\\W10\\Downloads\\codigos_postais.csv')

# Verificar se as colunas 'Concelho' e 'Distrito' já existem, se não, criar
if 'Concelho' not in df.columns:
    df['Concelho'] = ''
if 'Distrito' not in df.columns:
    df['Distrito'] = ''

# Preencher as colunas com as informações obtidas pela API com barra de progresso
for index, row in tqdm(df.iterrows(), total=df.shape, desc="Atualizando Códigos Postais"):
    if pd.isna(row['Concelho']) or pd.isna(row['Distrito']) or row['Concelho'] == '' or row['Distrito'] == '':
        concelho, distrito = get_postal_info(row['cp7'])
        df.at[index, 'Concelho'] = concelho
        df.at[index, 'Distrito'] = distrito
        time.sleep(1)  # Pausa para evitar sobrecarregar a API

# Salvar o DataFrame atualizado em um novo arquivo CSV
df.to_csv('C:\\Users\\W10\\Downloads\\arquivo_atualizado.csv', index=False)
print("Arquivo atualizado salvo com sucesso!")

# Função para inserir dados no MySQL
def insert_data(df):
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='new_schema',
            user='root',
            password='5KaI|1l<&8Z~'
        )
        if connection.is_connected():
            cursor = connection.cursor()
            for index, row in df.iterrows():
                sql = "INSERT INTO codigos_postais (cp7, concelho, distrito) VALUES (%s, %s, %s)"
                val = (row['cp7'], row['Concelho'], row['Distrito'])
                cursor.execute(sql, val)
            connection.commit()
            print("Dados inseridos com sucesso!")
    except Error as e:
        print(f"Erro ao conectar ao MySQL: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("Conexão ao MySQL encerrada.")

# Carregar o arquivo CSV
df = pd.read_csv('C:\\Users\\W10\\Downloads\\arquivo_atualizado.csv')

# Inserir dados no MySQL
insert_data(df)

# Função para buscar dados do MySQL
def fetch_data_from_mysql():
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='new_schema',
            user='root',
            password='5KaI|1l<&8Z~'
        )
        if connection.is_connected():
            cursor = connection.cursor(dictionary=True)
            cursor.execute("SELECT * FROM codigos_postais")
            result = cursor.fetchall()
            return result
    except Error as e:
        print(f"Erro ao conectar ao MySQL: {e}")
        return []
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Rota para buscar todos os dados
@app.route('/codigos_postais', methods=['GET'])
def get_codigos_postais():
    data = fetch_data_from_mysql()
    return jsonify(data)

# Rota para buscar dados por código postal
@app.route('/codigos_postais/<postal_code>', methods=['GET'])
def get_codigo_postal(postal_code):
    data = fetch_data_from_mysql()
    result = next((item for item in data if item["cp7"] == postal_code), None)
    if result:
        return jsonify(result)
    else:
        return jsonify({"message": "Código postal não encontrado"}), 404

if __name__ == '__main__':
    app.run(debug=True)

