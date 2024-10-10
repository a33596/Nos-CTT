# -*- coding: utf-8 -*-
"""
Created on Wed Oct  9 13:58:31 2024

@author: W10
"""

import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import requests
from bs4 import BeautifulSoup
import mysql.connector

# Função para obter concelho e distrito a partir do código postal usando web scraping
def obter_informacoes_webscraping(cp4, cp3, session):
    try:
        url = f'https://www.cttcodigopostal.pt/api/v1/85d55477664d47d589fde4b1451bc9fc/{cp4}-{cp3}'
        response = session.get(url)
        #print(response.json())
        #response.status_code;
        print(response.status_code)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        concelho = soup.find('span', {'class': 'concelho'}).text if soup.find('span', {'class': 'concelho'}) else 'Desconhecido'
        distrito = soup.find('span', {'class': 'distrito'}).text if soup.find('span', {'class': 'distrito'}) else 'Desconhecido'
        return concelho, distrito
    except Exception as e:
        print(f"An error occurred for {cp4}-{cp3}: {e}")
        return 'Erro', 'Erro'

class TestCodigoPostal(unittest.TestCase):

    @patch('requests.Session.get')
    def test_obter_informacoes_webscraping(self, mock_get):
        # Mock the response from requests
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.text = '''
            <html>
                <span class="concelho">Lisboa</span>
                <span class="distrito">Lisboa</span>
            </html>
        '''
        mock_get.return_value = mock_response

        with requests.Session() as session:
            concelho, distrito = obter_informacoes_webscraping('1000', '001', session)
            self.assertEqual(concelho, 'Lisboa')
            self.assertEqual(distrito, 'Lisboa')

    @patch('mysql.connector.connect')
    def test_inserir_atualizar_dados(self, mock_connect):
        # Mock the MySQL connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # Mock the DataFrame
        data = {
            'cp7': ['1000-001'],
            'concelho': ['Lisboa'],
            'distrito': ['Lisboa'],
            'cp4': ['1000'],
            'cp3': ['001']
        }
        df = pd.DataFrame(data)

        # Mock the select and insert/update queries
        mock_cursor.fetchone.return_value = None  # Simulate no existing record

        for index, row in df.iterrows():
            sql_select = "SELECT * FROM codigos_postais WHERE codigo_postal = %s"
            mock_cursor.execute(sql_select, (row['cp7'],))
            result = mock_cursor.fetchone()
            
            if result:
                sql_update = "UPDATE codigos_postais SET concelho = %s, distrito = %s WHERE codigo_postal = %s"
                mock_cursor.execute(sql_update, (row['concelho'], row['distrito'], row['cp7']))
            else:
                sql_insert = "INSERT INTO codigos_postais (codigo_postal, concelho, distrito) VALUES (%s, %s, %s)"
                mock_cursor.execute(sql_insert, (row['cp7'], row['concelho'], row['distrito']))

        # Verify that the insert query was called
        self.assertTrue(mock_cursor.execute.called)
        
if __name__ == '__main__':
    unittest.main()
