import requests
import csv

# Função para obter informações do código postal
def get_postal_info(postal_code):
    url = f"https://api.ctt.pt/v1/codigos-postais/{postal_code}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        return None

# Ler o ficheiro CSV e escrever os resultados
with open('codigos_postais.csv', mode='r') as infile, open('resultados.csv', mode='w', newline='') as outfile:
    reader = csv.reader(infile)
    writer = csv.writer(outfile)
    writer.writerow(['Código Postal', 'Concelho', 'Distrito'])
    
    for row in reader:
        postal_code = row
        info = get_postal_info(postal_code)
        if info:
            concelho = info.get('concelho')
            distrito = info.get('distrito')
            writer.writerow([postal_code, concelho, distrito])
        else:
            writer.writerow([postal_code, 'N/A', 'N/A'])







