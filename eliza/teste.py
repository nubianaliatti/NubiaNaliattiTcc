import os
import re

def pegar_nome_pasta_samsunghealth(caminho_raiz):
    """
    Função para encontrar e retornar o nome da pasta 'samsunghealth' dentro de 'extracted'

    Args:
        caminho_raiz: O caminho para a pasta 'extracted'

    Returns:
        O nome da pasta 'samsunghealth' (ex: 'samsunghealth_pedroalmeidavrb_2025010...'), ou None se não encontrado
    """
    
    if not os.path.exists(caminho_raiz):
        return None 

    for nome_pasta in os.listdir(caminho_raiz):
        caminho_completo = os.path.join(caminho_raiz, nome_pasta)
        if os.path.isdir(caminho_completo):  
           
            match = re.search(r"samsunghealth_.+", nome_pasta)  
            if match:
                return nome_pasta  
    return None  


caminho_extracted = "eliza/extracted" 
nome_pasta_samsunghealth = pegar_nome_pasta_samsunghealth(caminho_extracted)

if nome_pasta_samsunghealth:
    print(f"O nome da pasta 'samsunghealth' é: {nome_pasta_samsunghealth}")

    caminho_arquivos_csv = os.path.join(caminho_extracted, nome_pasta_samsunghealth)
    print(f"O caminho para os arquivos CSV é: {caminho_arquivos_csv}")

    caminhos_csv = [] 
    if os.path.exists(caminho_arquivos_csv):
      for arquivo in os.listdir(caminho_arquivos_csv):
        if arquivo.endswith(".csv"):
          caminho_completo_csv = os.path.join(caminho_arquivos_csv, arquivo)
          caminhos_csv.append(caminho_completo_csv) 
          print(f"Arquivo CSV encontrado: {arquivo}")

    print("\nArray com os caminhos dos arquivos CSV:")
    for caminho in caminhos_csv:
        print(caminho)
else:
    print("Nenhuma pasta 'samsunghealth' encontrada.")