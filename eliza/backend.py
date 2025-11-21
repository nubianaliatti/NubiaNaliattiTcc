import os
import zipfile
import re
from flask import Flask, request, jsonify
from flask_cors import CORS
import traceback
from process_csv import estruturar_csv_samsung
import subprocess
import os
import sys
import papermill as pm


app = Flask(__name__)
CORS(app)
# Exemplo: 500 MB
app.config['MAX_CONTENT_LENGTH'] = 500 * 1024 * 1024

#notebook_path = os.path.abspath(os.path.join(os.getcwd(), "..", "final_20_11.ipynb"))
notebook_path = os.path.abspath(os.path.join(os.getcwd(), "..", "final_20_11.ipynb"))
notebook_dir = os.path.dirname(notebook_path)

UPLOAD_FOLDER = './eliza/uploads'
EXTRACT_FOLDER = './eliza/extracted'

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(EXTRACT_FOLDER, exist_ok=True)

# Regex para arquivos Samsung Health com timestamp (14 dígitos)
file_patterns = [
    r"com\.samsung\.shealth\.exercise\.\d{14}",
    r"com\.samsung\.shealth\.exercise\.recovery_heart_rate\.\d{14}",
    r"com\.samsung\.shealth\.sleep\.\d{14}",
    r"com\.samsung\.shealth\.exercise\.weather\.\d{14}"
]

@app.route('/upload', methods=['POST'])
def upload_file():
    try:
        print("Recebendo arquivo...")
        if 'zipFile' not in request.files:
            return jsonify({"message": "Nenhum arquivo enviado."}), 400

        file = request.files['zipFile']
        if file.filename == '' or not file.filename.endswith('.zip'):
            return jsonify({"message": "Por favor, envie um arquivo .zip válido."}), 400

        # Salvar ZIP
        zip_path = os.path.join(UPLOAD_FOLDER, file.filename)
        file.save(zip_path)

        # Criar pasta para extração
        extract_path = os.path.join(EXTRACT_FOLDER, os.path.splitext(file.filename)[0])
        os.makedirs(extract_path, exist_ok=True)

        # Extrair arquivos que casam com os padrões
        matched_files = []
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            all_files = zip_ref.namelist()
            for file_name in all_files:
                relative_name = os.path.basename(file_name)
                if any(re.search(pattern, relative_name) for pattern in file_patterns):
                    new_name = re.sub(r"\.\d{14}", "", relative_name)  # remove timestamp
                    final_path = os.path.join(extract_path, new_name)
                    with zip_ref.open(file_name) as source, open(final_path, "wb") as target:
                        target.write(source.read())
                    matched_files.append(new_name)
                    
        # Aqui você pode chamar sua função de processamento
        print(estruturar_csv_samsung(extract_path))
        
        # Caminho para o script que você quer executar
        script_path = os.path.abspath('../final.py')  # ajuste conforme seu diretório
        # caminho da pasta onde os CSVs foram extraídos
        extracted_folder = os.path.abspath('./extracted')
        # Verifica se o arquivo existe
        if not os.path.isfile(script_path):
            print(f"❌ Erro: arquivo não encontrado em {script_path}")
            sys.exit(1)  # encerra o programa

        try:
            # Executa o script e captura saída
            result = ( subprocess.run(['python', script_path], cwd=extract_path, check=True) )
            # Mostra a saída do script
            print(result.stdout)
            print("✅ Processamento concluído com sucesso!")

        except subprocess.CalledProcessError as e:
            # Caso o script retorne erro
            print(f"❌ Ocorreu um erro ao executar o script: {e}")
            print("Saída do script:")
            print(e.stdout)
            print("Erros do script:")
            print(e.stderr)

        except Exception as ex:
            # Qualquer outro erro
            print(f"❌ Erro inesperado: {ex}")

        try:
            # Executa o notebook usando o Python do mesmo ambiente
            '''result = subprocess.run(
                [
                    sys.executable,  # garante que vai usar o mesmo Python que roda o backend
                    "-m", "nbconvert",
                    "--to", "notebook",
                    "--execute",
                    notebook_path,
                    "--output", "executed_notebook.ipynb"
                ],
                cwd=notebook_dir,
                check=True,
                capture_output=True,
                text=True
            )'''

            notebook_input = notebook_path
            notebook_output = notebook_input

            pm.execute_notebook(
                notebook_input,
                notebook_output,
                parameters=dict(distancia=5000)  # valor passado pelo backend
            )


            print("✅ Notebook executado com sucesso!")
            print("Saída do subprocess:")
            print(result.stdout)

        except subprocess.CalledProcessError as e:
            print("❌ Erro ao executar o notebook:")
            print(e.stdout)
            print(e.stderr)


        # Se nenhum arquivo foi encontrado, retorna aviso
        if not matched_files:
            os.remove(zip_path)
            return jsonify({
                "message": "Nenhum arquivo esperado foi encontrado no ZIP.",
                "patterns": file_patterns
            }), 200

        os.remove(zip_path)
        return jsonify({
            "message": "Arquivo processado com sucesso!",
            "extraidos": matched_files,
            "pasta": extract_path
        }), 200

    except Exception as e:
        print("\n=== ERRO DETALHADO ===")
        traceback.print_exc()
        print("=== FIM DO ERRO ===\n")
        return jsonify({"message": f"Erro ao processar o arquivo: {str(e)}"}), 500
    

if __name__ == '__main__':
    app.run(debug=True, use_reloader=False)
