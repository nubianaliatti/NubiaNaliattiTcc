import os
import csv

def estruturar_csv_samsung(directory):
    try:
        for file_name in os.listdir(directory):
            if file_name.endswith('.csv'):
                file_path = os.path.join(directory, file_name)
                
                with open(file_path, 'r', newline='', encoding='utf-8') as file:
                    reader = list(csv.reader(file, delimiter=','))
                
                if len(reader) > 1:
                    header = reader[1]  # Segunda linha como cabeçalho real
                    data = reader[2:]   # Dados reais começam na terceira linha
                    
                    # Verificar se é o arquivo específico e reorganizar coluna
                    if file_name == "com.samsung.shealth.exercise.csv":
                        if "com.samsung.health.exercise.exercise_type" in header:
                            index_exercise_type = header.index("com.samsung.health.exercise.exercise_type")
                            
                            # Movendo a coluna correta para a primeira posição
                            header.insert(0, header.pop(index_exercise_type))
                            for row in data:
                                row.insert(0, row.pop(index_exercise_type))
                            
                            data = [row for row in data if row[0] == "1002"]
                    
                    with open(file_path, 'w', newline='', encoding='utf-8') as file:
                        writer = csv.writer(file, delimiter=',')
                        writer.writerow(header)
                        writer.writerows(data)
        
        return {"message": "CSV estruturado com sucesso!"}
    except Exception as e:
        return {"message": f"Erro ao processar arquivos CSV: {str(e)}"}
