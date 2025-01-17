from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
import traceback
import fastavro
import os

# Создаем приложение FastAPI
app = FastAPI()

# Модель для входных данных
class TransformRequest(BaseModel):
    inputFileName: str  # Имя входного файла
    outputFileName: str # Имя выходного файла
    resultColumnName: str  # Имя результирующего столбца
    formula: str         # Формула для вычисления нового столбца

# Модель для выходных данных
class TransformResponse(BaseModel):
    outputFileName: str
    errorCode: int
    errorMessage: str

# Определяем эндпоинт для обработки запросов
@app.post("/transform",
          summary="Преобразование данных из файла",
          description="Принимает имя входного файла (в формате Avro), тип операции, два столбца для сложения и имя результирующего столбца. Возвращает имя выходного файла и информацию об ошибках.",
          response_model=TransformResponse,
          tags=["Data Transformation"])
def transform(request: TransformRequest):
    input_file = request.inputFileName
    result_column = request.resultColumnName
    output_file = request.outputFileName
    formula = request.formula

    try:
        code = compile(source=formula, filename='', mode='eval')

        # Читаем данные из входного файла Avro
        with open(input_file, "rb") as infile:
            reader = fastavro.reader(infile)
            data = [record for record in reader]
            schema = reader.schema

        # Применяем формулу
        for col in data:
            col[result_column] = eval(code)
        
        schema["fields"].append({"name": result_column, "type": "int"})

        # Записываем данные в выходной файл Avro
        with open(output_file, "wb") as outfile:
            fastavro.writer(outfile, schema, data)

        return TransformResponse(outputFileName=output_file, errorCode=0, errorMessage="")

    except Exception as e:
        error_message = traceback.format_exc()
        return TransformResponse(outputFileName="", errorCode=1, errorMessage=error_message)

# Запускаем сервер, если файл запускается напрямую
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
