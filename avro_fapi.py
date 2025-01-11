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
    operation: str      # Тип операции: "sum"
    resultColumnName: str  # Имя результирующего столбца
    column1: str        # Первый столбец для сложения
    column2: str        # Второй столбец для сложения

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
    operation = request.operation
    result_column = request.resultColumnName
    column1 = request.column1
    column2 = request.column2
    output_file = "output.avro"  # Имя выходного файла (по умолчанию)

    try:
        # Читаем данные из входного файла Avro
        with open(input_file, "rb") as infile:
            reader = fastavro.reader(infile)
            data = [record for record in reader]
            schema = reader.schema

        # Обрабатываем данные в зависимости от типа операции
        if operation == "sum":  # Пример: сложение двух указанных столбцов
            for record in data:
                if column1 in record and column2 in record:
                    record[result_column] = record[column1] + record[column2]
                else:
                    raise ValueError(f"Ожидаются столбцы '{column1}' и '{column2}' для операции 'sum'.")
            schema["fields"].append({"name": result_column, "type": "int"})

        else:
            raise ValueError(f"Неизвестная операция: {operation}")

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
