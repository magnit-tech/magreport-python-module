git checkout master
from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Dict
import uvicorn
import traceback
import fastavro

# Создаем приложение FastAPI
app = FastAPI()

# Модель для описания вычисляемого столбца
class CalcColumn(BaseModel):
    columnName: str
    columnFormula: str

# Модель для входного запроса
class TransformRequest(BaseModel):
    inputFileName: str
    outputFileName: str
    calcColumns: List[CalcColumn]

# Модель для ответа
class TransformResponse(BaseModel):
    outputFileName: str
    errorCode: int
    errorMessage: str
    calcErrors: Dict[str, str]  # Для каждого вычисляемого столбца: сообщение об ошибке или "Все было успешно"

@app.post(
    "/transform",
    summary="Преобразование данных из Avro-файла с расчетом новых столбцов",
    description="Принимает имя входного файла, выходного файла и массив вычисляемых столбцов с формулами. "
                "Вычисления выполняются последовательно: результат одного может использоваться в последующих. "
                "Если вычисление для записи завершается ошибкой, значение становится null, а ошибки агрегируются в ответе.",
    response_model=TransformResponse,
    tags=["Data Transformation"]
)
def transform(request: TransformRequest):
    input_file = request.inputFileName
    output_file = request.outputFileName
    calc_columns = request.calcColumns

    # Попытка компиляции формул заранее
    compiled_columns = []
    for col in calc_columns:
        try:
            code = compile(source=col.columnFormula, filename='', mode='eval')
            compiled_columns.append((col.columnName, code))
        except Exception:
            error_msg = f"Ошибка компиляции формулы для столбца '{col.columnName}':\n{traceback.format_exc()}"
            return TransformResponse(
                outputFileName="",
                errorCode=1,
                errorMessage=error_msg,
                calcErrors={}
            )

    # Инициализируем счетчики ошибок для каждого столбца
    error_counts = {col.columnName: 0 for col in calc_columns}

    try:
        # Чтение входного Avro-файла
        with open(input_file, "rb") as infile:
            reader = fastavro.reader(infile)
            data = [record for record in reader]
            schema = reader.schema

        # Динамически расширяем схему: добавляем вычисляемые поля, если их еще нет
        for calc in calc_columns:
            if not any(field["name"] == calc.columnName for field in schema["fields"]):
                schema["fields"].append({
                    "name": calc.columnName,
                    "type": ["null", "int", "float", "string"],  # Расширенный union-тип
                    "default": None
                })

        # Выполняем вычисления для каждой записи
        for record in data:
            # Используем переменную "col" для удобства доступа к полям записи при eval
            col = record
            for columnName, code in compiled_columns:
                try:
                    result = eval(code, {"col": col})
                    # Если результат имеет базовый тип (int, float или str), оставляем его как есть.
                    # Иначе приводим к строке.
                    if isinstance(result, (int, float, str)):
                        record[columnName] = result
                    else:
                        record[columnName] = str(result)
                except Exception:
                    record[columnName] = None
                    error_counts[columnName] += 1

        # Записываем результат в выходной Avro-файл
        with open(output_file, "wb") as outfile:
            fastavro.writer(outfile, schema, data, codec='snappy')

        # Формируем информацию об ошибках для каждого вычисляемого столбца
        calc_errors = {}
        for calc in calc_columns:
            if error_counts[calc.columnName] > 0:
                calc_errors[calc.columnName] = f"Ошибка вычисления в {error_counts[calc.columnName]} в экземплярах ячейки"
            else:
                calc_errors[calc.columnName] = "Все было успешно"

        return TransformResponse(
            outputFileName=output_file,
            errorCode=0,
            errorMessage="",
            calcErrors=calc_errors
        )

    except Exception:
        error_message = traceback.format_exc()
        return TransformResponse(
            outputFileName="",
            errorCode=1,
            errorMessage=error_message,
            calcErrors={}
        )

# Запуск сервера, если файл запускается напрямую
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
