from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict
import uvicorn
import traceback
import fastavro
from master_git_vers.routers import builtins


# Функция загрузки разрешённых функций из файла настроек
def load_allowed_builtins(file_path: str = "allowed_builtings.txt"):
    """
    Читает файл с именами разрешённых встроенных функций/типов и формирует словарь для безопасного eval.
    Каждая строка файла должна содержать имя встроенной функции или типа.
    """
    allowed = {}
    try:
        with open(file_path, "r") as f:
            for line in f:
                name = line.strip()
                if name:  # пропускаем пустые строки
                    if name in builtins.__dict__:
                        allowed[name] = builtins.__dict__[name]
                    else:
                        print(f"Встроенная функция или тип '{name}' не найден.")
        return {"__builtins__": allowed}
    except Exception as e:
        raise Exception(f"Ошибка при чтении файла разрешённых функций: {e}")

# Изначально загружаем SAFE_GLOBALS
SAFE_GLOBALS = load_allowed_builtins()

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

@app.get("/reload-builtins", summary="Перезагрузка разрешённых функций")
def reload_builtins():
    """
    Эндпоинт для перечитывания файла настроек и обновления SAFE_GLOBALS.
    """
    global SAFE_GLOBALS
    try:
        SAFE_GLOBALS = load_allowed_builtins()
        return {
            "message": "Разрешённые функции успешно перезагружены.",
            "allowed": list(SAFE_GLOBALS["__builtins__"].keys())
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

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

    # Проверка: имена производных столбцов не должны дублироваться
    calc_names = [calc.columnName for calc in calc_columns]
    if len(calc_names) != len(set(calc_names)):
        error_msg = "Имена производных столбцов не должны дублироваться между собой."
        return TransformResponse(
            outputFileName="",
            errorCode=1,
            errorMessage=error_msg,
            calcErrors={}
        )

    # Предварительная компиляция формул
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

    # Счетчики ошибок для каждого вычисляемого столбца
    error_counts = {col.columnName: 0 for col in calc_columns}

    try:
        # Чтение входного Avro-файла
        with open(input_file, "rb") as infile:
            reader = fastavro.reader(infile)
            data = [record for record in reader]
            schema = reader.schema

        # Проверка: вычисляемые столбцы не должны дублировать исходные имена столбцов
        original_column_names = [field["name"] for field in schema["fields"]]
        for calc in calc_columns:
            if calc.columnName in original_column_names:
                error_msg = f"Имя вычисляемого столбца '{calc.columnName}' дублирует имя исходного столбца."
                return TransformResponse(
                    outputFileName="",
                    errorCode=1,
                    errorMessage=error_msg,
                    calcErrors={}
                )

        # Расширяем схему добавлением вычисляемых столбцов
        for calc in calc_columns:
            schema["fields"].append({
                "name": calc.columnName,
                "type": ["null", "int", "float", "string"],  # Расширенный union-тип
                "default": None
            })

        # Выполняем вычисления для каждой записи
        for record in data:
            for columnName, code in compiled_columns:
                try:
                    # Используем SAFE_GLOBALS как globals и передаем данные записи в locals под именем 'col'
                    result = eval(code, SAFE_GLOBALS, {"col": record})
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

        # Формируем информацию об ошибках для каждого столбца
        calc_errors = {}
        for calc in calc_columns:
            if error_counts[calc.columnName] > 0:
                calc_errors[calc.columnName] = f"Ошибка вычисления в {error_counts[calc.columnName]} экземплярах ячейки"
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
