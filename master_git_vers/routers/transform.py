# routers/transform.py
from fastapi import APIRouter
from ..app_models import TransformRequest, TransformResponse
import fastavro
import traceback
from .. import app_config
from enum import Enum
import time

router = APIRouter()


# Функции для определения и агрегации типов

def determine_type(value):
    """
    Определяет тип переданного значения.
    Если value равно None, возвращает None.
    Если значение имеет тип int, возвращает "int".
    Если значение имеет тип float, возвращает "double".
    Если значение имеет тип str, возвращает "string".
    В остальных случаях возвращает "string".
    """
    if value is None:
        return None
    elif isinstance(value, int): #do enum
        return "int"
    elif isinstance(value, float):#do enum
        return "double"
    elif isinstance(value, str):#do enum
        return "string"
    else:
        return "string"


class TypePriority(Enum):
    INT = 1
    DOUBLE = 2
    STRING = 3

def aggregate_type(current_type, new_type):
    if new_type is None:
        return current_type
    if current_type is None:
        return new_type

    # Если оба типа одинаковые, возвращаем один из них
    if current_type == new_type:
        return current_type

    # Преобразуем строки в соответствующие члены enum
    current_priority = TypePriority[current_type.upper()]
    new_priority = TypePriority[new_type.upper()]  #upper - дорогая операция, добавить в конец, работать с enum

    return new_type if new_priority.value > current_priority.value else current_type


@router.post(
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
    # Замер времени начала обработки
    start_time = time.time()
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
    # Инициализация агрегированных типов для каждого вычисляемого столбца
    aggregated_types = {col.columnName: None for col in calc_columns}

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

        # Выполняем вычисления для каждой записи
        for record in data:
            for columnName, code in compiled_columns:
                try:
                    # Используем app_config.SAFE_GLOBALS как globals и передаем данные записи в locals под именем 'col'
                    result = eval(code, app_config.SAFE_GLOBALS, {"col": record})
                    # Обновляем агрегированный тип для данного столбца
                    current_value_type = determine_type(result)
                    aggregated_types[columnName] = aggregate_type(aggregated_types[columnName], current_value_type)

                    if isinstance(result, (int, float, str)):
                        record[columnName] = result
                    else:
                        record[columnName] = str(result)
                except Exception:
                    record[columnName] = None
                    error_counts[columnName] += 1

        # Добавляем вычисляемые столбцы в схему с агрегированным типом
        type_mapping = {"int": "int", "double": "float", "string": "string"}
        for calc in calc_columns:
            final_type = aggregated_types[calc.columnName] if aggregated_types[
                                                                  calc.columnName] is not None else "string"
            # Приводим полученный тип к верхнему регистру, чтобы получить INT, DOUBLE или STRING
            data_type_value = final_type.upper()

            schema["fields"].append({
                "name": calc.columnName,
                "type": ["null", type_mapping.get(final_type, "string")],
                "default": None,
                "dataType": data_type_value  # Новое поле в схеме
            })

        # Записываем результат в выходной Avro-файл
        with open(output_file, "wb") as outfile:
            fastavro.writer(outfile, schema, data, codec='snappy')
        # Замер времени окончания обработки
        end_time = time.time()
        elapsed_time = end_time - start_time

        # Формируем информацию об ошибках для каждого столбца
        calc_errors = {}
        for calc in calc_columns:
            if error_counts[calc.columnName] > 0:
                calc_errors[calc.columnName] = f"Ошибка вычисления в {error_counts[calc.columnName]} экземплярах ячейки"
            else:
                calc_errors[calc.columnName] = "Все было успешно"
        print(f"Общее время обработки: {elapsed_time:.3f} сек")
        return TransformResponse(
            outputFileName=output_file,
            errorCode=0,
            errorMessage=f"Общее время обработки: {elapsed_time:.3f} сек",
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
