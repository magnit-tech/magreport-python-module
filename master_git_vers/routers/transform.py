from fastapi import APIRouter
from ..app_models import TransformRequest, TransformResponse
import fastavro
import traceback
from .. import app_config
import time

router = APIRouter()

# --- Оптимизированные функции для определения и агрегации приоритетов ---

_INT, _DOUBLE, _STRING = 1, 2, 3

def determine_priority(value):
    """
    Определяет приоритет переданного значения:
    целые -> INT, float -> DOUBLE, прочие (включая None) -> STRING
    """
    t = type(value)
    if t is int:
        return _INT
    if t is float:
        return _DOUBLE
    return _STRING


def aggregate_priority(current, new):
    """
    Агрегирует два приоритета: если один None — возвращает другой,
    иначе — максимальный.
    """
    if current is None:
        return new
    if new is None:
        return current
    return new if new > current else current

# --- Конец оптимизированных функций ---

@router.post(
    "/transform",
    summary="Преобразование данных из Avro-файла с расчетом новых столбцов",
    description=(
        "Принимает имя входного файла, выходного файла и массив вычисляемых "
        "столбцов с формулами. Вычисления выполняются последовательно: "
        "результат одного может использоваться в последующих. Если вычисление "
        "для записи завершается ошибкой, значение становится null, а ошибки "
        "агрегируются в ответе."
    ),
    response_model=TransformResponse,
    tags=["Data Transformation"]
)
def transform(request: TransformRequest):
    input_file = request.inputFileName
    output_file = request.outputFileName
    calc_columns = request.calcColumns

    # 1) Проверка: уникальность имён вычисляемых столбцов
    names = [c.columnName for c in calc_columns]
    if len(names) != len(set(names)):
        return TransformResponse(
            outputFileName="",
            errorCode=1,
            errorMessage="Имена производных столбцов не должны дублироваться между собой.",
            calcErrors={}
        )

    start_time = time.time()

    # 2) Компиляция формул
    compiled = []
    for col in calc_columns:
        try:
            code = compile(col.columnFormula, filename="", mode="eval")
            compiled.append((col.columnName, code))
        except Exception:
            return TransformResponse(
                outputFileName="",
                errorCode=1,
                errorMessage=(
                    f"Ошибка компиляции формулы для столбца '{col.columnName}':\n"
                    f"{traceback.format_exc()}"
                ),
                calcErrors={}
            )

    # 3) Инициализация счётчиков
    error_counts = {c.columnName: 0 for c in calc_columns}
    priorities   = {c.columnName: None for c in calc_columns}
    _det = determine_priority
    _agg = aggregate_priority

    # 4) Локальные ссылки и подготовка для eval
    sg = app_config.SAFE_GLOBALS
    loc = {}
    writer = fastavro.writer
    AVRO_MAP = {
        _INT:    ("int",    "INT"),
        _DOUBLE: ("float",  "DOUBLE"),
        _STRING: ("string", "STRING"),
    }

    try:
        # 5) Чтение Avro
        with open(input_file, "rb") as f:
            reader = fastavro.reader(f)
            data   = [r for r in reader]
            schema = reader.schema

        # 6) Проверка дублирования имён
        existing = {fld["name"] for fld in schema["fields"]}
        for col in calc_columns:
            if col.columnName in existing:
                return TransformResponse(
                    outputFileName="",
                    errorCode=1,
                    errorMessage=(
                        f"Имя вычисляемого столбца '{col.columnName}' "
                        "дублирует имя исходного столбца."
                    ),
                    calcErrors={}
                )

        # 7) Основной цикл вычислений
        for rec in data:
            loc['col'] = rec
            for name, code in compiled:
                try:
                    res = eval(code, sg, loc)
                    pr  = _det(res)
                    priorities[name] = _agg(priorities[name], pr)
                    rec[name] = res if isinstance(res, (int, float, str)) else str(res)
                except Exception:
                    rec[name] = None
                    error_counts[name] += 1

        # 8) Обновление схемы и запись
        for col in calc_columns:
            p = priorities[col.columnName] or _STRING
            avro_type, data_type = AVRO_MAP[p]
            schema["fields"].append({
                "name":    col.columnName,
                "type":    ["null", avro_type],
                "default": None,
                "dataType": data_type,
            })

        with open(output_file, "wb") as f:
            writer(f, schema, data, codec="snappy")

        # 9) Отчёт и возврат ответа
        elapsed = time.time() - start_time
        calc_errors = {
            col.columnName: (
                f"Ошибка вычисления в {error_counts[col.columnName]} экземплярах"
                if error_counts[col.columnName] else "Все было успешно"
            )
            for col in calc_columns
        }

        return TransformResponse(
            outputFileName=output_file,
            errorCode=0,
            errorMessage=f"Общее время обработки: {elapsed:.3f} сек",
            calcErrors=calc_errors
        )

    except Exception:
        return TransformResponse(
            outputFileName="",
            errorCode=1,
            errorMessage=traceback.format_exc(),
            calcErrors={}
        )
