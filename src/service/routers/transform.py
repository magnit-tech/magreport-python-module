from fastapi import APIRouter
from ..app_models import TransformRequest, TransformResponse
import fastavro
import traceback
from .. import app_config
import re
from collections import deque

router = APIRouter()

# --- Приоритеты ---
_INT, _DOUBLE, _STRING = 1, 2, 3

def determine_priority(value):
    t = type(value)
    if t is int:
        return _INT
    elif t is float:
        return _DOUBLE
    else:
        return _STRING

def aggregate_priority(current, new):
    if current is None:
        return new
    if new is None:
        return current
    return new if new > current else current

# --- Зависимости / сортировка ---
def extract_dependencies(formula):
    return re.findall(r"col\[['\"]([^'\"]+)['\"]\]", formula)

def build_dependency_graph(calc_columns):
    names = {c.columnName for c in calc_columns}
    graph = {}
    for c in calc_columns:
        deps = extract_dependencies(c.columnFormula)
        graph[c.columnName] = [d for d in deps if d in names]
    return graph

def topological_sort(graph):
    in_deg = {u: len(deps) for u, deps in graph.items()}
    rev   = {u: [] for u in graph}
    for u, deps in graph.items():
        for v in deps:
            rev[v].append(u)
    q = deque(u for u, d in in_deg.items() if d == 0)
    order = []
    while q:
        u = q.popleft()
        order.append(u)
        for w in rev[u]:
            in_deg[w] -= 1
            if in_deg[w] == 0:
                q.append(w)
    cyclic = {u for u, d in in_deg.items() if d > 0}
    return order, cyclic

# --- Основной эндпоинт ---
@router.post("/transform", response_model=TransformResponse, tags=["Data Transformation"])
def transform(request: TransformRequest):
    input_file   = request.inputFileName
    output_file  = request.outputFileName
    calc_columns = request.calcColumns

    # 1) Уникальность имён
    names = [c.columnName for c in calc_columns]
    if len(names) != len(set(names)):
        return TransformResponse(
            outputFileName="",
            errorCode=1,
            errorMessage="Имена производных столбцов не должны дублироваться.",
            calcErrors={}
        )

    # 2) Построить граф и получить порядок + циклики
    graph = build_dependency_graph(calc_columns)
    order, cyclic = topological_sort(graph)

    # 3) Компиляция формул в нужном порядке
    code_map = {c.columnName: compile(c.columnFormula, "", "eval") for c in calc_columns}
    compiled = [(n, code_map[n]) for n in order + [n for n in names if n in cyclic]]

    # 4) Инициализация счётчиков и деталей первой ошибки
    error_counts  = {n: 0 for n in names}
    error_details = {n: None for n in names}
    priorities    = {n: None for n in names}

    try:
        # 5) Чтение входного Avro
        with open(input_file, "rb") as f:
            reader = fastavro.reader(f)
            data   = list(reader)
            schema = reader.schema

        # 6) Проверка дублирования имён полей
        existing = {fld["name"] for fld in schema["fields"]}
        for n in names:
            if n in existing:
                return TransformResponse(
                    outputFileName="",
                    errorCode=1,
                    errorMessage=f"Вычисляемый столбец '{n}' дублирует существующий.",
                    calcErrors={}
                )

        # 7) Вычисления
        for row_idx, rec in enumerate(data, start=1):
            loc = {"col": rec}
            for name, code in compiled:
                if name in cyclic:
                    rec[name] = None
                    error_counts[name] += 1
                    continue
                try:
                    res = eval(code, app_config.SAFE_GLOBALS, loc)
                    pr  = determine_priority(res)
                    priorities[name] = aggregate_priority(priorities[name], pr)
                    rec[name] = res if isinstance(res, (int, float, str)) else str(res)
                except Exception as e:
                    rec[name] = None
                    if error_counts[name] == 0:
                        error_details[name] = (row_idx, str(e))
                    error_counts[name] += 1

        # 8) Обновление схемы и запись выходного Avro
        AVRO_MAP = {
            _INT:    ("int","INT"),
            _DOUBLE: ("float","DOUBLE"),
            _STRING: ("string","STRING"),
        }
        for n in names:
            p = priorities[n] or _STRING
            avro_t, data_t = AVRO_MAP[p]
            schema["fields"].append({
                "name":     n,
                "type":     ["null", avro_t],
                "default":  None,
                "dataType": data_t
            })

        with open(output_file, "wb") as f:
            fastavro.writer(f, schema, data, codec="snappy")

        # 9) Формирование calcErrors
        calc_errors = {}
        for n in names:
            cnt = error_counts[n]
            if cnt == 0:
                calc_errors[n] = "Все было успешно"
            else:
                row_idx, msg = error_details[n]
                calc_errors[n] = (
                    f"Ошибка вычисления в {cnt} экземплярах. "
                    f"Первая ошибка на строке {row_idx}: {msg}"
                )

        return TransformResponse(
            outputFileName=output_file,
            errorCode=0,
            errorMessage="",      # пусто, т.к. времени больше нет
            calcErrors=calc_errors
        )

    except Exception as e:
        return TransformResponse(
            outputFileName="",
            errorCode=1,
            errorMessage=f"Internal error: {e}",
            calcErrors={}
        )