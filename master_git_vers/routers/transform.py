from fastapi import APIRouter
from ..app_models import TransformRequest, TransformResponse
import fastavro
import traceback
from .. import app_config
import time
import re
from collections import deque

router = APIRouter()

# --- Оптимизированные функции для определения и агрегации приоритетов ---
_INT, _DOUBLE, _STRING = 1, 2, 3

def determine_priority(value):
    t = type(value)
    if t is int:
        return _INT
    if t is float:
        return _DOUBLE
    return _STRING


def aggregate_priority(current, new):
    if current is None:
        return new
    if new is None:
        return current
    return new if new > current else current

# --- Функции для построения графа и сортировки ---

def extract_dependencies(formula):
    pattern = r"col\[['\"]([^'\"]+)['\"]\]"
    return re.findall(pattern, formula)


def build_dependency_graph(calc_columns):
    computed = set(c.columnName for c in calc_columns)
    graph = {}
    for c in calc_columns:
        deps = extract_dependencies(c.columnFormula)
        graph[c.columnName] = [d for d in deps if d in computed]
    return graph


def topological_sort(graph):
    """
    Выполняет топологическую сортировку ориентированного графа.
    Возвращает (ordered_list, cyclic_nodes).
    graph: dict, где ключ — узел, значение — список зависимостей (ребра u->deps)
    """
    # 1) Вычисляем число зависимостей (in-degree) для каждого узла
    in_degree = {u: len(deps) for u, deps in graph.items()}
    # 2) Строим обратный список: для каждого узла список зависимых от него
    reverse_adj = {u: [] for u in graph}
    for u, deps in graph.items():
        for v in deps:
            reverse_adj[v].append(u)
    # 3) Инициализируем очередь узлов без зависимостей
    queue = deque([u for u, deg in in_degree.items() if deg == 0])
    ordered = []
    # 4) Проходимся по очереди
    while queue:
        u = queue.popleft()
        ordered.append(u)
        # для каждого зависимого уменьшаем счётчик
        for dependent in reverse_adj.get(u, []):
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)
    # 5) Узлы с ненулевым in_degree — частью цикла
    cyclic = {u for u, deg in in_degree.items() if deg > 0}
    return ordered, cyclic

# --- Конец функций ---

@router.post(
    "/transform",
    summary="Преобразование данных из Avro-файла с расчетом новых столбцов",
    description=(
        "Принимает имя входного файла, выходного файла и массив вычисляемых "
        "столбцов с формулами. Вычисления выполняются в порядке топологической "
        "сортировки зависимостей. Циклические зависимости дают null и лог ошибок."
    ),
    response_model=TransformResponse,
    tags=["Data Transformation"]
)
def transform(request: TransformRequest):
    input_file = request.inputFileName
    output_file = request.outputFileName
    calc_columns = request.calcColumns

    # 1) Проверка уникальности имён
    names = [c.columnName for c in calc_columns]
    if len(names) != len(set(names)):
        return TransformResponse(outputFileName="", errorCode=1,
            errorMessage="Имена производных столбцов не должны дублироваться.", calcErrors={})

    # 2) Построение графа зависимостей
    graph = build_dependency_graph(calc_columns)
    print(f"Dependency graph: {graph}")
    order, cyclic = topological_sort(graph)
    print(f"Topological order: {order}")
    print(f"Cyclic nodes: {cyclic}")

    start_time = time.time()

    # 3) Компиляция формул
    name_to_code = {c.columnName: compile(c.columnFormula, '', 'eval') for c in calc_columns}
    compiled = []
    # Упорядочиваем в соответствии с topological sort
    for name in order + [n for n in names if n in cyclic]:
        compiled.append((name, name_to_code[name]))
    print(f"Compile order: {[name for name, _ in compiled]}")

    # 4) Инициализация счётчиков
    error_counts = {n: 0 for n in names}
    priorities   = {n: None for n in names}
    _det = determine_priority
    _agg = aggregate_priority

    # 5) Подготовка локалей и writer
    sg = app_config.SAFE_GLOBALS
    loc = {}
    writer = fastavro.writer
    AVRO_MAP = {
        _INT:    ("int","INT"),
        _DOUBLE: ("float","DOUBLE"),
        _STRING: ("string","STRING"),
    }

    try:
        # 6) Чтение Avro
        with open(input_file, 'rb') as f:
            reader = fastavro.reader(f)
            data   = [r for r in reader]
            schema = reader.schema

        # 7) Проверка дублирования имён
        existing = {fld['name'] for fld in schema['fields']}
        for n in names:
            if n in existing:
                return TransformResponse(
                    outputFileName="", errorCode=1,
                    errorMessage=f"Вычисляемый столбец '{n}' дублирует существующий.",
                    calcErrors={}
                )

        # 8) Основной цикл вычислений
        for rec in data:
            loc['col'] = rec
            for name, code in compiled:
                if name in cyclic:
                    rec[name] = None
                    error_counts[name] += 1
                    continue
                try:
                    res = eval(code, sg, loc)
                    pr  = _det(res)
                    priorities[name] = _agg(priorities[name], pr)
                    rec[name] = res if isinstance(res,(int,float,str)) else str(res)
                except Exception:
                    rec[name] = None
                    error_counts[name] += 1

        # 9) Обновление схемы
        for n in names:
            p = priorities[n] or _STRING
            avro_t, data_t = AVRO_MAP[p]
            schema['fields'].append({'name':n,'type':['null',avro_t],'default':None,'dataType':data_t})

        with open(output_file,'wb') as f:
            writer(f,schema,data,codec='snappy')

        # 10) Отчёт
        elapsed = time.time() - start_time
        calc_errors = {n:(f"Ошибка вычисления в {error_counts[n]} экземплярах" if error_counts[n] else "Все было успешно") for n in names}
        return TransformResponse(outputFileName=output_file,errorCode=0,errorMessage=f"Время: {elapsed:.3f} сек",calcErrors=calc_errors)

    except Exception:
        return TransformResponse(outputFileName="",errorCode=1,errorMessage=traceback.format_exc(),calcErrors={})
