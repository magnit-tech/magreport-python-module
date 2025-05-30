# routers/transform.py

from fastapi import APIRouter
from ..app_models import TransformRequest, TransformResponse, CalcStatus
import fastavro
from .. import app_config
import re
from collections import deque
import time
import logging

# создаём роутер и логгер
router = APIRouter()
logger = logging.getLogger("transform")
logger.setLevel(logging.INFO)

# приоритеты типов для выбора Avro-типа
_INT, _DOUBLE, _STRING = 1, 2, 3

def determine_priority(v):
    """Определяем приоритет по типу значения."""
    if isinstance(v, int):
        return _INT
    if isinstance(v, float):
        return _DOUBLE
    return _STRING

def aggregate_priority(cur, new):
    """Агрегируем максимальный приоритет среди значений."""
    if cur is None:
        return new
    return new if new and new > cur else cur

def extract_dependencies(formula: str) -> list[str]:
    """
    Извлекаем из формулы имена столбцов,
    на которые она ссылается: col['name'].
    """
    return re.findall(r"col\[['\"]([^'\"]+)['\"]\]", formula)

def build_dependency_graph(cols: list) -> dict[str, list[str]]:
    """
    Строим граф зависимостей: для каждого
    производного столбца список входящих ссылок.
    """
    names = {c.columnName for c in cols}
    return {
        c.columnName: [
            dep for dep in extract_dependencies(c.columnFormula)
            if dep in names
        ]
        for c in cols
    }

def topological_sort(graph: dict[str, list[str]]) -> tuple[list[str], set[str]]:
    """
    Топологическая сортировка графа.
    Возвращает (order, cyclic), где
      - order — вычисляемые без циклов
      - cyclic — имена, оставшиеся в цикле
    """
    in_deg = {u: len(deps) for u, deps in graph.items()}
    rev = {u: [] for u in graph}
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

@router.post("/transform", response_model=TransformResponse, tags=["Data Transformation"])
def transform(request: TransformRequest):
    """
    Основной эндпоинт:
    1) Проверка уникальности имён
    2) Построение графа зависимостей + компиляция формул
    3) Чтение входного Avro
    4) Вычисление новых столбцов с безопасным eval
    5) Обновление схемы и запись результата
    6) Формирование массива статусов расчёта
    """
    t0 = time.time()
    names = [c.columnName for c in request.calcColumns]

    # 1) Уникальность имён
    if len(names) != len(set(names)):
        logger.info("[transform] [transform] duplicate column names in request")
        return TransformResponse(
            outputFileName="",
            errorCode=1,
            errorMessage="duplicate column names",
            calcStatuses=[]
        )

    # 2) Граф зависимостей + топосортировка
    graph = build_dependency_graph(request.calcColumns)
    order, cyclic = topological_sort(graph)

    # Компиляция формул в байткод
    code_map = {
        c.columnName: compile(c.columnFormula, "<string>", "eval")
        for c in request.calcColumns
    }
    # Вычислять в порядке topological + циклические в конце
    compiled = [(n, code_map[n]) for n in order + [n for n in names if n in cyclic]]

    # Инициализация счётчиков ошибок и приоритетов типов
    errs = {n: 0 for n in names}
    details = {n: None for n in names}
    prio = {n: None for n in names}

    try:
        # 3) Чтение входного Avro
        t_read_start = time.time()
        with open(request.inputFileName, "rb") as f:
            reader = fastavro.reader(f)
            data = list(reader)
            schema = reader.schema
        t_read = time.time() - t_read_start
        logger.info(f"[transform] [transform] read {len(data)} records in {t_read:.3f}s")


        # Проверка, что новые столбцы не дублируют существующие
        existing = {fld["name"] for fld in schema["fields"]}
        for n in names:
            if n in existing:
                logger.info(f"[transform] [transform] column '{n}' exists in schema")
                return TransformResponse(
                    outputFileName="",
                    errorCode=1,
                    errorMessage=f"column '{n}' exists",
                    calcStatuses=[]
                )

        # 4) Вычисление формул по записям
        t_calc_start = time.time()
        for idx, rec in enumerate(data, start=1):
            loc = {"col": rec}
            for n, code in compiled:
                if n in cyclic:
                    # при цикле сразу None
                    rec[n] = None
                    errs[n] += 1
                    continue
                try:
                    res = eval(code, app_config.SAFE_GLOBALS, loc)
                    prio[n] = aggregate_priority(prio[n], determine_priority(res))
                    # записываем результат (int/float/str или str(res))
                    rec[n] = res if isinstance(res, (int, float, str)) else str(res)
                except Exception as e:
                    rec[n] = None
                    # сохраняем первую ошибку
                    if errs[n] == 0:
                        details[n] = (idx, str(e))
                    errs[n] += 1
        t_calc = time.time() - t_calc_start
        logger.info(f"[transform] [transform] computed formulas in {t_calc:.3f}s")

        # 5) Обновление схемы и запись выходного Avro
        t_write_start = time.time()
        AVRO_MAP = {
            _INT:    ("int",    "INT"),
            _DOUBLE: ("float",  "DOUBLE"),
            _STRING: ("string", "STRING"),
        }
        for n in names:
            p = prio[n] or _STRING
            avro_t, _ = AVRO_MAP[p]
            schema["fields"].append({
                "name":    n,
                "type":    ["null", avro_t],
                "default": None
            })

        with open(request.outputFileName, "wb") as f:
            fastavro.writer(f, schema, data, codec="snappy")
        t_write = time.time() - t_write_start
        logger.info(f"[transform] [transform] wrote output in {t_write:.3f}s")

        # 6) Формируем статус расчёта по каждому столбцу
        statuses = []
        for n in names:
            if errs[n] == 0:
                # 1 — успех
                statuses.append(CalcStatus(status=1, message="success"))
            else:
                idx, msg = details[n]
                statuses.append(CalcStatus(
                    status=0,
                    message=f"Ошибка в {errs[n]} записях; первая на строке {idx}: {msg}"
                ))

        # Общий лог по всем этапам
        t_total = time.time() - t0
        logger.info(f"[transform] [transform] total time {t_total:.3f}s")

        return TransformResponse(
            outputFileName=request.outputFileName,
            errorCode=0,
            errorMessage="",
            calcStatuses=statuses
        )

    except Exception as e:
        # при аварии логируем ошибку с трассировкой
        t_exc = time.time() - t0
        logger.error(f"[transform] [transform] failed after {t_exc:.3f}s: {e}", exc_info=True)
        return TransformResponse(
            outputFileName="",
            errorCode=1,
            errorMessage=f"Internal error: {e}",
            calcStatuses=[]
        )
