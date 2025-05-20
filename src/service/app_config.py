# app_config.py
import builtins


def load_allowed_builtins(file_path: str = "allowed_builtings.txt"):
    """
    Читает файл с именами разрешённых встроенных функций/типов и формирует словарь для безопасного eval.
    Каждая строка файла должна содержать имя встроенной функции или типа.
    """
    allowed = {}
    try:
        with open(file_path, "r", encoding="utf-8-sig") as f:
            for line in f:
                name = line.strip()
                if name:  # пропускаем пустые строки
                    if name in builtins.__dict__:
                        # Если это __import__, используем нашу функцию safe_import
                        if name == "__import__":
                            allowed[name] = safe_import
                        else:
                            allowed[name] = builtins.__dict__[name]
                    else:
                        print(f"Встроенная функция или тип '{name}' не найден.")
        return {"__builtins__": allowed}
    except Exception as e:
        raise Exception(f"Ошибка при чтении файла разрешённых функций: {e}")


def load_allowed_imports(file_path: str = "allowed_imports.txt"):
    """
    Читает файл с именами разрешённых модулей для импорта.
    Возвращает множество имён модулей.
    """
    allowed = set()
    try:
        with open(file_path, "r", encoding="utf-8-sig") as f:
            for line in f:
                name = line.strip()
                if name:  # пропускаем пустые строки
                    allowed.add(name)
        return allowed
    except Exception as e:
        raise Exception(f"Ошибка при чтении файла разрешённых импортов: {e}")


def safe_import(name, globals=None, locals=None, fromlist=(), level=0):
    """
    Разрешает импортировать модуль только если он есть в ALLOWED_IMPORTS.
    """
    if name not in ALLOWED_IMPORTS:
        raise ImportError(f"Импорт модуля '{name}' не разрешён")
    return __import__(name, globals, locals, fromlist, level)


ALLOWED_IMPORTS = load_allowed_imports()
SAFE_GLOBALS = load_allowed_builtins()