# routers/builtins.py
from fastapi import APIRouter, HTTPException
from ..app_config import load_allowed_builtins, load_allowed_imports, safe_import
from .. import app_config

router = APIRouter()


@router.get("/reload-builtins", summary="Перезагрузка разрешённых функций")
def reload_builtins():
    """
    Эндпоинт для перечитывания файла настроек и обновления SAFE_GLOBALS.
    """
    try:
        app_config.SAFE_GLOBALS = load_allowed_builtins()
        return {
            "message": "Разрешённые функции успешно перезагружены.",
            "allowed": list(app_config.SAFE_GLOBALS["__builtins__"].keys())
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/reload-imports")  # Новый эндпоинт
def reload_imports():
    """Перезагружает список разрешённых модулей из allowed_imports.txt"""
    try:
        app_config.ALLOWED_IMPORTS = load_allowed_imports()
        # Обновляем __import__ в SAFE_GLOBALS, если он есть
        if "__import__" in app_config.SAFE_GLOBALS["__builtins__"]:
            app_config.SAFE_GLOBALS["__builtins__"]["__import__"] = safe_import
        return {
            "message": "Разрешённые импорты обновлены.",
            "allowed_imports": list(app_config.ALLOWED_IMPORTS)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))