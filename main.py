from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn

# Создаем приложение FastAPI
app = FastAPI()

class NameRequest(BaseModel):
    name: str

# Определяем модель данных для входного запроса
class GreetResponse(BaseModel):
    message: str


# Определяем эндпоинт для обработки запросов
@app.post("/Приветствие",
          summary="Приветствие пользователя",  # Краткое описание
          description="Эта функция принимает имя пользователя и возвращает приветственное сообщение.",
          # Полное описание
          response_model=GreetResponse,  # Модель возвращаемого ответа
          tags=["Greetings"])  # Группировка маршрута)
def greet(request: NameRequest):
    return {"message": f"Hello {request.name}!"}


"""
if __name__ == "__main__":
    uvicorn.run("main:app", port=8001, reload=True)   # еще один вариант запуска
"""

# Для запуска сервиса используйте команду:
# uvicorn main:app --reload
