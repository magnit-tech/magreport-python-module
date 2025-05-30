# Документация проекта
##magreport-python-module
Внешний Python-модуль для системы отчётности и BI Магрепорт

# Установка
Требуется выполнить
```
  pip install -r requirements.txt
```

# Запуск

```
  fastapi dev main.py
```
## Обзор
Данный проект реализует REST API с использованием FastAPI для безопасного выполнения пользовательских вычислений над данными из Avro-файлов. Проект предоставляет два основных эндпоинта:
- **/reload-builtins** – для динамической перезагрузки списка разрешённых встроенных функций, используемых при выполнении пользовательских формул.
- **/transform** – для преобразования данных из Avro-файлов с расчётом новых столбцов по заданным формулам.
- **/reload-imports** – для динамической перезагрузки списка разрешённых импортов, используемых при выполнении пользовательских формул.


## Структура проекта

### main.py
- **Назначение:** Точка входа в приложение.
- **Функциональность:** 
  - Инициализация FastAPI приложения.
  - Подключение маршрутов из модулей `builtins` и `transform`.
  - Запуск сервера через uvicorn.

### app_config.py
- **Назначение:** Загрузка и управление конфигурационными параметрами для безопасного выполнения выражений.
- **Основные функции:**
  - `load_allowed_builtins`: Читает файл `allowed_builtings.txt` и формирует словарь разрешённых встроенных функций и типов для использования в `eval`.
  - `load_allowed_imports`: Читает файл `allowed_imports.txt` и формирует список разрешённых модулей для импорта.
  - `safe_import`: Обеспечивает безопасный импорт модулей, используя список разрешённых импортов.
- **Глобальные переменные:**
  - `ALLOWED_IMPORTS` – набор модулей, разрешённых для импорта.
  - `SAFE_GLOBALS` – словарь разрешённых встроенных функций, используемых при вычислениях.

### app_models.py
- **Назначение:** Определение моделей данных с использованием Pydantic.
- **Модели:**
  - `CalcColumn`: Описывает вычисляемый столбец (имя столбца и формула вычисления).
  - `TransformRequest`: Модель запроса для эндпоинта `/transform`, содержащая имя входного файла, имя выходного файла и список вычисляемых столбцов.
  - `TransformResponse`: Модель ответа, содержащая имя выходного файла, код ошибки, сообщение об ошибке и подробную информацию о возникших ошибках вычислений.

### routers/builtins.py
- **Назначение:** Реализация эндпоинта для обновления списка разрешённых встроенных функций.
- **Эндпоинт:**
  - **GET /reload-builtins:** Перечитывает файл настроек (через `load_allowed_builtins`) и обновляет глобальную переменную `SAFE_GLOBALS`. Возвращает сообщение об успешном обновлении и список разрешённых функций.

### routers/transform.py
- **Назначение:** Обработка данных из Avro-файлов с вычислением дополнительных столбцов.
- **Эндпоинт:**
  - **POST /transform:**
    - Принимает объект запроса, содержащий:
      - `inputFileName`: имя входного Avro-файла.
      - `outputFileName`: имя выходного Avro-файла.
      - `calcColumns`: список вычисляемых столбцов с их формулами.
    - **Процесс выполнения:**
      - **Проверка уникальности имён:** Проверяется отсутствие дублирования имён вычисляемых столбцов, а также пересечение с исходными именами столбцов из Avro-схемы.
      - **Компиляция формул:** Формулы для вычисляемых столбцов компилируются. При ошибке компиляции возвращается сообщение об ошибке.
      - **Вычисление значений:** Для каждой записи Avro-файла вычисляются новые значения по заданным формулам. Если вычисление для записи завершается ошибкой, значение столбца устанавливается в `null`, а ошибка учитывается.
      - **Обновление схемы:** В исходную схему добавляются новые вычисляемые столбцы.
      - **Запись результата:** Результаты записываются в выходной Avro-файл с использованием кодека Snappy.
    - **Возвращаемые данные:** Объект `TransformResponse`, содержащий имя выходного файла, код и сообщение об ошибке (если возникли проблемы), а также детальную информацию по ошибкам для каждого вычисляемого столбца.

## Конфигурация
Проект использует два конфигурационных файла для обеспечения безопасности вычислений:
- **allowed_builtings.txt:** Содержит список имён встроенных функций/типов, разрешённых для использования в вычислениях через `eval`.
- **allowed_imports.txt:** Содержит список модулей, разрешённых для импорта через функцию `safe_import`.

Эти файлы позволяют контролировать, какие функции и модули могут использоваться при выполнении пользовательских формул, минимизируя риск выполнения нежелательного или вредоносного кода.

