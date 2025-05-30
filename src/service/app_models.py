# app_models.py
from pydantic import BaseModel
from typing import List

class CalcColumn(BaseModel):
    columnName: str
    columnFormula: str

class CalcStatus(BaseModel):
    status: int   # 0 — ошибка, 1 — успех
    message: str  # текст ошибки или "success"

class TransformRequest(BaseModel):
    inputFileName: str
    outputFileName: str
    calcColumns: List[CalcColumn]

class TransformResponse(BaseModel):
    outputFileName: str
    errorCode: int
    errorMessage: str
    calcStatuses: List[CalcStatus]
