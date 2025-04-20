# app_models.py
from pydantic import BaseModel
from typing import List, Dict


class CalcColumn(BaseModel):
    columnName: str
    columnFormula: str


class TransformRequest(BaseModel):
    inputFileName: str
    outputFileName: str
    calcColumns: List[CalcColumn]


class TransformResponse(BaseModel):
    outputFileName: str
    errorCode: int
    errorMessage: str
    calcErrors: Dict[str, str]
