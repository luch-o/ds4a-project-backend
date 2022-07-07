from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Union
from utils import compute_prob
from unidecode import unidecode

router = APIRouter()

VAR_NAMES = [
    "age_group",
    "cause",
    "marital_status",
    "scholarship",
    "vulnerability_factor",
]


class ModelInput(BaseModel):
    department: str
    municipality: str
    gender: str
    age_group: Union[str, None] = None
    cause: Union[str, None] = None
    marital_status: Union[str, None] = None
    scholarship: Union[str, None] = None
    vulnerability_factor: Union[str, None] = None


@router.post("/")
def predict(record: ModelInput):
    """
    Return a list of predicted probabilities depending on the models used and
    a global probability computed as the mean of the probailities
    """
    record_dict = record.dict()
    department = unidecode(record.department.upper())
    municipality = unidecode(record.municipality.upper())
    gender = record.gender.upper()

    try:
        probs = [
            {
                "var": var,
                "prob": compute_prob(
                    department, municipality, gender, var.upper(), record_dict[var]
                ),
            }
            for var in VAR_NAMES
            if record_dict.get(var) is not None
        ]
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    if not probs:
        raise HTTPException(
            status_code=400, detail=f"At least one of {VAR_NAMES} must be specified"
        )

    global_prob = sum([p["prob"] for p in probs]) / len(probs)

    return {"probabilities": probs, "global": global_prob}
