import os
import boto3
import joblib
import numpy as np
import pandas as pd
from io import BytesIO
from scipy.special import erf


# module variables

BUCKET = "ds4a-co6-t107-datalake"
aws = boto3.Session()
s3 = aws.client("s3")
models = {} # init empty, lazy loading of models

# load functions

def load_model(var: str):
    key = f"assets/models/{var}-brm.joblib"
    with BytesIO() as f:
        s3.download_fileobj(Bucket=BUCKET, Key=key, Fileobj=f)
        f.seek(0)
        model = joblib.load(f)
    return model


def load_df(key: str):
    obj = s3.get_object(Bucket=BUCKET, Key=key).get("Body")
    return pd.read_csv(obj)

# prediction functions

def compute_probs_from_scores(scores):
    pre_erf_scores = scores
    std = np.std(scores)
    if len(scores) >= 2 and std > 0.0:
        pre_erf_scores = (scores - np.mean(scores)) / (np.std(scores) * np.sqrt(2))
    erf_score = erf(pre_erf_scores)
    probs = erf_score.clip(0, 1.0).ravel()
    return probs


def compute_prob(dpto, municipio, gender, var_name, var_value, model_name="brm"):
    """
    This function is called from the data selected in the frontend and returns the
    probability associated.

    Usage examples:
    VAR_NAME="CAUSE"
    MODEL_NAME="brm"
    model = load_model(f"OUTPUT/{VAR_NAME}-{MODEL_NAME}.pickle")
    print(compute_prob("BOGOTA", "BOGOTA", "MASCULINO", VAR_NAME, "Desamor", model, MODEL_NAME))
    print(compute_prob("ANTIOQUIA", "MEDELLIN", "MASCULINO", VAR_NAME, "Bullying", model, MODEL_NAME))
    print(compute_prob("CAUCA", "SUAREZ", "MASCULINO", VAR_NAME, "Acceso a armas de fuego", model, MODEL_NAME))
    """
    # load template dataframe with right colnames and zero values
    df = load_df(f"assets/templates/{var_name}_{model_name}_TEMPLATE.csv").head(10)
    
    if var_name not in models:
        models[var_name] = load_model(var_name)
    
    model = models[var_name]
    # Setup 1 on dpto municipio gender var_name var_value
    df.loc[
        0,
        [
            f"DPTO_{dpto}",
            f"MUNICIPIO_{municipio}",
            f"GENDER_{gender}",
            f"{var_name}_{var_value}",
        ],
    ] = 1

    # compute the probability
    prob = -1.0
    if model_name == "brm":
        decision_scores = np.array(model.score_samples(df))
        threshold = 0.98
        decision_scores[decision_scores >= threshold] = 1.8
        prob = compute_probs_from_scores(decision_scores)
    return prob[0]
