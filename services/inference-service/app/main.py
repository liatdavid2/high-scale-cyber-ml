import os, time, threading
from collections import deque
from pathlib import Path
import mlflow, mlflow.sklearn
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

STATIC_DIR=Path(__file__).resolve().parent/"static"
TRACKING=os.getenv("MLFLOW_TRACKING_URI","http://mlflow:5000")
MODEL_NAME=os.getenv("MLFLOW_REGISTERED_MODEL_NAME","cyber-intrusion-model")
MODEL_ALIAS=os.getenv("MLFLOW_MODEL_ALIAS","champion")
MODEL_URI=f"models:/{MODEL_NAME}@{MODEL_ALIAS}"
FEATURES=["dur","rate","total_bytes","total_packets","byte_ratio_src_dst","packet_ratio_src_dst","bytes_per_packet"]

app=FastAPI(title="Inference Service",version="1.0.0")
app.mount("/static",StaticFiles(directory=STATIC_DIR),name="static")
mlflow.set_tracking_uri(TRACKING)

model=None; model_error=None; model_loaded_at=None; lock=threading.Lock()
telemetry=deque(maxlen=10000); latencies=deque(maxlen=5000)
counters={"requests":0,"success":0,"errors":0,"schema_errors":0}
started_at=time.time()

class PredictRequest(BaseModel):
    dur: float
    rate: float
    total_bytes: float
    total_packets: float
    byte_ratio_src_dst: float
    packet_ratio_src_dst: float
    bytes_per_packet: float

def pct(values,q):
    if not values:return 0.0
    a=sorted(values); i=min(int((len(a)-1)*q),len(a)-1)
    return round(float(a[i]),3)

def load_model(force=False):
    global model, model_error, model_loaded_at
    with lock:
        if model is not None and not force:return
        try:
            model=mlflow.sklearn.load_model(MODEL_URI)
            model_loaded_at=time.time(); model_error=None
        except Exception as e:
            model=None; model_error=str(e); raise

def get_model():
    if model is None: load_model()
    return model

@app.on_event("startup")
def startup():
    try: load_model()
    except Exception: pass

@app.get("/")
def home(): return FileResponse(STATIC_DIR/"index.html")

@app.get("/health")
def health():
    return {"status":"ok" if model is not None else "degraded","model_uri":MODEL_URI,"model_loaded":model is not None,"model_error":model_error}

@app.post("/api/predict")
def predict(req:PredictRequest):
    counters["requests"]+=1
    t=time.perf_counter()
    try:
        d=req.model_dump()
        row=pd.DataFrame([[d[f] for f in FEATURES]],columns=FEATURES)
        mdl=get_model()
        pred=int(mdl.predict(row)[0])
        prob=float(mdl.predict_proba(row)[0][1]) if hasattr(mdl,"predict_proba") else float(pred)
        ms=(time.perf_counter()-t)*1000
        latencies.append(ms); counters["success"]+=1
        telemetry.append({"ts":time.time(),"prediction":pred,"probability":prob,"latency_ms":ms,"features":d})
        return {"prediction":pred,"label":"ATTACK" if pred else "NORMAL","probability":round(prob,6),"latency_ms":round(ms,3),"model_uri":MODEL_URI}
    except Exception as e:
        counters["errors"]+=1
        raise HTTPException(400,str(e))

@app.post("/api/reload-model")
def reload():
    try:
        load_model(True); return {"status":"reloaded","model_uri":MODEL_URI}
    except Exception as e: raise HTTPException(500,str(e))

@app.get("/api/evaluation")
def evaluation():
    elapsed=max(time.time()-started_at,1); total=counters["requests"]
    return {"model_uri":MODEL_URI,"model_loaded":model is not None,"model_loaded_at":model_loaded_at,"model_error":model_error,
            "requests":total,"success":counters["success"],"errors":counters["errors"],"schema_errors":counters["schema_errors"],
            "error_rate":round(counters["errors"]/total,6) if total else 0,"throughput_rps":round(counters["success"]/elapsed,2),
            "p50_ms":pct(latencies,.5),"p95_ms":pct(latencies,.95),"p99_ms":pct(latencies,.99),"uptime_sec":int(elapsed)}

@app.get("/api/telemetry")
def get_telemetry(limit:int=1000):
    limit=min(max(limit,1),10000)
    return {"count":min(limit,len(telemetry)),"items":list(telemetry)[-limit:],"evaluation":evaluation()}
