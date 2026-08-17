import os, time
from pathlib import Path
import numpy as np, pandas as pd, requests
from scipy.stats import ks_2samp
from sklearn.metrics import average_precision_score, roc_auc_score, f1_score, recall_score, precision_score
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

STATIC_DIR=Path(__file__).resolve().parent/"static"
DATA_DIR=Path(os.getenv("DATA_DIR","/shared/data/raw"))
INFERENCE_URL=os.getenv("INFERENCE_URL","http://inference-service:8000").rstrip("/")
TEST_NAMES=["UNSW_NB15_testing-set.csv","UNSW_NB15_testing_set.csv"]
FEATURES=["dur","rate","total_bytes","total_packets","byte_ratio_src_dst","packet_ratio_src_dst","bytes_per_packet"]

app=FastAPI(title="Monitoring Service",version="1.0.0")
app.mount("/static",StaticFiles(directory=STATIC_DIR),name="static")

def find_test():
    for n in TEST_NAMES:
        p=DATA_DIR/n
        if p.exists(): return p
    raise FileNotFoundError("UNSW-NB15 test CSV not found")

def build_features(df):
    d=df[["dur","rate","sbytes","dbytes","spkts","dpkts","label"]].copy()
    for c in d.columns:d[c]=pd.to_numeric(d[c],errors="coerce").fillna(0)
    tb=d["sbytes"]+d["dbytes"];tp=d["spkts"]+d["dpkts"]
    X=pd.DataFrame({"dur":d["dur"],"rate":d["rate"],"total_bytes":tb,"total_packets":tp,
                    "byte_ratio_src_dst":d["sbytes"]/(d["dbytes"]+1),"packet_ratio_src_dst":d["spkts"]/(d["dpkts"]+1),
                    "bytes_per_packet":tb/(tp+1)})
    return X,d["label"].astype(int)

def psi(e,a,bins=10):
    e=np.asarray(e,float);a=np.asarray(a,float)
    if len(e)<2 or len(a)<2:return 0.0
    edges=np.unique(np.quantile(e,np.linspace(0,1,bins+1)))
    if len(edges)<3:return 0.0
    ec,_=np.histogram(e,bins=edges);ac,_=np.histogram(a,bins=edges)
    ep=np.clip(ec/max(ec.sum(),1),1e-6,None);ap=np.clip(ac/max(ac.sum(),1),1e-6,None)
    return float(np.sum((ap-ep)*np.log(ap/ep)))

def telemetry(limit=2000):
    r=requests.get(f"{INFERENCE_URL}/api/telemetry",params={"limit":limit},timeout=15);r.raise_for_status();return r.json()

@app.get("/")
def home():return FileResponse(STATIC_DIR/"index.html")

@app.get("/api/monitor")
def monitor():
    try:t=telemetry()
    except Exception as e:raise HTTPException(503,str(e))
    items=t["items"];ev=t["evaluation"]
    if not items:return {"evaluation":ev,"sample_count":0,"prediction_attack_rate":0,"avg_confidence":0,"low_confidence_rate":0,"drift":[],"alerts":["No inference telemetry yet."]}
    prod=pd.DataFrame([x["features"] for x in items]);probs=np.array([x["probability"] for x in items]);preds=np.array([x["prediction"] for x in items])
    base_df=pd.read_csv(find_test());base_df=base_df.sample(n=min(5000,len(base_df)),random_state=42);base,_=build_features(base_df)
    drift=[];alerts=[]
    for f in FEATURES:
        p=psi(base[f],prod[f]);ks=float(ks_2samp(base[f],prod[f]).statistic)
        status="HIGH" if p>=.25 else "MEDIUM" if p>=.10 else "LOW"
        drift.append({"feature":f,"psi":round(p,4),"ks":round(ks,4),"status":status})
        if status=="HIGH":alerts.append("High drift: "+f)
    if ev["p95_ms"]>50:alerts.append("Inference p95 latency above 50 ms")
    if ev["error_rate"]>.01:alerts.append("Inference error rate above 1%")
    conf=np.maximum(probs,1-probs)
    return {"evaluation":ev,"sample_count":len(items),"prediction_attack_rate":round(float(preds.mean()),4),
            "avg_confidence":round(float(conf.mean()),4),"low_confidence_rate":round(float((conf<.60).mean()),4),
            "drift":drift,"alerts":alerts}

@app.post("/api/quality-check")
def quality_check():
    df=pd.read_csv(find_test()).sample(n=2000,random_state=42);X,y=build_features(df)
    probs=[];preds=[];errors=0
    for _,row in X.iterrows():
        try:
            r=requests.post(f"{INFERENCE_URL}/api/predict",json=row.to_dict(),timeout=10);r.raise_for_status();d=r.json()
            probs.append(d["probability"]);preds.append(d["prediction"])
        except Exception:
            errors+=1;probs.append(0.0);preds.append(0)
    probs=np.asarray(probs);preds=np.asarray(preds);y=np.asarray(y)
    return {"rows":len(y),"errors":errors,"pr_auc":round(float(average_precision_score(y,probs)),4),
            "roc_auc":round(float(roc_auc_score(y,probs)),4),"f1":round(float(f1_score(y,preds,zero_division=0)),4),
            "recall":round(float(recall_score(y,preds,zero_division=0)),4),
            "precision":round(float(precision_score(y,preds,zero_division=0)),4),"checked_at":int(time.time())}
