import json
import os
from pathlib import Path
from app.store import upsert_documents

KNOWLEDGE_ROOT = Path(os.getenv("KNOWLEDGE_ROOT", "/data/knowledge"))
MITRE_FILE = KNOWLEDGE_ROOT / "mitre" / "enterprise-attack.json"
CAPEC_CANDIDATES = [
    KNOWLEDGE_ROOT / "capec" / "capec.json",
    KNOWLEDGE_ROOT / "capec" / "capec-stix.json",
    KNOWLEDGE_ROOT / "capec" / "stix-capec.json",
]

def _compact(parts):
    return " | ".join(str(x).strip() for x in parts if x not in (None, "", [], {}))

def _mitre_documents(path: Path):
    raw = json.loads(path.read_text(encoding="utf-8"))
    docs = []
    for obj in raw.get("objects", []):
        if obj.get("type") not in {"attack-pattern", "course-of-action", "intrusion-set", "malware", "tool"}:
            continue
        if obj.get("revoked") or obj.get("x_mitre_deprecated"):
            continue

        ext_id = None
        for ref in obj.get("external_references", []):
            if ref.get("external_id"):
                ext_id = ref["external_id"]
                break

        name = obj.get("name", "")
        description = obj.get("description", "")
        text = _compact([
            f"MITRE ATT&CK {obj.get('type')}",
            f"ID: {ext_id}" if ext_id else None,
            f"Name: {name}" if name else None,
            description,
        ])
        if text:
            docs.append({
                "source": "MITRE ATT&CK",
                "id": ext_id or obj.get("id"),
                "name": name,
                "text": text,
            })
    return docs

def _capec_documents(path: Path):
    raw = json.loads(path.read_text(encoding="utf-8"))
    objects = raw.get("objects", []) if isinstance(raw, dict) else raw
    docs = []

    for obj in objects:
        if not isinstance(obj, dict):
            continue
        if obj.get("type") != "attack-pattern":
            continue
        if obj.get("revoked"):
            continue

        ext_id = None
        for ref in obj.get("external_references", []):
            candidate = ref.get("external_id")
            if candidate and str(candidate).upper().startswith("CAPEC"):
                ext_id = candidate
                break

        name = obj.get("name", "")
        description = obj.get("description", "")
        text = _compact([
            "CAPEC attack pattern",
            f"ID: {ext_id}" if ext_id else None,
            f"Name: {name}" if name else None,
            description,
        ])
        if text:
            docs.append({
                "source": "CAPEC",
                "id": ext_id or obj.get("id"),
                "name": name,
                "text": text,
            })
    return docs

def ingest_all():
    docs = []
    loaded = []
    missing = []

    if MITRE_FILE.exists():
        mitre = _mitre_documents(MITRE_FILE)
        docs.extend(mitre)
        loaded.append({"source": "MITRE ATT&CK", "file": str(MITRE_FILE), "documents": len(mitre)})
    else:
        missing.append(str(MITRE_FILE))

    capec_file = next((p for p in CAPEC_CANDIDATES if p.exists()), None)
    if capec_file:
        capec = _capec_documents(capec_file)
        docs.extend(capec)
        loaded.append({"source": "CAPEC", "file": str(capec_file), "documents": len(capec)})
    else:
        missing.append("CAPEC: " + " OR ".join(map(str, CAPEC_CANDIDATES)))

    if not docs:
        raise FileNotFoundError(
            "No knowledge files found. Expected MITRE/CAPEC under /data/knowledge."
        )

    indexed = upsert_documents(docs)
    return {
        "status": "ok",
        "collection": os.getenv("QDRANT_COLLECTION", "cyber_knowledge"),
        "indexed": indexed,
        "loaded": loaded,
        "missing": missing,
    }
