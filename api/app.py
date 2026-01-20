from typing import List, Dict, Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from bson import ObjectId


MONGO_URI = "mongodb://mongo:mongo@localhost:27017/?authSource=admin"
DB_NAME = "bigdata_analytics"


app = FastAPI(title="Big Data Analytics API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_db():
    client = MongoClient(MONGO_URI)
    return client[DB_NAME]


def serialize_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Convertit ObjectId en str pour JSON."""
    if "_id" in doc and isinstance(doc["_id"], ObjectId):
        doc["_id"] = str(doc["_id"])
    return doc


@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/kpis/{name}")
async def get_kpis(name: str, limit: int = 1000) -> List[Dict[str, Any]]:
    """
    Retourne un ensemble de KPIs depuis Mongo.

    `name` doit correspondre à une des clés utilisées dans le dashboard:
    - globaux, croissance, rfm, clv_detail, clv_pays,
      retention_global, retention_summary, produits, top_produits_ca
    """
    valid_names = {
        "globaux",
        "croissance",
        "rfm",
        "clv_detail",
        "clv_pays",
        "retention_global",
        "retention_summary",
        "produits",
        "top_produits_ca",
    }
    if name not in valid_names:
        raise HTTPException(status_code=404, detail="KPI inconnu")

    coll_name = f"kpis_{name}"
    db = get_db()
    cursor = db[coll_name].find({}, limit=limit)
    return [serialize_doc(doc) for doc in cursor]


@app.get("/facts/{name}")
async def get_facts(name: str, limit: int = 10000) -> List[Dict[str, Any]]:
    """
    Retourne une table de faits depuis Mongo.

    `name` parmi: ca_jour, ca_semaine, ca_mois, ca_heure, ca_pays
    """
    valid_names = {"ca_jour", "ca_semaine", "ca_mois", "ca_heure", "ca_pays"}
    if name not in valid_names:
        raise HTTPException(status_code=404, detail="Fact inconnue")

    coll_name = f"facts_{name}"
    db = get_db()
    cursor = db[coll_name].find({}, limit=limit)
    return [serialize_doc(doc) for doc in cursor]


@app.get("/analytics/{name}")
async def get_analytics(name: str, limit: int = 10000) -> List[Dict[str, Any]]:
    """
    Retourne une table d'analytics depuis Mongo.

    `name` parmi: saisonnalite_jour, saisonnalite_heure,
                  saisonnalite_mois, concentration_summary, cohortes_total
    """
    valid_names = {
        "saisonnalite_jour",
        "saisonnalite_heure",
        "saisonnalite_mois",
        "concentration_summary",
        "cohortes_total",
    }
    if name not in valid_names:
        raise HTTPException(status_code=404, detail="Analytics inconnues")

    coll_name = f"analytics_{name}"
    db = get_db()
    cursor = db[coll_name].find({}, limit=limit)
    return [serialize_doc(doc) for doc in cursor]


@app.get("/meta/last-refresh")
async def get_last_refresh() -> Dict[str, Any]:
    """Retourne les infos sur le dernier refresh Gold -> Mongo."""
    db = get_db()
    doc = db["metadata_refresh"].find_one(sort=[("refreshed_at", -1)])
    if not doc:
        raise HTTPException(status_code=404, detail="Aucun refresh trouvé")
    return serialize_doc(doc)


# Pour lancer localement:
# uvicorn api.app:app --reload --port 8000


