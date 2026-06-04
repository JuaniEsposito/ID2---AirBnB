import os
import re
from pymongo import MongoClient
from dotenv import load_dotenv


load_dotenv()


def infer_tipo_propiedad(documento):
    titulo = (documento.get("titulo") or "").strip().lower()
    if re.search(r"departamento|depto|dpto|apto", titulo):
        return "Departamento"
    if re.search(r"caba[ñn]a", titulo):
        return "Cabaña"
    if re.search(r"habitaci[oó]n|\bhab\b", titulo):
        return "Habitación"
    if re.search(r"casa", titulo):
        return "Casa"
    return documento.get("tipo_propiedad") or "Alojamiento"


def main():
    uri = os.getenv("MONGO_URI")
    database_name = os.getenv("MONGO_DB_NAME", "ID2")
    collection_name = os.getenv("MONGO_COLLECTION_NAME", "propiedades")

    if not uri:
        raise SystemExit("Falta MONGO_URI en .env")

    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    collection = client[database_name][collection_name]

    total = 0
    modified = 0

    for documento in collection.find({}):
        total += 1
        updates = {}

        tipo_propiedad = infer_tipo_propiedad(documento)
        if documento.get("tipo_propiedad") != tipo_propiedad:
            updates["tipo_propiedad"] = tipo_propiedad

        if "id_moneda" not in documento and documento.get("moneda"):
            updates["id_moneda"] = documento["moneda"]

        if "metadata_anfitrion" not in documento:
            anfitrion_id = documento.get("anfitrion_id")
            if anfitrion_id is not None:
                updates["metadata_anfitrion"] = {
                    "anfitrion_id": anfitrion_id,
                    "nombre": f"Anfitrión #{anfitrion_id}",
                }

        if updates:
            collection.update_one({"_id": documento["_id"]}, {"$set": updates})
            modified += 1

    print(f"Documentos revisados: {total}")
    print(f"Documentos actualizados: {modified}")


if __name__ == "__main__":
    main()