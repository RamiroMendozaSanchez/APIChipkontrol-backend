from fastapi import FastAPI, HTTPException
import httpx
import motor.motor_asyncio
import asyncio
from datetime import datetime, timezone
import base64
import hashlib
import time
import platform
import psutil
from bson import ObjectId
from typing import List, Dict
from dotenv import load_dotenv
import os

# Cargar variables de entorno desde .env
load_dotenv()

app = FastAPI()

# Configuración de MongoDB
MONGO_URL = os.getenv("MONGO_URL")
client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URL)
db = client['wialon']
groups_collection = db['groups']
units_collection = db['units']
ids_units = db['ids_units']

# Configuración de API
BASE_URL = os.getenv("WIALON_BASE_URL")
TOKEN = os.getenv("WIALON_TOKEN")
API_SITRACK_URL = os.getenv("SITRACK_API_URL")

# Credenciales para la autorización en Sitrack
APPLICATION_NAME = os.getenv("SITRACK_APPLICATION_NAME")
SECRET_KEY = os.getenv("SITRACK_SECRET_KEY")


async def get_sid():
    """Obtiene el SID de autenticación desde la API de Wialon."""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{BASE_URL}?svc=token/login&params={{\"token\":\"{TOKEN}\"}}"
            )
            response.raise_for_status()
            return response.json().get("eid", None)
    except httpx.RequestError as e:
        print(f"Error de conexión al obtener SID: {e}")
        return None
    except Exception as e:
        print(f"Error inesperado al obtener SID: {e}")
        return None


async def fetch_group_ids():
    """
    Obtiene los IDs de las unidades desde la colección 'ids_units' de MongoDB
    y almacena toda la información en MongoDB.
    """
    sid = await get_sid()
    if not sid:
        print("No se pudo obtener el SID.")
        return

    units = await ids_units.find().to_list(100)  # Limitar a 100 documentos por consulta
    tasks = [get_unit_info(sid, unit.get("unit_id"), unit.get("loginCode")) for unit in units]
    results = await asyncio.gather(*tasks)

    valid_results = [data for data in results if data]
    if valid_results:
        await units_collection.delete_many({})
        await units_collection.insert_many(valid_results)
        print("Información completa de las unidades almacenada en MongoDB")


def format_time_iso8601(timestamp: int) -> str:
    """Formatea un timestamp en formato ISO 8601."""
    if not timestamp:
        return ""
    dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    return dt.strftime('%Y-%m-%dT%H:%M:%SZ')


async def get_unit_info(sid, unit_id, login_code):
    """Obtiene toda la información de una unidad específica desde Wialon."""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{BASE_URL}?svc=core/search_item&params={{\"id\":\"{unit_id}\",\"flags\":4611686018427387903}}&sid={sid}"
            )
            response.raise_for_status()
            data = response.json().get("item", {})
            if not data:
                return None

            return {
                "id": unit_id,
                "loginCode": str(login_code),
                "nm": str(data.get("nm", "")),
                "imei_no": data.get("uid", ""),
                "latitude": data.get("pos", {}).get("y", ""),
                "longitude": data.get("pos", {}).get("x", ""),
                "speed": data.get("pos", {}).get("s", ""),
                "angle": str(data.get("pos", {}).get("c", "")),
                "satellite": data.get("pos", {}).get("sc", ""),
                "time": format_time_iso8601(data.get("pos", {}).get("t", "")),
                "battery_voltage": str(data.get("lmsg", {}).get("p", {}).get("pwr_ext", "0")),
                "gps_validity": data.get("lmsg", {}).get("p", {}).get("valid", "V"),
                "server_status": "pending",
            }
    except httpx.RequestError as e:
        print(f"Error de conexión al obtener datos de la unidad {unit_id}: {e}")
        return None
    except Exception as e:
        print(f"Error inesperado al obtener datos de la unidad {unit_id}: {e}")
        return None


def create_authorization(application_name: str, secret_key: str) -> str:
    """Genera un encabezado de autorización."""
    try:
        unix_timestamp = int(time.time())
        data_to_hash = f"{application_name}{secret_key}{unix_timestamp}".encode('utf-8')
        md5_hash = hashlib.md5(data_to_hash).digest()
        hashed_value_base64 = base64.b64encode(md5_hash).decode('utf-8')
        return f'application="{application_name}",signature="{hashed_value_base64}",timestamp="{unix_timestamp}"'
    except Exception as e:
        print(f"Error generando el encabezado de autorización: {e}")
        return None

async def fetch_units_data():
    """Envía los datos de las unidades a la API de Sitrack y actualiza el estado en MongoDB."""
    start_time = asyncio.get_event_loop().time()

    # Generar el encabezado de autorización
    authorization_header = create_authorization(APPLICATION_NAME, SECRET_KEY)
    if not authorization_header:
        print("No se pudo generar el encabezado de autorización.")
        return

    # Configurar los encabezados de la solicitud
    headers = {
        "Authorization": f"SWSAuth {authorization_header}",
        "Content-Type": "application/json"
    }

    sid = await get_sid()
    if not sid:
        print("No se pudo obtener el SID.")
        return

    units = await units_collection.find().to_list(100)  # Limitar a 100 documentos por consulta
    update_tasks = []

    for unit in units:
        try:
            # Preparar los datos de la unidad para enviar a Sitrack
            unit_data = {
                "loginCode": unit["loginCode"],
                "reportDate": unit["time"],
                "reportType": "2",
                "latitude": unit["latitude"],
                "longitude": unit["longitude"],
                "gpsDop": 0.2,
                "speed": unit["speed"],
                "gpsSatellites": unit["satellite"],
            }

            # Enviar los datos a la API de Sitrack
            async with httpx.AsyncClient() as client:
                response = await client.put(API_SITRACK_URL, json=unit_data, headers=headers)
                response.raise_for_status()  # Lanza una excepción si la respuesta no es exitosa

                # Verificar la respuesta de la API

                status = response.json().get("response", {})
                print(response.json())
                if status == "Received":
                    update_tasks.append(
                        units_collection.update_one({"id": unit["id"]}, {"$set": {"server_status": status}})
                    )
                else:
                    update_tasks.append(
                        units_collection.update_one(
                            {"id": unit["id"]},
                            {"$set": {"server_status": "failed", "error_message": "Respuesta no válida de Sitrack"}}
                        )
                    )

        except httpx.RequestError as e:
            print(f"Error de conexión al enviar datos de la unidad {unit['id']}: {e}")
            update_tasks.append(
                units_collection.update_one(
                    {"id": unit["id"]},
                    {"$set": {"server_status": "failed", "error_message": f"Error de conexión: {e}"}}
                )
            )
        except Exception as e:
            print(f"Error inesperado al enviar datos de la unidad {unit['id']}: {e}")
            update_tasks.append(
                units_collection.update_one(
                    {"id": unit["id"]},
                    {"$set": {"server_status": "failed", "error_message": f"Error inesperado: {e}"}}
                )
            )

    # Ejecutar todas las actualizaciones en paralelo
    await asyncio.gather(*update_tasks)

    elapsed_time = asyncio.get_event_loop().time() - start_time
    print(f"fetch_units_data completado en {elapsed_time:.2f} segundos")
    print("Datos de unidades enviados y guardados en MongoDB")

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(schedule_tasks())


async def schedule_tasks():
    """Programa las tareas para ejecutarse secuencialmente cada 10 segundos."""
    while True:
        start_time = asyncio.get_event_loop().time()
        try:
            await fetch_group_ids()
            await fetch_units_data()
        except Exception as e:
            print(f"Error durante la ejecución de las tareas: {e}")
        elapsed_time = asyncio.get_event_loop().time() - start_time
        sleep_time = max(0, 10 - elapsed_time)
        print(f"Ciclo completado en {elapsed_time:.2f} segundos. Esperando {sleep_time:.2f} segundos...")
        await asyncio.sleep(sleep_time)


def serialize_mongo_document(doc):
    """Convierte un documento de MongoDB en un formato serializable."""
    if isinstance(doc, list):
        return [serialize_mongo_document(item) for item in doc]
    elif isinstance(doc, dict):
        return {key: serialize_mongo_document(value) for key, value in doc.items()}
    elif isinstance(doc, ObjectId):
        return str(doc)
    else:
        return doc


@app.get("/")
async def root():
    return {"message": "FastAPI está funcionando"}


@app.get("/server-info")
async def server_info():
    try:
        server_info = {
            "system": platform.system(),
            "node": platform.node(),
            "release": platform.release(),
            "version": platform.version(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "cpu_count": psutil.cpu_count(logical=True),
            "memory": {
                "total": f"{psutil.virtual_memory().total / (1024 ** 3):.2f} GB",
                "available": f"{psutil.virtual_memory().available / (1024 ** 3):.2f} GB",
                "used": f"{psutil.virtual_memory().used / (1024 ** 3):.2f} GB",
                "percent_used": psutil.virtual_memory().percent,
            },
            "disk_usage": {
                "total": f"{psutil.disk_usage('/').total / (1024 ** 3):.2f} GB",
                "used": f"{psutil.disk_usage('/').used / (1024 ** 3):.2f} GB",
                "free": f"{psutil.disk_usage('/').free / (1024 ** 3):.2f} GB",
                "percent_used": psutil.disk_usage('/').percent,
            },
        }
        return server_info
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener información del servidor: {e}")


@app.get("/mongo-status")
async def mongo_status():
    try:
        await client.admin.command("ping")
        return {"status": "MongoDB está funcionando correctamente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al conectar con MongoDB: {e}")


@app.get("/groups")
async def get_groups():
    try:
        groups = await groups_collection.find().to_list(100)  # Limitar a 100 documentos por consulta
        serialized_groups = serialize_mongo_document(groups)
        return {"groups": serialized_groups}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener grupos: {e}")


@app.get("/units")
async def get_units():
    try:
        units = await units_collection.find().to_list(100)  # Limitar a 100 documentos por consulta
        serialized_units = serialize_mongo_document(units)
        return {"units": serialized_units}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener unidades: {e}")