from fastapi import FastAPI, WebSocket, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import json
from kafka import KafkaConsumer
from threading import Thread

app = FastAPI(title="Agent Management API")

# CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

registered_agents = {}

@app.post("/agents/register")
def register_agent(agent: dict):
    name = agent.get("name")
    description = agent.get("description")
    topic = agent.get("topic")

    if not name or not topic:
        raise HTTPException(status_code=400, detail="Missing name or topic")
    registered_agents[name] = {
        "description": description,
        "topic": topic
    }
    return {"message": f"Agent '{name}' registered successfully."}

@app.get("/agents/list")
def list_agents():
    return {"agents": registered_agents}

@app.websocket("/alerts")
async def alerts_ws(websocket: WebSocket):
    await websocket.accept()

    def kafka_listener():
        consumer = KafkaConsumer(
            "anomaly-alerts",
            bootstrap_servers="localhost:9092",
            value_deserializer=lambda m: json.loads(m.decode("utf-8"))
        )
        for message in consumer:
            asyncio.run(websocket.send_json(message.value))

    Thread(target=kafka_listener, daemon=True).start()

    try:
        while True:
            await asyncio.sleep(1)
    except Exception:
        await websocket.close()
