import json
import asyncio
import websockets
from useful_funcs import get_key


# Function to map short keys to full words
def map_keys(data):
    key_mapping = {
        "ev": "event_type",
        "sym": "symbol",
        "v": "volume",
        "av": "accumulated_volume",
        "op": "official_opening_price",
        "vw": "volume_weighted_average_price",
        "o": "opening_price",
        "c": "closing_price",
        "h": "highest_price",
        "l": "lowest_price",
        "a": "average_price",
        "z": "average_trade_size",
        "s": "start_timestamp",
        "e": "end_timestamp"
    }
    return {key_mapping.get(k, k): v for k, v in data.items()}


# Function to process and store data
async def process_and_store_data(data, db):
    mapped_data = map_keys(data)
    await db.insert("your_table_name", mapped_data)  # Replace 'your_table_name' with your actual table name


# Async function for WebSocket client
async def main(db, companies=None):
    if not companies:
        companies = '*'

    uri = "wss://delayed.polygon.io/stocks"
    api_key = get_key(['polygon_io'])
    async with websockets.connect(uri) as ws:
        # Send authentication message
        auth_data = {"action": "auth", "params": api_key}
        await ws.send(json.dumps(auth_data))

        # Check authentication response
        response = await ws.recv()
        response_data = json.loads(response)
        if response_data[0]["status"] != "auth_success":
            return

        # Subscribe to stream with data aggregated by minute
        # TODO this won't work if companies provided
        subscribe_message = {"action": "subscribe", "params": f"AM.{companies}"}
        await ws.send(json.dumps(subscribe_message))
        # Process incoming messages
        while True:
            try:
                message = await ws.recv()
                data = json.loads(message)
                print(data)
                await process_and_store_data(data, db)
            except asyncio.CancelledError:
                # Check for stop event periodically
                continue