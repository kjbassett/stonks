import asyncio

from app import create_app

app = asyncio.run(create_app())

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, single_process=True)
