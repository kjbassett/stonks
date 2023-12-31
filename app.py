import asyncio
import importlib
import os

from sanic import Sanic, response
from sanic_jinja2 import SanicJinja2


def create_app(db):
    app = Sanic(__name__)

    @app.listener("before_server_stop")
    async def close_db(app, loop):
        await db.close()

    jinja = SanicJinja2(app)

    # import main from all py files in data_sources
    actions = {
        filename[:-3]: {
            "task": None,
            "function": importlib.import_module(f"data_sources.{filename[:-3]}").main,
        }
        for filename in os.listdir("data_sources")
        if filename.endswith(".py")
    }

    @app.route("/")
    async def index(request):
        nonlocal actions
        return jinja.render("control_panel.html", request, actions=actions.keys())

    @app.route("/start/<action>")
    async def start(request, action):
        nonlocal actions
        if action not in actions:
            return response.json({"error": f"{action} not found"})
        act = actions[action]
        if act["task"]:
            if act["task"].done():
                await act["task"]
            else:
                return response.json({"status": f"{action} is already running"})
        act["task"] = asyncio.create_task(act["function"](db))
        return response.json({"status": f"{action} started"})

    @app.route("/stop/<action>")
    async def stop(request, action):
        nonlocal actions
        if action not in actions:
            return response.json({"error": f"{action} not found"})
        act = actions[action]
        if not act["task"]:
            return response.json({"status": f"{action} has not started"})
        if act["task"].done():
            return response.json({"status": f"{action} is already finished"})
        act["task"].cancel()
        return response.json({"status": f"{action} stopped"})

    @app.route("/status/<action>")
    async def status(request, action):
        nonlocal actions
        if action not in actions:
            return response.json({"error": f"{action} not found"})
        act = actions[action]
        if not act["task"]:
            return response.json({"running": False})
        if act["task"].done():
            return response.json({"running": False, "result": act["task"].result()})
        else:
            return response.json({"running": True})

    return app
