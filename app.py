import asyncio

from sanic import response, Sanic
from sanic_jinja2 import SanicJinja2

from config import CONFIG
from models.db.async_database import AsyncDatabase
from plugins.load_plugins import load_plugins


def create_app():
    app = Sanic("Stonks")
    db = AsyncDatabase(CONFIG["db_folder"] + CONFIG["db_name"])

    print("CREATING APP")

    @app.listener("before_server_stop")
    async def close_db(app, loop):
        await db.close()

    jinja = SanicJinja2(app)

    # import main from all py files in data_sources
    metadata, plugins = load_plugins()

    @app.route("/")
    async def index(request):
        nonlocal plugins
        return jinja.render("control_panel.html", request, metadata=metadata)

    @app.route("/start/<plugin>")
    async def start(request, plugin):
        nonlocal plugins
        if plugin not in plugins:
            return response.json({"error": f"{plugin} not found"})
        act = plugins[plugin]
        if act["task"]:
            if act["task"].done():
                await act["task"]
            else:
                return response.json({"status": f"{plugin} is already running"})
        act["task"] = asyncio.create_task(act["function"](db))
        return response.json({"status": f"{plugin} started"})

    @app.route("/stop/<plugin>")
    async def stop(request, plugin):
        nonlocal plugins
        if plugin not in plugins:
            return response.json({"error": f"{plugin} not found"})
        act = plugins[plugin]
        if not act["task"]:
            return response.json({"status": f"{plugin} has not started"})
        if act["task"].done():
            return response.json({"status": f"{plugin} is already finished"})
        act["task"].cancel()
        return response.json({"status": f"{plugin} stopped"})

    @app.route("/status/<plugin>")
    async def status(request, plugin):
        nonlocal plugins
        if plugin not in plugins:
            return response.json({"error": f"{plugin} not found"})
        act = plugins[plugin]
        if not act["task"]:
            return response.json({"running": False})
        if act["task"].done():
            return response.json({"running": False, "result": act["task"].result()})
        else:
            return response.json({"running": True})

    return app
