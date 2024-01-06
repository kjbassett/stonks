import asyncio
import importlib
import inspect
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
    plugins = load_plugins("plugins")

    @app.route("/")
    async def index(request):
        nonlocal plugins
        return jinja.render("control_panel.html", request, plugins=plugins)

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


def load_plugins(folder):
    plugins = {}
    for root, dirs, files in os.walk(folder):
        for file in files:
            if file.endswith(".py") and file != "__init__.py":
                module = importlib.import_module(f".{file[:-3]}", package=folder)
                for name, func in inspect.getmembers(module, inspect.isfunction):
                    if getattr(func, "is_plugin", False):
                        plugins[name] = func
    return plugins
