import asyncio

from data_access.dao_manager import dao_manager
from plugins.load_plugins import load_plugins
from sanic import Sanic, response
from sanic_jinja2 import SanicJinja2


async def create_app():
    app = Sanic("Stonks")
    await dao_manager.initialize()

    print("CREATING APP")

    @app.listener("before_server_stop")
    async def close_db(app, loop):
        print("Closing database connection")
        await dao_manager.db.close()

    jinja = SanicJinja2(app)

    # import main from all py files in data_sources
    metadata, plugins = load_plugins()

    @app.route("/")
    async def index(request):
        nonlocal plugins
        return jinja.render("control_panel.html", request, metadata=metadata)

    @app.route("/start/<plugin>", methods=["POST"])
    async def start(request, plugin):
        print(f"Received request to start {plugin}")
        nonlocal plugins
        if plugin not in plugins:
            return response.json({"error": f"{plugin} not found"})
        print("Found plugin")
        act = plugins[plugin]
        if act["task"]:
            if act["task"].done():
                await act["task"]
            else:
                return response.json({"status": f"{plugin} is already running"})
        act["task"] = asyncio.create_task(act["function"](**request.form))
        print(f"Started {plugin}")
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
