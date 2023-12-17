import asyncio
from sanic import Sanic, response
from sanic_jinja2 import SanicJinja2
import importlib
import os


def create_app(db_instance):
    app = Sanic(__name__)
    jinja = SanicJinja2(app)

    # import main from all py files in data_sources
    actions = {
        filename[:-3]: {
            'task': None,
            'function': importlib.import_module(f"data_sources.{filename[:-3]}").main
        }
        for filename in os.listdir("data_sources") if filename.endswith(".py")
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
        action = actions[action]
        if action['task']:
            if action['task'].done():
                await action['task']
            else:
                return response.json({"status": f"{action} is already running"})
        action['task'] = asyncio.create_task(action['function'](db_instance))
        return response.json({"status": f"{action['task']} started"})

    @app.route("/stop/<action>")
    async def stop(request, action):
        nonlocal actions
        if action not in actions:
            return response.json({"error": f"{action} not found"})
        if actions[action]['task'].done():
            return response.json({"status": f"{action} is not running"})
        else:
            actions[action]['task'].cancel()
            print(actions[action]['task'].done())
            return response.json({"status": f"{action} stopped"})


    @app.route("/status/<action>")
    async def status(request, action):
        nonlocal actions
        if action not in actions:
            return response.json({"error": f"{action} not found"})
        if not actions[action]['task'] or actions[action]['task'].done():
            return response.json({"running": False})
        return response.json({"running": True})
    return app
