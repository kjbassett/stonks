import asyncio
from sanic import Sanic, response
from sanic_jinja2 import SanicJinja2
from data_sources.stream_price_data import websocket_client


def create_app(db_instance):
    app = Sanic(__name__)
    app.ctx.status_manager = StatusManager()  # Assuming you have a StatusManager class as before
    jinja = SanicJinja2(app)
    actions = {}

    @app.route("/")
    async def index(request):
        return jinja.render("control_panel.html", request)

    @app.route("/start/<action>")
    async def start(request, action):
        nonlocal actions
        if action not in actions or actions[action].done():
            if action == "stream_price_data":  # Example for WebSocket
                actions[action] = {'stop_event': asyncio.Event(),
                                   'thread': asyncio.create_task(
                                       websocket_client(db_instance, stop_event, app)
                )}
            else:
                # Handle other components
                pass
            return response.json({"status": f"{action} started"})
        return response.json({"status": f"{action} is already running"})

    @app.route("/stop/<action>")
    async def stop(request, action):
        nonlocal actions
        if action in actions and not actions[action].done():
            if action == "polygon_websocket":  # Example for WebSocket
                actions[action].cancel()  # Cancel the task
                actions[action] = None
            else:
                # Handle other components
                pass
            return response.json({"status": f"{action} stopped"})
        return response.json({"status": f"{action} is not running"})

    @app.route("/status/<action>")
    async def status(request, action):
        st = app.ctx.status_manager.get_status(action)
        return response.json(st)

    return app


class StatusManager:
    def __init__(self):
        self.statuses = {
            "strea_price_data": {"authenticated": False, "message": "", "running": False},
            # Other components can be added here
        }

    def update_status(self, component, status):
        self.statuses[component].update(status)

    def get_status(self, component):
        return self.statuses.get(component, {})