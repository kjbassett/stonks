from ezmt.organism import Organism
from webrock.decorator import plugin


@plugin()
async def predict_latest_data(name: str, version: str):
    model = Organism.load(name, version)
    await model.predict(log_state=True)
