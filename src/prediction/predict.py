from ezmt.organism import Organism
from webrock.decorator import plugin


@plugin()
async def predict_latest_data(name: str, version: str, log_state: bool = False):
    model = Organism.load(name, version)
    print(f"log_state: {log_state}")
    await model.predict(log_state=log_state)
