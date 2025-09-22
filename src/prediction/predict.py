from ezmt.organism import Organism
from webrock.decorator import plugin


@plugin()
async def predict_latest_data(model_folder: str):
    model = Organism.load(model_folder)
    await model.predict()
