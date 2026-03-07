# stonks
A futile attempt at making money. But also because I enjoy coding.

This code will gather data related to the stock market and perform automatic trading.
It will use this data to try and predict future price changes. It uses a combination of a neural network and a genetic algorithm to train the best model.

## Install

1. Use python 3.10 to create a virtual environment in the project's root directory. (Only do this once).

    ```python3.10 -m venv venv```

2. Activate the virtual environment

    ```venv/scripts/activate```

3. Install the libraries

    ```pip install -r requirements.txt```

4. Run the program

    ```rock```


## Application Components
<details><summary>API</summary>
Massive (previously Polygon.io) is an online service that supplies market data through their API. This not only includes data from stock trading but also from the news.
</details>

<details><summary>Database</summary>
The database is a SQLite database, and it is where we store the data pulled from the Polygon.io API. There are also tables for Reddit, but fetching that information will be completed after a working prototype
</details>

<details><summary>Prediction Model</summary>
The prediction model is a neural network, and the hyperparameters for it and its preprocessing steps are "optimized" using a genetic algorithm. The network itself is trained using back propagation. The input data for the neural network includes, but is not limited to, current price, trading volume, their relation to historical price and trading volume, volitility, and encoded news data. It outputs a predicted percentage change in price over the next n days.

The genetic algorithm has been decoupled and is located in the library GeneticModelTuner, imported through ezmt. We can edit this library whenever it is needed.
</details>

<details><summary>Web Interface</summary>
The web interface is a simple way to trigger functions that we have written. Upon starting the application, the web server scans all code written in the "plugins" folder. If it finds a function decorated with the @plugin() decorator, it stores its metadata in a dictionary. The dictionary of metadata is supplied to a jinja template. The template iterates through the metadata and generates the webpage.

The web server has been decoupled and is located in the library Webrock. We can edit this library whenever it is needed. The entrypoint of the project is through the terminal command "rock".
</details>

## Future Developments
* Predicting the distribution of investment across multiple companies that yields the most money
* Predicting multiple price changes at different intervals in the future
</details>
