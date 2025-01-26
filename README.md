# stonks
A futile attempt at making money. But also because I enjoy coding.

This code will gather data related to stock prices, including historical price data, reddit posts, analyst ratings, and more.
It will use this data to try and predict future price changes.

## Install

1. Use python 3.10 to create a virtual environment in the project's root directory

    ```python3.10 -m venv venv```

2. Activate the virtual environment

    ```venv/scripts/activate```

3. Install the libraries

    ```pip install -r requirements.txt```

4. Run the program

    ```sanic run --single-process```


## Application Components
<details><summary>API</summary>
Polygon.io is an online service that supplies market data through their API. This not only includes data from stock trading but also from the news. When the code is ready, we will subscribe to their service.
</details>

<details><summary>Database</summary>
The database is a SQLite database, and it is where we store the data pulled from the Polygon.io API. There are also tables for Reddit, but fetching that information will be completed after a working prototype
</details>

<details><summary>Prediction Model</summary>
The prediction model is a neural network, and the hyperparameters for it and its preprocessing steps are "optimized" using a genetic algorithm. The network itself is trained using back propagation. The input data for the neural network includes, but is not limited to, current price, trading volume, their relation to historical price and trading volume, volitility, and encoded news data. It outputs a predicted percentage change in price over the next n days.
</details>

<details><summary>Web Interface</summary>
The web interface is a simple way to trigger functions that we have written. Upon starting the application, the web server scans all code written in the "plugins" folder. If it finds a function decorated with the @plugin() decorator, it stores its metadata in a dictionary. The dictionary of metadata is supplied to a jinja template. The template iterates through the metadata and generates the webpage.
</details>

## Future Developments
<details><summary>Web Server Decoupling</summary>
The idea behind the website it that it acts as a quick and easy way to execute code for your current project, or it could be a hub of your commonly used scripts. You can easily plop an @plugin() on top of any function, and its controls will be generated in the webpage. 

With that idea in mind, this program was written backwards, or rather inside out. I would like to import the web server into my stonks project, but in its current state, stonks is within the plugins folder of the web server.
</details>

<details><summary>Scheduler</summary>
This is an addition to the web server that would allow you to schedule function calls. Some features that I would like to add are:

* Scheduling a one-time function call
* Scheduling recurring function calls
* Saving the current schedule to a file and loading it.
* Chaining function calls

</details>

<details><summary>3. Adding Data Sources</summary>
There is a lot of information on the internet that could help predict a company's stock price change.
It'd be a shame if we didn't use it. Reddit, other organization's stock recommendations, news sources that are not covered by Polygon.io, the weather, world events, etc.
</details>

<details><summary>4. Changing what is predicted</summary>
Currently we predict the percentage change after some amount of time. We could try to get more information or try a different strategy. Some ideas:

* Predicting the percentage change and a confidence level
* Predicting only buy/sell
* Predicting the distribution of investment across multiple companies that yields the most money
</details>
