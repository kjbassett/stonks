# Claude Code Project Instructions

## Project Overview
This code will gather data related to the stock market and perform automatic trading.
It will use this data to try and predict future price changes. It uses a combination of a neural network and a genetic algorithm to train the best model.
Trading safety is a top priority. 


## Application Components
### Data API
Massive (previously Polygon.io) is an online service that supplies market data through their API. This not only includes data from stock trading but also from the news.

### Trading API
Schwab API

### Database
The database is a SQLite database, and it is where we store the data pulled from the Polygon.io API. There are also tables for Reddit, but fetching that information will be completed after a working prototype

### Prediction Model
The prediction model is a neural network, and the hyperparameters for it and its preprocessing steps are "optimized" using a genetic algorithm. The network itself is trained using back propagation. The input data for the neural network includes, but is not limited to, current price, trading volume, their relation to historical price and trading volume, volitility, and encoded news data. It outputs a predicted percentage change in price over the next n days.

The genetic algorithm has been decoupled and is located in the library GeneticModelTuner, imported through ezmt. We can edit this library whenever it is needed.

### Web Interface
The web interface is a simple way to trigger functions that we have written. Upon starting the application, the web server scans all code written in the "plugins" folder. If it finds a function decorated with the @plugin() decorator, it stores its metadata in a dictionary. The dictionary of metadata is supplied to a jinja template. The template iterates through the metadata and generates the webpage.

The web server has been decoupled and is located in the library Webrock. We can edit this library whenever it is needed. The entrypoint of the project is through the terminal command "rock".

## Tech Stack
Language: Python 3, some javascript
Web framework: Webrock (uses Sanic)
Database: SQLite
Testing: unittest
Formatting: Black
Async: True (asyncio)

## Coding Standards

### General
- Follow Pep8, don't fight the formatter
- All functions and methods must have type annotations
- All public functions and classes must have docstrings (Google style)
- All private functions start with an underscore
- No magic numbers. use named constants
- Config is stored in config.json
- Functions should do one thing. If you need 'and' to describe it, split it.
- Maximum function length: 30 lines (docstrings excluded).
- Never repeat yourself. Frequently check if you are writing similar code. If found, abstract the common logic or otherwise refactor
- Use abstract base classes to define an interface if there will be multiple classes that could inherit it.

### Naming
- Variables and functions: snake_case
- Classes: PascalCase
- Constants: UPPER_SNAKE_CASE
- Booleans: prefix with 'is_', 'has_', 'can_'
- Functions should all be verb phrases

### Error Handling
- Never use a bare 'except:' -- always catch specific exceptions
- Always log exceptions and raise or handle them. Do not do nothing about them.
- Validate all inputs at system boundaries (API endpoints, event consumers)

### Logging
- Always asynchronously log using aiologger (standard logging is synchronous and blocks the event loop)
- Each module has its own logging settings
- Use a rotating file handler, make the max file size and number of files configurable through config.json

### Security
- Never hard code credentials, URLs, or secrets. Use environment variables
- config.json holds all non-secret config
- Validate and sanitize all user inputs before use
- Use parameterized queries. Do not string-interpolate SQL

### Testing
- Parent test folder: tests
- Test files must end in "test"
- Every function must have unit tests covering: the happy path, at least 2 edge cases, and at least 1 invalid input.
- Use the AAA pattern (Arrange, Act, Assert) in all tests
- Tests must not share a state with one another
- Mock all I/O in unit tests
- Test folder and file structure must match the source structure. src/my_folder/my_file.py -> tests/unit/my_folder/my_file_test.py
- Claude can generate unit tests freely
- Behavioral tests test user-visible behavior and business rules. Claude cannot fill in assertion values but must fill in a placeholder and prevent the test from passing
- If code is async, use unittest.IsolatedAsyncioTestCase
- Mock async methods with AsyncMock, not MagicMock

### Handling ambiguity
If the intent of a requirement is unclear, do not assume. Instead, stop and list your assumptions explicitly in your response and ask for clarification before proceeding

## Architecture Notes
- All services should use a data access object retrieved from the dao_manager. Default data access objects are created for each table and inherit BaseDAO.
- See memory/trading.md for trading module design and safety rules.
- See memory/architecture.md for full data flow, DB schema, and tech stack details.

## Things Claude Must Never Do in This Repo
- Claude must not modify the public API contract of any endpoint without asking the user and explaining why.
- Add new dependencies in requirements.txt without asking a human.
- Implement behavior outside the stated Scope

## Things Claude Should Always Do
- Record important decisions in decision_log.csv, including interpretation decisions made by humans and design decisions. Columns are decision, rationale, alternatives_considered, made_by, date, time
- Suggest new features and better design choices if they will help the user or customer.
- Tell us how to better prompt itself after an issue is encountered
- Update the readme and claude md, but it should ask a human before edits.
- Check for code duplication and overcomplication