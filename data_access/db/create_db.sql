-- Company Table
CREATE TABLE Company (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT,
  symbol TEXT UNIQUE NOT NULL,
  industry_id INTEGER,
  FOREIGN KEY(industry_id) REFERENCES Industry(id)
);

CREATE TABLE Industry (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT UNIQUE NOT NULL
);

-- TradingData Table
CREATE TABLE TradingData (
  company_id INTEGER NOT NULL,
  timestamp INTEGER NOT NULL,
  open REAL,
  high REAL,
  low REAL,
  close REAL,
  vw_average REAL,
  volume INTEGER,
  UNIQUE (company_id, timestamp),
  FOREIGN KEY(company_id) REFERENCES Company(id)
);

-- Reddit Table
CREATE TABLE Reddit (
  id TEXT PRIMARY KEY,
  subreddit TEXT,
  parent_id TEXT,
  title TEXT,
  body TEXT,  -- Content of the post
  author_id TEXT,
  score INTEGER
);

-- RedditCompanyLink Table
CREATE TABLE RedditCompanyLink (
  company_id INTEGER,
  reddit_id INTEGER,
  FOREIGN KEY(company_id) REFERENCES Company(id),
  FOREIGN KEY(reddit_id) REFERENCES Reddit(id),
  UNIQUE (company_id, reddit_id)
);

-- News Table
CREATE TABLE News (
  id TEXT PRIMARY KEY,
  source TEXT,
  timestamp INTEGER,
  title TEXT,
  body TEXT,
  UNIQUE (source, title, timestamp)
);

-- NewsCompanyLink Table
CREATE TABLE NewsCompanyLink (
  company_id INTEGER,
  news_id TEXT,
  FOREIGN KEY(company_id) REFERENCES Company(id),
  FOREIGN KEY(news_id) REFERENCES News(id),
  UNIQUE (company_id, news_id)
);

CREATE TABLE NewsGap (
  company_id INTEGER,
  start INTEGER,
  end INTEGER,
  FOREIGN KEY(company_id) REFERENCES Company(id),
  UNIQUE (company_id, start, end)
);

-- DailyCompanyData Table
CREATE TABLE DailyCompanyData (
  date TEXT,
  source TEXT,
  company_id INTEGER,
  value TEXT,
  type TEXT,
  FOREIGN KEY(date) REFERENCES Calendar(date),
  FOREIGN KEY(company_id) REFERENCES Company(id),
  UNIQUE(date, source, company_id)
);

-- Models Table
CREATE TABLE Model (
  id INTEGER PRIMARY KEY,
  name TEXT,
  performance REAL,
  performance_metric TEXT,
  configuration TEXT  -- JSON stored as text
);

-- Prediction Table
CREATE TABLE Prediction (
  model_id INTEGER,
  company_id INTEGER,
  prediction REAL,
  timestamp INTEGER,
  FOREIGN KEY(model_id) REFERENCES Model(id),
  FOREIGN KEY(company_id) REFERENCES Company(id),
  UNIQUE (model_id, company_id)
);

-- Calendar Table
CREATE TABLE Calendar (
  date TEXT PRIMARY KEY,
  open INTEGER,
  close INTEGER
);

-- Trading Data Gap
CREATE TABLE TradingDataGap (
  company_id INTEGER,
  start INTEGER,
  end INTEGER,
  FOREIGN KEY(company_id) REFERENCES Company(id),
  UNIQUE (company_id, start, end)
);