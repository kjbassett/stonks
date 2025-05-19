-- Company Table
CREATE TABLE Company (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT,
  symbol TEXT UNIQUE NOT NULL,
  industry_id INTEGER,
  FOREIGN KEY(industry_id) REFERENCES Industry(id)
);

CREATE TABLE Industry (
  id INTEGER PRIMARY KEY,
  name TEXT UNIQUE NOT NULL,
  office_id INTEGER NOT NULL,
  FOREIGN KEY(office_id) REFERENCES IndustryOffice(id)
);

CREATE TABLE IndustryOffice (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
)

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

-- TradingDataAggregation Table
CREATE TABLE TradingDataAggregation (
  company_id INTEGER NOT NULL,
  interval TEXT NOT NULL,  -- aggregation interval (e.g., 'hour', 'day')
  date DATE,
  hour INTEGER,
  start INTEGER,
  end INTEGER,
  open REAL,
  high REAL,
  low REAL,
  close REAL,
  avg_close REAL,
  cv_close REAL,
  price_change REAL, --percent change
  avg_volume REAL,
  cv_volume REAL,
  row_count INTEGER,
  UNIQUE (company_id, interval, date, hour),
  FOREIGN KEY(company_id) REFERENCES Company(id)
);

CREATE INDEX idx_trading_data_aggregation ON TradingDataAggregation (company_id, date, hour, interval);

-- Reddit Table
CREATE TABLE Reddit (
  id TEXT PRIMARY KEY,
  subreddit TEXT,
  parent_id TEXT,
  title TEXT,
  body TEXT,  -- Content of the post
  author_id TEXT,
  score INTEGER,
  timestamp INTEGER NOT NULL DEFAULT 0,
  updated_at INTEGER NOT NULL DEFAULT 0
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
  sentiment INTEGER,
  sentiment_reasoning TEXT,
  FOREIGN KEY(company_id) REFERENCES Company(id),
  FOREIGN KEY(news_id) REFERENCES News(id),
  UNIQUE (company_id, news_id)
);

CREATE TABLE NewsAttemptedQueries (
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

-- Calendar Table (TODO is this needed?)
CREATE TABLE Calendar (
  date TEXT PRIMARY KEY,
  open INTEGER,
  close INTEGER
);

-- Trading Data Gap
CREATE TABLE TradingDataAttemptedQueries (
  company_id INTEGER,
  start INTEGER,
  end INTEGER,
  FOREIGN KEY(company_id) REFERENCES Company(id),
  UNIQUE (company_id, start, end)
);