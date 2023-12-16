-- Companies Table
CREATE TABLE Companies (
  id INTEGER PRIMARY KEY,
  name TEXT,
  symbol TEXT UNIQUE NOT NULL,
  industry TEXT
);

-- TradingData Table
CREATE TABLE TradingData (
  company_id INTEGER,
  timestamp INTEGER,
  open REAL,
  high REAL,
  low REAL,
  close REAL,
  volume INTEGER,
  UNIQUE (company_id, timestamp),
  FOREIGN KEY(company_id) REFERENCES Companies(id)
);

-- Reddit Table
CREATE TABLE Reddit (
  id INTEGER PRIMARY KEY,
  subreddit TEXT,
  parent_id INTEGER,
  title TEXT,
  body TEXT,  -- Content of the post
  author_id INTEGER,
  upvotes INTEGER,
  downvotes INTEGER
);

-- RedditCompaniesLink Table
CREATE TABLE RedditCompaniesLink (
  company_id INTEGER,
  reddit_id INTEGER,
  FOREIGN KEY(company_id) REFERENCES Companies(id),
  FOREIGN KEY(reddit_id) REFERENCES Reddit(id),
  UNIQUE (company_id, reddit_id)
);

-- News Table
CREATE TABLE News (
  id INTEGER PRIMARY KEY,
  source TEXT,
  timestamp INTEGER,
  title TEXT,
  body TEXT,
  UNIQUE (source, title, timestamp)
);

-- NewsCompaniesLink Table
CREATE TABLE NewsCompaniesLink (
  company_id INTEGER,
  news_id INTEGER,
  FOREIGN KEY(company_id) REFERENCES Companies(id),
  FOREIGN KEY(news_id) REFERENCES News(id),
  UNIQUE (company_id, news_id)
);

-- DailyCompanyData Table
CREATE TABLE DailyCompanyData (
  date TEXT,
  source TEXT,
  company_id INTEGER,
  value TEXT,
  type TEXT,
  FOREIGN KEY(date) REFERENCES Calendar(date),
  FOREIGN KEY(company_id) REFERENCES Companies(id),
  UNIQUE(date, source, company_id)
);

-- Models Table
CREATE TABLE Models (
  id INTEGER PRIMARY KEY,
  name TEXT,
  performance REAL,
  performance_metric TEXT,
  configuration TEXT  -- JSON stored as text
);

-- Predictions Table
CREATE TABLE Predictions (
  model_id INTEGER,
  company_id INTEGER,
  prediction REAL,
  timestamp INTEGER,
  FOREIGN KEY(model_id) REFERENCES Models(id),
  FOREIGN KEY(company_id) REFERENCES Companies(id),
  UNIQUE (model_id, company_id)
);

-- Calendar Table
CREATE TABLE Calendar (
  date TEXT PRIMARY KEY,
  open INTEGER,
  close INTEGER
);

-- Trading Data Gaps
CREATE TABLE TradingDataGaps (
  company_id INTEGER,
  start INTEGER,
  end INTEGER,
  FOREIGN KEY(company_id) REFERENCES Companies(id),
  UNIQUE (company_id, start, end)
);