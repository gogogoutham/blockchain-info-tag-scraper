blockchain-info-tag-scraper
===========================

node.js program for scraping address tag information posted on blockchain.info

Installation
============

- Clone repository, and install all pre-requisites within cloned directory as follows:

```
npm install async cheerio debug pg request sql
```
- Create the target tables to store the scraped data in PostgreSQL (see the sql/ directory). Modify table names as necessary.
- Create a Postgres formatted password file containing database connection information. As of 9.1 the format is as follows:
http://www.postgresql.org/docs/9.1/static/libpq-pgpass.html


Usage
=====

Modify the run.js file in the root directory of the repository to match your configuration and execute:

```
node run.js
````
