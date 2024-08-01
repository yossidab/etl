# Data ETL Process

## Description
This project implements a data ETL (Extract, Transform, Load) process using Node.js and PostgreSQL. It includes three main components:
1. **Client**: Sends events to the server.
2. **Server**: Receives events from the client and writes them to a local file. It's using buffer (The buffer size is configurable), and after predefined time it's flushing all the buffer's events to server-events.jsonl file.
   using buffer is useful in order to decrease the file lock which required on read/write operation.
4. **Data Processor**: Watches the events file and processes new events to update user revenues in the database. this service also use file lock/unlock mechanism therfore every time it's read from file
   it's lock it and after processing and updatign the DB it's deleting the processed events (if the process was unsuccessfull it's write it back to server-events.jsonl)

## Prerequisites
- Node.js (version 14.x or higher)
- PostgreSQL (version 12.x or higher)

## Setup Instructions

### 1. Clone the Repository
```sh
git clone [<repository_url>](https://github.com/yossidab/etl.git)

### 2. Install dependancies
npm install

### 3. Setup PostgreSQL Database
CREATE DATABASE etl_db;
CREATE USER etl_user WITH ENCRYPTED PASSWORD 'your postgres passward';
GRANT ALL PRIVILEGES ON DATABASE etl_db TO etl_user;

### 4. Run the Server
node server.js

### 5. Run the Data Processor
node data_processor.js

### 6. Run the Client
node client.js

