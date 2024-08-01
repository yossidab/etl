const express = require('express');
const bodyParser = require('body-parser');
const fs = require('fs');
const winston = require('winston');
const { Pool } = require('pg');
const lockfile = require('proper-lockfile');

const APP = express();
const PORT = 8000;
const AUTH_SECRET = 'secret';
const SERVER_EVENTS_PATH = './server-events.jsonl';
const EVENT_FILE = 'server-events.jsonl';
const BUFFER = [];
const BUFFER_SIZE = 10000; // Adjust the buffer size as needed
const FLASH_INTERVAL = 36000; // Flush interval in milliseconds

const POOL = new Pool({
  user: 'postgres',
  host: 'localhost',
  database: 'etl_db',
  password: '12344321',
  port: 5432, 
});

POOL.query(
  `CREATE TABLE IF NOT EXISTS users_revenue (
    user_id TEXT PRIMARY KEY,
    revenue INTEGER DEFAULT 0
  )`,
  (err) => {
    if (err) {
      LOGGER.error(`Error creating table: ${err.message}`);
    } else {
      LOGGER.info('Database and table initialized.');
    }
  }
);

const LOGGER = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(({ timestamp, level, message }) => {
      return `${timestamp} [${level}]: ${message}`;
    })
  ),
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: 'server.log' })
  ],
});

APP.use(bodyParser.json());

// Middleware to authenticate the client
APP.use((req, res, next) => {
  const authHeader = req.headers['authorization'];
  if (authHeader === AUTH_SECRET) {
    LOGGER.debug(`Authorized request: ${req.method} ${req.url}`);
    next();
  } else {
    LOGGER.warn(`Unauthorized request: ${req.method} ${req.url}`);
    res.status(401).send('Unauthorized');
  }
});

async function isFileLocked(filePath) {
  try {
    const isLocked = await lockfile.check(filePath);
    return isLocked;
  } catch (err) {
    return false;
  }
}

// Function to flush the buffer to the file
async function flushBuffer(){
  if (BUFFER.length > 0) {
    const data = BUFFER.join('\n') + '\n';

    if (await isFileLocked(EVENT_FILE)) {
      setImmediate(() => flushBuffer(), 1000);
      return;
    } else {
      try {
        await lockfile.lock(EVENT_FILE);
        LOGGER.debug(`Locked file: ${EVENT_FILE}`);
      } catch {
        setImmediate(() => flushBuffer(), 1000);
        return;
      }
    }

    fs.appendFile(EVENT_FILE, data, (err) => {
      if (err) {
        LOGGER.error(`Error writing to file: ${err.message}`);
      } else {
        LOGGER.debug(`Flushed ${BUFFER.length} events to file.`);
        BUFFER.length = 0; // Clear the buffer
      }
    });
    await lockfile.unlock(EVENT_FILE);
  }
};

setInterval(flushBuffer, FLASH_INTERVAL);

// POST /liveEvent endpoint
APP.post('/liveEvent', (req, res) => {
  const event = req.body;
  if(event.userId && event.name && event.value) {
    BUFFER.push(JSON.stringify(event));

    if (BUFFER.length >= BUFFER_SIZE) {
      flushBuffer();
    }
  
    res.status(200).send('Event received and buffered.');
  } else {
    res.status(422).send('Event missing userID/name/value');
  }

});

// GET /userEvents/:userid endpoint
APP.get('/userEvents', async (req, res) => {
  const userId = req.query.userid;

  try {
    const result = await POOL.query('SELECT * FROM users_revenue WHERE user_id = $1', [userId]);
    if (result.rows.length > 0) {
      LOGGER.debug(`Events retrieved for user ${userId}.`);
      res.status(200).json(result.rows[0]);
    } else {
      res.status(404).send('User not found');
    }
  } catch (err) {
    LOGGER.error(`Error querying the database: ${err.message}`);
    res.status(500).send('Error querying the database');
  }
});

APP.listen(PORT,async () => {
  await createJSONLFile(SERVER_EVENTS_PATH);
  LOGGER.info(`Server is running on http://localhost:${PORT}`);
});

process.on('SIGINT', handleExit);
process.on('SIGTERM', handleExit);
process.on('SIGHUP', handleExit);
process.on('SIGUSR1', handleExit);
process.on('exit', handleExit);

function handleExit() {
  flushBuffer(); // Ensure the buffer is flushed before exit
  POOL.end((err) => {
    if (err) {
      LOGGER.error(`Error closing the database connection: ${err.message}`);
    }
    LOGGER.info('Closed the database connection.');
    process.exit(0);
  }); 
}

function createJSONLFile(filePath) {
  return new Promise((resolve, reject) => {
      fs.writeFile(filePath, '', { flag: 'wx', mode: 0o666 }, (err) => {
          if (err) {
              if (err.code === 'EEXIST') {
                  console.log('File already exists');
                  resolve();
              } else {
                  console.error('Error creating JSONL file:', err);
                  reject(err);
              }
          } else {
              console.log('New JSONL file created successfully with appropriate permissions');
              resolve();
          }
      });
  });
}