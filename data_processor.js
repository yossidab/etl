const fs = require('fs');
const { Pool } = require('pg');
const winston = require('winston');
const lockfile = require('proper-lockfile');

// Setup Winston logger
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
    new winston.transports.File({ filename: 'dataProcessor.log' })
  ],
});

// Initialize the database connection
const POOL = new Pool({
  user: 'postgres',
  host: 'localhost',
  database: 'etl_db',
  password: '12344321',
  port: 5432,
});
const EVENT_FILE = 'server-events.jsonl';
const MAX_LIMIT = 1000;

// Check if the file is locked
async function isFileLocked(filePath) {
  try {
    const isLocked = await lockfile.check(filePath);
    return isLocked;
  } catch (err) {
    return false;
  }
}

// Function to process a batch of lines from the file
async function processBatch(fileName) {
  let lines = [];
  try {
    if (await isFileLocked(fileName)) {
      setTimeout(() => processBatch(fileName), 6000);
      return;
    } else {
      try {
        await lockfile.lock(fileName);
        LOGGER.debug(`Locked file: ${fileName}`);
      } catch {
        setTimeout(() => processBatch(fileName), 6000);
        return;
      }
    }

    // Read the file line by line until the batch size is reached or the file ends
    const fileData = fs.readFileSync(fileName, 'utf-8');
    lines = fileData.split('\n').filter(Boolean).slice(0, MAX_LIMIT);

    if (lines.length === 0) {
      LOGGER.debug('No lines to process.');
      await lockfile.unlock(fileName);
      LOGGER.debug(`Unlocked file: ${fileName}`);
      return;
    }

    // Write the remaining lines back to the file
    const remainingLines = fileData.split('\n').filter(Boolean).slice(MAX_LIMIT);
    fs.writeFileSync(fileName, remainingLines.join('\n') + '\n');

    // Release the file lock
    await lockfile.unlock(fileName);
    LOGGER.debug(`Unlocked file: ${fileName}`);

  } catch (err) {
    if (err.message === 'Lock file is already being held') {
      processBatch(fileName);
    }
    await lockfile.unlock(fileName);
    return;
  }

  // Process the batch of lines if successfully read and removed from the file
  if (lines.length > 0) {
    const userRevenueMap = {};

    lines.forEach((line) => { 
      try {
        const event = JSON.parse(line);
        const userId = event.userId;
        const revenue = event.name === 'add_revenue' ? event.value : -event.value;

        if (!userRevenueMap[userId]) {
          userRevenueMap[userId] = 0;
        }
        userRevenueMap[userId] += revenue;
      } catch (err) {
        LOGGER.error(`Error parsing event: ${err.message}`);
      }
    });

    // Update the user's revenue in the database
    const client = await POOL.connect();
    try {
      await client.query('BEGIN');
      for (const [userId, revenue] of Object.entries(userRevenueMap)) {
        const result = await client.query(
          `UPDATE users_revenue SET revenue = revenue + $1 WHERE user_id = $2 RETURNING *`,
          [revenue, userId]
        );

        if (result.rowCount === 0) {
          await client.query(
            `INSERT INTO users_revenue (user_id, revenue) VALUES ($1, $2)`,
            [userId, revenue]
          );
          LOGGER.debug(`Inserted new user ${userId} with initial revenue ${revenue}.`);
        } else {
          LOGGER.debug(`Updated revenue for user ${userId} by ${revenue}.`);
        }
      }
      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK');
      LOGGER.info(`Error updating/inserting revenue for users: ${err.message}`);
      // Re-write the lines to the file if there's an error
      fs.appendFile(fileName, lines.join('\n') + '\n' + remainingLines.join('\n') + '\n', (err) => {
        if (err) {
          LOGGER.error(`Error writing to file: ${err.message}`);
        } else {
          LOGGER.debug(`Flushed ${lines.length} events to file.`);
        }
      });
    } finally {
      client.release();
    }
  }

  // Process the next batch
  setImmediate(() => processBatch(fileName));
};

// Function to watch the file for changes
function watchFile(fileName) {
  LOGGER.info('The data processor is running');
  fs.watch(fileName, (eventType) => {
    if (eventType === 'change') {
      LOGGER.debug(`File changed: ${fileName}`);
      processBatch(fileName);
    }
  });

  // Initial processing of the file
  processBatch(fileName);
};

watchFile(EVENT_FILE);

// Gracefully close the database connection on process exit
process.on('SIGINT', () => {
  POOL.end((err) => {
    if (err) {
      LOGGER.error(`Error closing the database connection: ${err.message}`);
    }
    LOGGER.info('Closed the database connection.');
    process.exit(0);
  });
});
