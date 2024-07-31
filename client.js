const axios = require('axios');
const fs = require('fs');
const readline = require('readline');

const SECRET = 'secret';
const SERVER_URL = 'http://localhost:8000/liveEvent';

async function sendEvent(event) {
    try {
        const response = await axios.post(SERVER_URL, event, {
            headers: {
                'Authorization': SECRET
            }
        });
        console.log(`Event sent successfully: ${response.status}`);
    } catch (error) {
        console.error(`Error sending event: ${error.message}`);
    }
}

async function processEvents(filePath) {
    const fileStream = fs.createReadStream(filePath);
    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });

    for await (const line of rl) {
        if(line !== undefined && line !== '') {
            const event = JSON.parse(line);
            await sendEvent(event);
        }
    }
}

setInterval(() => processEvents('events.jsonl'), 2000);

