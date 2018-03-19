require('dotenv').config()
const mqtt = require('mqtt');
const mqttClient = mqtt.connect('mqtt://192.168.1.132');
const { Pool, Client } = require('pg')
const pool = new Pool()

mqttClient.on('connect', () => {
    console.log("Connected!");
    mqttClient.subscribe('#');
});

mqttClient.on('message', (topic, message) => {
    let json = JSON.parse(message.toString());

    if (topic === 'desk/temperature') {
        insertSample(json.secondsSinceEpoch * 1000, json.degreesCelsius, null).catch(error => console.error(error));
    } else if (topic === 'desk/humidity') {
        insertSample(json.secondsSinceEpoch * 1000, null, json.relativeHumidity).catch(error => console.error(error));
    }
});

async function insertSample(time, temperature, humidity) {
    const client = await pool.connect()
    try {
        const result = await client.query('INSERT INTO conditions(time, temperature, humidity) VALUES($1, $2, $3);', [new Date(time), temperature, humidity]);
        if (result.rowCount != 1) {
            console.warn("Row count was not equal to 1 in: ", result);
        }
    } finally {
        client.release()
    }
}
