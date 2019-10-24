require('dotenv').config()
const mqtt = require('mqtt');
const mqttClient = mqtt.connect('mqtt://192.168.1.16');
const { Pool, Client } = require('pg')
const pool = new Pool()

mqttClient.on('connect', () => {
    console.log("Connected!");
    mqttClient.subscribe('#');
});

mqttClient.on('message', (topic, message) => {
    let json = JSON.parse(message.toString());

    if (topic === 'esp32/battery') {
        let voltage = json.voltage.substring(0, json.voltage.length - 1); // Remove the trailing 'V'
        let state_of_charge = json.state_of_charge.substring(0, json.voltage.length - 1); // Remove the trailing '%'
        insertBattery(json.datetime, voltage, state_of_charge).catch(error => console.error(error));
    } else if (topic === 'esp32/temperature') {
        insertTemperature(json.datetime, json.temperature_0, 0);
        insertTemperature(json.datetime, json.temperature_1, 1);
    } else {
        console.log("Unexpected message: " + topic + ": " + message);
    }
});

async function insertBattery(time, voltage, stateOfCharge) {
    const client = await pool.connect()
    try {
        const result = await client.query('INSERT INTO battery(voltage, state_of_charge, client_time) VALUES($1, $2, $3);', [voltage, stateOfCharge, time]);
        if (result.rowCount != 1) {
            console.warn("Row count was not equal to 1 in: ", result);
        }
    } finally {
        client.release()
    }
}

async function insertTemperature(time, temperature, sensorNumber) {
    const client = await pool.connect()
    try {
        const result = await client.query('INSERT INTO temperature(client_time, temperature, sensor) VALUES($1, $2, $3);', [time, temperature, sensorNumber]);
        if (result.rowCount != 1) {
            console.warn("Row count was not equal to 1 in: ", result);
        }
    } finally {
        client.release()
    }
}