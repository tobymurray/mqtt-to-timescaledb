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
        let state_of_charge = json.state_of_charge.substring(0, json.state_of_charge.length - 1); // Remove the trailing '%'
        insertBattery(json.datetime, voltage, state_of_charge, json.mac).catch(error => console.error(error));
    } else if (topic === 'esp32/temperature') {
        insertTemperature(json.datetime, json.temperature_0, 0, json.mac);
        insertTemperature(json.datetime, json.temperature_1, 1, json.mac);
    } else if (topic === 'esp32/heap') {
        insertHeap(parseInt(json.free_heap_size), parseInt(json.low_water_mark), json.mac).catch(error => console.error(error));
    } else if (topic === 'esp32/2/battery') {
        let voltage = json.voltage.substring(0, json.voltage.length - 1); // Remove the trailing 'V'
        let state_of_charge = json.state_of_charge.substring(0, json.state_of_charge.length - 1); // Remove the trailing '%'
        insertBattery(json.datetime, voltage, state_of_charge, json.mac).catch(error => console.error(error));
    } else if (topic === 'esp32/2/temperature') {
        insertTemperature(json.datetime, json.temperature_0, 0, json.mac);
        insertTemperature(json.datetime, json.temperature_1, 1, json.mac);
    } else if (topic === 'esp32/2/heap') {
        insertHeap(parseInt(json.free_heap_size), parseInt(json.low_water_mark), json.mac).catch(error => console.error(error));
    } else {
        console.log("Unexpected message: " + topic + ": " + message);
    }
});

async function insertBattery(time, voltage, stateOfCharge, mac) {
    let client;
    try {
        client = await pool.connect()
    } catch (e) {
        console.error("Failed to connect to database pool due to: " + e.message)
        return;
    }
    try {
        const result = await client.query('INSERT INTO battery(voltage, state_of_charge, client_time, mac) VALUES($1, $2, $3, $4);', [voltage, stateOfCharge, time, mac]);
        if (result.rowCount != 1) {
            console.warn("Row count was not equal to 1 in: ", result);
        }
    } finally {
        client.release()
    }
}

async function insertTemperature(time, temperature, sensorNumber, mac) {
    let client;
    try {
        client = await pool.connect()
    } catch (e) {
        console.error("Failed to connect to database pool due to: " + e.message)
        return;
    }
    try {
        const result = await client.query('INSERT INTO temperature(client_time, temperature, sensor, mac) VALUES($1, $2, $3, $4);', [time, temperature, sensorNumber, mac]);
        if (result.rowCount != 1) {
            console.warn("Row count was not equal to 1 in: ", result);
        }
    } finally {
        client.release()
    }
}

async function insertHeap(free_heap, low_water_mark, mac) {
    let client;
    try {
        client = await pool.connect()
    } catch (e) {
        console.error("Failed to connect to database pool due to: " + e.message)
        return;
    }
    try {
        const result = await client.query('INSERT INTO heap(free_heap, low_water_mark, mac) VALUES($1, $2, $3);', [free_heap, low_water_mark, mac]);
        if (result.rowCount != 1) {
            console.warn("Row count was not equal to 1 in: ", result);
        }
    } finally {
        client.release()
    }
}