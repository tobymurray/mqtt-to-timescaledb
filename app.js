require('dotenv').config()
const mqtt = require('mqtt');
const mqttClient = mqtt.connect('mqtt://192.168.1.198');
const { Pool, Client } = require('pg')
const pm2 = require('pm2');
const pool = new Pool()

pm2.launchBus((err, bus) => {
    console.log('connected', bus);

    bus.on('process:exception', function (data) {
        console.log(arguments);
    });

    bus.on('log:err', function (data) {
        console.log('logged error', arguments);
    });

    bus.on('reconnect attempt', function () {
        console.log('Bus reconnecting');
    });

    bus.on('close', function () {
        console.log('Bus closed');
    });
});

mqttClient.on('connect', () => {
    console.log("Connected!");
    mqttClient.subscribe('#');
});

mqttClient.on('message', (topic, message) => {
    let json = JSON.parse(message.toString());

    if (topic === 'incubator/temperature') {
        insertTemperature(json.datetime, json.temperature, json.mac);
    } else if (topic === 'incubator/humidity') {
        insertHumidity(json.datetime, json.relative_humidity, json.mac);
    } else if (topic === 'incubator/eggTurner') {
        insertEggTurner(json.datetime, json.status, json.mac);
    } else {
        console.log("Unexpected message: " + topic + ": " + message);
    }
});

async function insertTemperature(time, temperature, mac) {
    let client = await pool.connect().catch((e) => {
        console.error("Failed to connect to database pool due to: " + e.message);
        return;
    });

    try {
        const result = await client.query('INSERT INTO temperature(client_time, temperature, mac) VALUES($1, $2, $3);', [time, temperature, mac])
            .catch((e) => {
                console.error("Failed to connect to database pool due to: " + e.message);
                return;
            });
        if (result.rowCount != 1) {
            console.warn("Row count was not equal to 1 in: ", result);
        }
    } finally {
        client.release()
    }
}

async function insertHumidity(time, humidity, mac) {
    let client = await pool.connect().catch((e) => {
        console.error("Failed to connect to database pool due to: " + e.message);
        return;
    });

    try {
        const result = await client.query('INSERT INTO humidity(client_time, humidity, mac) VALUES($1, $2, $3);', [time, humidity, mac])
            .catch((e) => {
                console.error("Failed to connect to database pool due to: " + e.message);
                return;
            });
        if (result.rowCount != 1) {
            console.warn("Row count was not equal to 1 in: ", result);
        }
    } finally {
        client.release()
    }
}

async function insertEggTurner(time, status, mac) {
    let client = await pool.connect().catch((e) => {
        console.error("Failed to connect to database pool due to: " + e.message);
        return;
    });

    try {
        const result = await client.query('INSERT INTO egg_turner(client_time, status, mac) VALUES($1, $2, $3);', [time, status, mac])
            .catch((e) => {
                console.error("Failed to connect to database pool due to: " + e.message);
                return;
            });
        if (result.rowCount != 1) {
            console.warn("Row count was not equal to 1 in: ", result);
        }
    } finally {
        client.release()
    }
}
