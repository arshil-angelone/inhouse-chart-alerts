#!/usr/bin/node

const { Kafka } = require('kafkajs')
const fs = require('fs')
const { WebClient } = require('@slack/web-api');

const token = "xoxb-877123502210-1295697286752-M8JKb6fjyd2XL"

const web = new WebClient(token);

const conversationId = '#alerts-live-portfolio-kafka-redis';

let nseStat=fs.readFileSync('/monitoring/synthetic/nsestats.json');
let marketStat=JSON.parse(nseStat);
let cmState=marketStat['marketState'][0];
let statusMsg=cmState['marketStatusMessage'];
if (!(statusMsg=="Normal Market is Open" || statusMsg=="Pre-Open Session Closed – Normal Market will begin shortly")){
    console.log("Market is closed, Exiting...")
    process.exit(1)
}
(async () => {
    web.chat.postMessage({ channel: conversationId, text: 'Starting Monitoring of topic: mcx_fo_ltp-trade-data' });
})();

const kafka = new Kafka({
    clientId: 'tm_1',
    brokers: ['192.168.254.194:9092', '192.168.254.174:9092', '192.168.254.133:9092'],
    ssl: {
        rejectUnauthorized: false,
        cert: fs.readFileSync('ClientCert.pem', 'utf-8')
    },
})

const topic = 'mcx_fo_ltp-trade-data'
const consumer = kafka.consumer({ groupId: 'tm_1' })
var lastReceivedTimestamp = new Date();

const run = async () => {
    await consumer.connect()
    await consumer.subscribe({ topic, fromBeginning: false })
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            lastReceivedTimestamp = new Date();
        },
    })
}

run().catch(e => console.error(`[topic_mon] ${e.message}`, e))

setInterval(() => {
    var now = new Date();
    var diff = (now.getTime() - lastReceivedTimestamp.getTime()) / 1000;
    if (diff > 5.0) {
        console.log(`Last message received ${diff} seconds ago.`);
        (async () => {
            web.chat.postMessage({ channel: conversationId, text: 'mcx_fo_ltp-trade-data: Message not received for more than 5 sec' });
        })();
    }
}, 5000);
const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.map(type => {
    process.on(type, async e => {
        try {
            console.log(`process.on ${type}`)
            console.error(e)
            await consumer.disconnect()
            process.exit(0)
        } catch (_) {
            process.exit(1)
        }
    })
})

signalTraps.map(type => {
    process.once(type, async () => {
        try {
            await consumer.disconnect()
        } finally {
            process.kill(process.pid, type)
        }
    })
})
