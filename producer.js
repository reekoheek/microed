const { KafkaClient, HighLevelProducer } = require('kafka-node');
const debug = require('debug')('microed:producer');
const path = require('path');
const Queue = require('./lib/queue');
const { Mutex } = require('await-semaphore');

class Producer {
  constructor ({ dataDir = path.join(process.cwd(), '.microed'), kafkaHost } = {}) {
    this.kafkaHost = kafkaHost;

    this.raw = this.createProducer();

    this.mutex = new Mutex();
    this.queue = new Queue({ dataDir });
  }

  async send (topic, value) {
    await this.queue.put({ topic, value });

    if (!this.raw.ready) {
      return;
    }

    this.drain();
  }

  async drain () {
    await this.mutex.use(async () => {
      try {
        debug('Draining...');

        while (1) {
          let items = await this.queue.fetch();
          if (items.length === 0) {
            return;
          }

          let topicGroups = {};

          items.forEach(({ topic, value }) => {
            let topicGroup = topicGroups[topic] = topicGroups[topic] || { topic, partition: 0, messages: [] };
            try {
              topicGroup.messages.push(JSON.stringify(value));
            } catch (err) {
              debug('Invalid message', err);
            }
          });

          let payloads = [];
          for (let topic in topicGroups) {
            payloads.push(topicGroups[topic]);
          }

          await new Promise((resolve, reject) => {
            this.raw.send(payloads, (err, data) => {
              if (err) {
                return reject(err);
              }

              resolve(data);
            });
          });

          await this.queue.commit();
        }
      } catch (err) {
        // noop
        // console.error('Draining caught err', err);
      }
    });
  }

  async destroy () {
    await this.queue.close();
    await new Promise(resolve => this.raw.close(resolve));
    debug('Producer destroyed');
  }

  createProducer () {
    let client = new KafkaClient({
      kafkaHost: this.kafkaHost,
    });

    client.on('error', err => {
      debug('Client error', err);
    });

    let producer = new HighLevelProducer(client);

    producer.on('error', err => {
      debug('Producer error', err);
    });

    producer.once('ready', () => {
      debug('Producer ready');
      this.drain();
    });

    return producer;
  }
}

module.exports = Producer;
