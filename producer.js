const { KafkaClient, HighLevelProducer } = require('kafka-node');
const debug = require('debug')('microed:producer');
const path = require('path');
const Queue = require('./lib/queue');
const { Mutex } = require('await-semaphore');

class Producer {
  constructor ({ dataDir = path.join(process.cwd(), '.microed'), kafkaHost = 'localhost:9092' } = {}) {
    this.kafkaHost = kafkaHost;

    this.raw = this.createProducer();

    this.mutex = new Mutex();
    this.queue = new Queue({ dataDir });

    this.repeatDrain();
  }

  async send (topic, value) {
    await this.queue.put({ topic, value });

    this.drain();
  }

  async repeatDrain () {
    clearTimeout(this.repeatDrainTimeout);

    await this.drain();

    this.repeatDrainTimeout = setTimeout(() => this.repeatDrain(), 1000);
  }

  async drain (force) {
    if (!this.raw.ready) {
      return;
    }

    if (!force && this.draining) {
      return;
    }

    this.draining = true;
    await this.mutex.use(async () => {
      try {
        debug('Draining...');

        let items;
        while ((items = await this.queue.fetch())) {
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
    this.draining = false;
  }

  async destroy () {
    clearTimeout(this.repeatDrainTimeout);
    await this.drain(true);
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
