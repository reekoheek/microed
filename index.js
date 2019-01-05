const { KafkaClient, Consumer, HighLevelProducer } = require('kafka-node');
const debug = require('debug')('microed:microed');
const levelup = require('levelup');
const leveldown = require('leveldown');

class Microed {
  static get MicroedObserver () {
    return require('./observer');
  }

  static get microedMiddleware () {
    return require('./middleware');
  }

  constructor (options) {
    this.options = options;
    this.producer = this.createProducer();
    this.consumers = [];

    let { dataDir = './.microed' } = options || {};
    this.dataDir = dataDir;
    this.db = levelup(leveldown(this.dataDir));
    this.index = 0;
    this.unwritten = [];
    // this.messages = [];
  }

  observe (topic, callback) {
    let consumer = this.consumers.find(consumer => consumer.topic === topic);
    if (!consumer) {
      let client = this.createClient();
      let opts = {
        // autoCommit: false,
      };

      consumer = new Consumer(client, [ { topic, partition: 0 } ], opts);

      consumer.topic = topic;
      consumer.observers = [];

      consumer.on('error', () => {
        // noop
        // debug('Consumer error', err);
      });

      client.on('connect', () => {
        clearTimeout(consumer.tDebounceResume);
        consumer.tDebounceResume = setTimeout(() => {
          // consumer.setOffset(topic, 0, 0);
          consumer.resume();
        }, 1000);
      });

      consumer.on('message', async message => {
        consumer.pause();

        if (topic === message.topic) {
          let value = JSON.parse(message.value);
          let result = Object.assign({}, message, { value });

          await Promise.all(consumer.observers.map(observe => observe(result)));
        }

        // await new Promise(resolve => {
        //   consumer.commit(resolve);
        // });

        consumer.resume();
      });

      this.consumers.push(consumer);
    }

    consumer.observers.push(callback);
  }

  unobserve (topic, callback) {
    if (!topic) {
      this.consumers.forEach(consumer => {
        clearTimeout(consumer.tDebounceResume);
        consumer.close();
        consumer.client.close();
      });
      this.consumers = [];
      return;
    }

    let consumerIndex = this.consumers.findIndex(consumer => consumer.topic === topic);
    if (consumerIndex === -1) {
      return;
    }

    let consumer = this.consumers[consumerIndex];

    if (callback) {
      let index = consumer.observers.findIndex(o => o === callback);
      if (index !== -1) {
        consumer.observers.splice(index, 1);
      }
    } else {
      consumer.observers = [];
    }

    if (consumer.observers.length === 0) {
      this.consumers.splice(consumerIndex, 1);

      clearTimeout(consumer.tDebounceResume);
      consumer.close();
      consumer.client.close();
    }
  }

  async send (topic, value) {
    await this.putMessage({ topic, value });

    if (!this.producer.ready) {
      return;
    }

    this.sendAllMessages();
  }

  async putMessage (m) {
    if (this.sending) {
      this.unwritten.push(m);
      // console.log('put unwritten')
      return;
    }

    let batch = this.db.batch();
    if (this.unwritten.length) {
      this.unwritten.forEach(m => {
        batch = batch.put(this.index++, JSON.stringify(m));
      });
      this.unwritten = [];
    }

    batch = batch.put(this.index++, JSON.stringify(m));

    await batch.write();
    // console.log('put written')
  }

  getMessages () {
    return new Promise((resolve, reject) => {
      let messages = [];
      let stream = this.db.createReadStream({ limit: 1 });
      stream.on('data', ({ key, value }) => {
        messages.push({
          key,
          value: JSON.parse(value),
        });
      });

      stream.on('error', err => {
        reject(err);
      });

      stream.on('end', () => {
        resolve(messages);
      });
    });
  }

  async destroy () {
    await this.db.close();

    let closables = [ this.producer, ...this.consumers ];
    await Promise.all(closables.map(closable => {
      return new Promise(resolve => {
        closable.close(resolve);
        closable.client.close();
      });
    }));
  }

  async sendAllMessages () {
    if (this.sending) {
      return;
    }

    this.sending = true;

    while (true) {
      try {
        let messages = await this.getMessages();

        if (messages.length === 0) {
          break;
        }

        // console.log('written messages', messages);
        await this.sendMessages(messages.map(m => m.value));

        await this.deleteMessages(messages);
      } catch (err) {
        break;
      }
    }

    let messages = await this.getMessages();
    if (messages.length === 0) {
      // drained
      this.index = 0;
    }

    if (this.unwritten.length) {
      try {
        await this.sendMessages(this.unwritten);
        this.unwritten = [];
      } catch (err) {
        // noop
      }
    }

    this.sending = false;
  }

  async sendMessages (messages) {
    let topicGroups = {};

    messages.forEach(({ topic, value }) => {
      let topicGroup = topicGroups[topic] = topicGroups[topic] || { topic, messages: [] };
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
      this.producer.send(payloads, (err, data) => {
        if (err) {
          return reject(err);
        }

        resolve(data);
      });
    });
  }

  async deleteMessages (messages) {
    let batch = this.db.batch();

    messages.forEach(({ key }) => {
      batch = batch.del(key);
    });

    await batch.write();
  }

  createProducer () {
    let client = this.createClient();

    let producer = new HighLevelProducer(client);

    producer.on('error', () => {
      // noop
      // debug('Producer error', err);
    });

    producer.once('ready', () => {
      this.sendAllMessages();
    });

    return producer;
  }

  createClient () {
    let client = new KafkaClient(this.options);

    client.on('error', () => {
      // noop
      // debug('Client error', err);
    });

    return client;
  }
}

module.exports = Microed;
