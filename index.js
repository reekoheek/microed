const { KafkaClient, Consumer, HighLevelProducer } = require('kafka-node');
const debug = require('debug')('microed:microed');

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
    this.messages = [];
  }

  observe (topic, callback) {
    let consumer = this.consumers.find(consumer => consumer.topic === topic);
    if (!consumer) {
      let client = this.createClient();
      consumer = new Consumer(client, [ { topic, partition: 0 } ]);

      consumer.topic = topic;
      consumer.observers = [];

      consumer.on('error', err => {
        debug('Consumer error', err);
      });

      consumer.on('message', async message => {
        if (topic !== message.topic) {
          return;
        }

        let value = JSON.parse(message.value);
        let result = Object.assign({}, message, { value });

        await Promise.all(consumer.observers.map(observe => observe(result)));
      });

      this.consumers.push(consumer);
    }

    consumer.observers.push(callback);
  }

  unobserve (topic, callback) {
    if (!topic) {
      this.consumers.forEach(consumer => {
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

      consumer.close();
      consumer.client.close();
    }
  }

  send (topic, value) {
    this.messages.push({ topic, value });

    if (!this.producer.ready) {
      return;
    }

    this.sendMessages();
  }

  async destroy () {
    let closables = [ this.producer, ...this.consumers ];
    await Promise.all(closables.map(closable => {
      return new Promise(resolve => {
        closable.close(resolve);
        closable.client.close();
      });
    }));
  }

  sendMessages () {
    if (this.messages.length === 0) {
      return;
    }

    let topicGroups = {};
    this.messages.forEach(({ topic, value }) => {
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

    this.producer.send(payloads, (err, data) => {
      if (err) {
        debug('Send error', err);
        return;
      }

      debug('Sent', data);
    });

    this.messages = [];
  }

  createProducer () {
    let client = this.createClient();

    let producer = new HighLevelProducer(client);

    producer.on('error', err => {
      debug('Producer error', err);
    });

    producer.once('ready', () => {
      this.sendMessages();
    });

    return producer;
  }

  createClient () {
    let client = new KafkaClient(this.options);

    client.on('error', err => {
      debug('Client error', err);
    });

    return client;
  }
}

module.exports = Microed;
