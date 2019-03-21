const { ConsumerGroupStream } = require('kafka-node');
const debug = require('debug')('microed:consumer');

class Consumer {
  constructor ({ kafkaHost } = {}) {
    this.kafkaHost = kafkaHost || 'localhost:9092';

    this.consumers = [];
  }

  observe (topic, callback) {
    this.getConsumer(topic).observers.push(callback);
  }

  unobserve (topic, callback) {
    if (!topic) {
      this.consumers.forEach(consumer => {
        clearTimeout(consumer.tDebounceResume);
        consumer.close();
        // consumer.client.close();
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
      // consumer.client.close();
    }
  }

  async destroy () {
    await Promise.all(this.consumers.map(consumer => {
      return new Promise(resolve => {
        consumer.close(resolve);
      });
    }));
    debug('Consumer destroyed');
  }

  getConsumer (topic) {
    let consumer = this.consumers.find(consumer => consumer.topic === topic);

    if (!consumer) {
      consumer = this.createConsumer(topic);

      this.consumers.push(consumer);
    }

    return consumer;
  }

  createConsumer (topic) {
    let consumer = new ConsumerGroupStream({
      kafkaHost: this.kafkaHost,
      ssl: false,
      // groupId: groupId,
      sessionTimeout: 30000,
      protocol: ['roundrobin'],
      fromOffset: 'earliest', // latest
      migrateHLC: false,
      migrateRolling: false,
      fetchMaxBytes: 1024 * 100,
      fetchMinBytes: 1,
      fetchMaxWaitMs: 100,
      autoCommit: true,
      autoCommitIntervalMs: 5000,
      connectRetryOptions: {
        retries: 1000, // overwritten by forever
        factor: 3,
        minTimeout: 1000, // 1 sec
        maxTimeout: 30000, // 30 secs
        randomize: true,
        forever: true,
        unref: false,
      },
      encoding: 'buffer',
      keyEncoding: 'buffer',
    }, topic);

    consumer.topic = topic;
    consumer.observers = [];

    consumer.on('connect', () => {
      debug(`Consumer (${topic}) connected`);
      consumer.resume();
    });

    consumer.on('error', err => {
      debug(`Consumer (${topic}) error`, err);
    });

    consumer.on('offsetOutOfRange', err => {
      debug(`Consumer (${topic}) offsetOutOfRange`, err);
    });

    let consume = async message => {
      try {
        consumer.pause();

        if (topic === message.topic) {
          let value = JSON.parse(message.value);
          let result = Object.assign({}, message, { value });

          await Promise.all(consumer.observers.map(observe => observe(result)));
        }

        // await new Promise(resolve => {
        //   consumer.commit(resolve);
        // });
      } catch (err) {
        debug('Consumer#onData got error', err);
      } finally {
        consumer.resume();
      }
    };
    consumer.on('data', consume);
    // consumer.on('message', consume);

    return consumer;
  }
}

module.exports = Consumer;
