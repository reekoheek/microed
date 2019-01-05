const Microed = require('..');
const assert = require('assert');
const rmdir = require('./_lib/rmdir');

describe('Microed', () => {
  beforeEach(async () => {
    await rmdir('./.microed-producer');
    await rmdir('./.microed-consumer');
  });

  afterEach(async () => {
    await rmdir('./.microed-producer');
    await rmdir('./.microed-consumer');
  });

  describe('cases', () => {
    it('produce and consume topic', async () => {
      let producer = new Microed({ dataDir: './.microed-producer' });
      let consumer = new Microed({ dataDir: './.microed-consumer' });

      try {
        await new Promise((resolve, reject) => {
          try {
            let fooCount = 0;
            let barCount = 0;

            consumer.observe('foo', foo => {
              // console.log('got foo', foo);
              fooCount++;

              if (fooCount >= 1 && barCount >= 2) {
                resolve();
              }
            });

            consumer.observe('bar', bar => {
              // console.log('got bar', bar);
              barCount++;

              if (fooCount >= 1 && barCount >= 2) {
                resolve();
              }
            });

            producer.send('foo', { name: 'foo1' });
            producer.send('bar', { name: 'bar1' });
            producer.send('bar', { name: 'bar2' });
          } catch (err) {
            reject(err);
          }
        });
      } finally {
        await producer.destroy();
        await consumer.destroy();
      }
    });
  });

  describe('#observe()', () => {
    it('add observer', async () => {
      let microed = new Microed({ dataDir: './.microed-consumer' });
      try {
        microed.observe('foo', () => {});

        assert.strictEqual(microed.consumers.length, 1);
        assert.strictEqual(microed.consumers[0].observers.length, 1);
      } finally {
        await microed.destroy();
      }
    });
  });

  describe('#unobserve()', () => {
    it('remove single observer', async () => {
      let microed = new Microed({ dataDir: './.microed-consumer' });
      try {
        let observer1 = () => {};
        let observer2 = () => {};
        microed.observe('foo', observer1);
        microed.observe('foo', observer2);

        microed.unobserve('foo', observer2);

        assert.strictEqual(microed.consumers.length, 1);
        assert.strictEqual(microed.consumers[0].observers.length, 1);
        assert.strictEqual(microed.consumers[0].observers[0], observer1);

        microed.unobserve('foo', observer1);

        assert.strictEqual(microed.consumers.length, 0);
      } finally {
        await microed.destroy();
      }
    });

    it('remove all observers by topic', async () => {
      let microed = new Microed({ dataDir: './.microed-consumer' });
      try {
        let observer1 = () => {};
        let observer2 = () => {};
        microed.observe('foo', observer1);
        microed.observe('foo', observer2);

        microed.unobserve('foo');

        assert.strictEqual(microed.consumers.length, 0);
      } finally {
        await microed.destroy();
      }
    });

    it('remove all observers', async () => {
      let microed = new Microed({ dataDir: './.microed-consumer' });
      try {
        let observer1 = () => {};
        let observer2 = () => {};
        microed.observe('foo', observer1);
        microed.observe('bar', observer2);

        microed.unobserve();

        assert.strictEqual(microed.consumers.length, 0);
      } finally {
        await microed.destroy();
      }
    });
  });
});
