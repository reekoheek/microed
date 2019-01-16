const { Consumer } = require('..');
const assert = require('assert');

describe('Consumer', () => {
  describe('#observe()', () => {
    it('add observer', async () => {
      let consumer = new Consumer();
      try {
        consumer.observe('foo', () => {});

        assert.strictEqual(consumer.consumers.length, 1);
        assert.strictEqual(consumer.consumers[0].observers.length, 1);
      } finally {
        await consumer.destroy();
      }
    });
  });

  describe('#unobserve()', () => {
    it('remove single observer', async () => {
      let consumer = new Consumer();
      try {
        let observer1 = () => {};
        let observer2 = () => {};
        consumer.observe('foo', observer1);
        consumer.observe('foo', observer2);

        consumer.unobserve('foo', observer2);

        assert.strictEqual(consumer.consumers.length, 1);
        assert.strictEqual(consumer.consumers[0].observers.length, 1);
        assert.strictEqual(consumer.consumers[0].observers[0], observer1);

        consumer.unobserve('foo', observer1);

        assert.strictEqual(consumer.consumers.length, 0);
      } finally {
        await consumer.destroy();
      }
    });

    it('remove all observers by topic', async () => {
      let consumer = new Consumer();
      try {
        let observer1 = () => {};
        let observer2 = () => {};
        consumer.observe('foo', observer1);
        consumer.observe('foo', observer2);

        consumer.unobserve('foo');

        assert.strictEqual(consumer.consumers.length, 0);
      } finally {
        await consumer.destroy();
      }
    });

    it('remove all observers', async () => {
      let consumer = new Consumer();
      try {
        let observer1 = () => {};
        let observer2 = () => {};
        consumer.observe('foo', observer1);
        consumer.observe('bar', observer2);

        consumer.unobserve();

        assert.strictEqual(consumer.consumers.length, 0);
      } finally {
        await consumer.destroy();
      }
    });
  });
});
