const { Consumer } = require('..');
const assert = require('assert');

describe('Consumer', () => {
  describe('#observe()', () => {
    it('add observer', async () => {
      let microed = new Consumer();
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
      let microed = new Consumer();
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
      let microed = new Consumer();
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
      let microed = new Consumer();
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
