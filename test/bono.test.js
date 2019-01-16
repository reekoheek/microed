const { middleware } = require('..');
const Bundle = require('bono');
const test = require('supertest');
const assert = require('assert');

describe('bono addons', () => {
  it('add microed to ctx state', async () => {
    let bundle = new Bundle();
    let producer = {
      destroy () {
        // noop
      },
    };

    bundle.use(middleware({ producer }));

    try {
      bundle.get('/', ctx => {
        assert(ctx.state.microedProducer);
        assert.strictEqual(ctx.state.microedProducer, producer);
      });

      await test(bundle.callback())
        .get('/')
        .expect(200);
    } finally {
      await producer.destroy();
    }
  });
});
