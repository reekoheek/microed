const Microed = require('..');
const { microedMiddleware } = Microed;
const Bundle = require('bono');
const test = require('supertest');
const assert = require('assert');
const rmdir = require('./_lib/rmdir');

describe('bono addons', () => {
  beforeEach(async () => {
    await rmdir('./.microed-producer');
  });

  afterEach(async () => {
    await rmdir('./.microed-producer');
  });

  it('add microed to ctx state', async () => {
    let bundle = new Bundle();
    let microed = new Microed({ dataDir: './.microed-producer' });

    bundle.use(microedMiddleware({ microed }));

    try {
      bundle.get('/', ctx => {
        assert(ctx.state.microed);
        assert.strictEqual(ctx.state.microed, microed);
      });

      await test(bundle.callback())
        .get('/')
        .expect(200);
    } finally {
      microed.destroy();
    }
  });
});
