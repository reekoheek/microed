const { Producer, Consumer, Observer } = require('..');
const { Manager } = require('node-norm');
const rmdir = require('./_lib/rmdir');
const path = require('path');

const PRODUCER_DATA_DIR = path.join(process.cwd(), '.microed');

describe('norm addons', () => {
  beforeEach(async () => {
    await rmdir(PRODUCER_DATA_DIR);
  });

  afterEach(async () => {
    await rmdir(PRODUCER_DATA_DIR);
  });

  it('send update as event', async () => {
    let producer = new Producer({ dataDir: PRODUCER_DATA_DIR });
    let consumer = new Consumer();

    try {
      let manager = createManager([
        {
          name: 'foo',
          observers: [
            new Observer(),
          ],
        },
      ]);

      await manager.runSession(async session => {
        session.state.microedProducer = producer;

        let { rows: [ row ] } = await session.factory('foo')
          .insert({ name: 'foo1' })
          .save();

        await session.factory('foo', row.id)
          .set({ name: 'foo2', desc: 'Foo2' })
          .save();

        await session.factory('foo', row.id).delete();
      });

      await new Promise(resolve => {
        let hit = 0;
        consumer.observe('foo', x => {
          // console.log(x);

          hit++;
          if (hit >= 3) {
            resolve();
          }
        });
      });
    } finally {
      await consumer.destroy();
      await producer.destroy();
    }
  }).timeout(30000);
});

function createManager (schemas = []) {
  let manager = new Manager({
    connections: [
      {
        adapter: require('node-norm/adapters/memory'),
        data: {},
        schemas,
      },
    ],
  });

  return manager;
}
