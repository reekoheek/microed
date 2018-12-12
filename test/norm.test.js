const Microed = require('..');
const { MicroedObserver } = Microed;
const { Manager } = require('node-norm');

describe('norm addons', () => {
  it('send update as event', async () => {
    let consumer = new Microed();
    let producer = new Microed();

    try {
      let manager = createManager([
        {
          name: 'foo',
          observers: [
            new MicroedObserver(),
          ],
        },
      ]);

      await manager.runSession(async session => {
        session.state.microed = producer;

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
      consumer.destroy();
      producer.destroy();
    }
  });
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
