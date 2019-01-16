const { Producer, Consumer } = require('..');
const rmdir = require('./_lib/rmdir');
const path = require('path');

const PRODUCER_DATA_DIR = path.join(process.cwd(), '.microed');

describe('Case', () => {
  beforeEach(async () => {
    await rmdir(PRODUCER_DATA_DIR);
  });

  afterEach(async () => {
    await rmdir(PRODUCER_DATA_DIR);
  });

  it('produce and consume topic', async () => {
    let producer = new Producer({ dataDir: PRODUCER_DATA_DIR });
    let consumer = new Consumer();

    try {
      await new Promise((resolve, reject) => {
        try {
          let fooCount = 0;
          let barCount = 0;

          consumer.observe('foo', foo => {
            fooCount++;

            // console.log('got foo', foo, fooCount, barCount);
            if (fooCount >= 1 && barCount >= 2) {
              resolve();
            }
          });

          consumer.observe('bar', bar => {
            barCount++;

            // console.log('got bar', bar, fooCount, barCount);
            if (fooCount >= 1 && barCount >= 2) {
              resolve();
            }
          });

          producer.send('foo', { name: 'foo1' });
          // console.log('send foo');
          producer.send('bar', { name: 'bar1' });
          // console.log('send bar');
          producer.send('bar', { name: 'bar2' });
          // console.log('send bar');
        } catch (err) {
          reject(err);
        }
      });
    } finally {
      await producer.destroy();
      await consumer.destroy();
    }
  }).timeout(30000);
});
