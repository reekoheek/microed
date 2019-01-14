const { Consumer } = require('..');

let consumer = new Consumer({ dataDir: './.microed-consumer' });

consumer.observe('foo', ({ value }) => {
  console.info('consume', value);
  // await new Promise(resolve => setTimeout(resolve, 2000));
});

process.on('SIGINT', async () => {
  console.info('destroying');
  await consumer.destroy();
  console.info('destroyed');
  process.exit();
});
