const Microed = require('..');

let consumer = new Microed({ dataDir: './.microed-consumer' });

consumer.observe('foo', ({ value }) => {
  console.info('consume', value);
  // await new Promise(resolve => setTimeout(resolve, 300));
});
