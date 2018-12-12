const Microed = require('..');

let consumer = new Microed();

consumer.observe('foo', ({ value }) => {
  console.info('consume', value);
});
