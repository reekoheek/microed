const Microed = require('..');

let producer = new Microed();

function trySend () {
  let value = { date: new Date() };

  console.info('produce', value);

  producer.send('foo', value);

  setTimeout(trySend, 1000);
}

trySend();
