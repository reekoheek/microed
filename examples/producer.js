const Microed = require('..');

const INTERVAL = 100;

let producer = new Microed({ dataDir: './.microed-producer' });

function send () {
  let value = { date: new Date() };

  console.info('produce', value);

  producer.send('foo', value);
}

function trySend () {
  send();

  setTimeout(trySend, INTERVAL);
}

trySend();

// producer.sending = true;
// send();
// send();
// send();
// producer.sending = false;
// send();
// send();
