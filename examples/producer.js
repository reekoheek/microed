const { Producer } = require('..');

const INTERVAL = 100;

let index = Number(process.argv[2]) || 0;
let producer = new Producer({ dataDir: './.microed-producer' });

function send () {
  let value = { date: new Date(), index: index++ };

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

process.on('SIGINT', async () => {
  console.info('destroying');
  await producer.destroy();
  console.info('destroyed');
  process.exit();
});
