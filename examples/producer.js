const { Producer } = require('..');

const INTERVAL = 0;

let index = Number(process.argv[2]) || 0;
let producer = new Producer({ dataDir: './.microed-producer' });

function send () {
  let value = { date: new Date(), index: index++ };

  console.info('produce', value);

  producer.send('foo', value);
}

let sendT;
function trySend () {
  // for (let i = 0; i < 20; i++) {
  send();
  // }

  sendT = setTimeout(trySend, INTERVAL);
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
  clearTimeout(sendT);
  await producer.destroy();
  console.info('destroyed');
  process.exit();
});
