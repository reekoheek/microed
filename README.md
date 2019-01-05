# Microed

Microservices Event Driven library

## Usage

```js
const Microed = require('microed');

const INTERVAL = 100;

let producer = new Microed({ dataDir: './.microed-producer' });
let consumer = new Microed({ dataDir: './.microed-consumer' });

function send () {
  let value = { date: new Date() };
  console.info('produce', value);
  producer.send('foo', value);
}

function repeatSend () {
  send();
  setTimeout(repeatSend, INTERVAL);
}

repeatSend();

consumer.observe('foo', ({ value }) => {
  console.info('consume', value);
});
```

## Test

- Set in `/etc/hosts` `127.0.0.1 kafka`
- `docker-compose -f docker-compose.kafka.yml up`
- `npm test`
