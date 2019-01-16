const mkdirp = require('./mkdirp');
const path = require('path');
const fs = require('fs');
const debug = require('debug')('microed:lib:queue');
const { promisify } = require('util');
const { Mutex } = require('await-semaphore');
const [ copyFile, unlink, stat ] = [
  promisify(fs.copyFile),
  promisify(fs.unlink),
  promisify(fs.stat),
];

class Queue {
  constructor ({ dataDir }) {
    if (!dataDir) {
      throw new Error('Queue must specify data dir');
    }

    if (dataDir[0] !== '/') {
      dataDir = path.resolve(process.cwd(), dataDir);
    }

    this.dataDir = dataDir;
    mkdirp(dataDir);

    this.file = path.join(this.dataDir, 'queue');
    this.mutex = new Mutex();
    this.fetchPosition = 0;
    this.fetchItems = [];

    this.putItems = [];
    this.flushing = false;
  }

  put ({ topic, value }) {
    this.putItems.push({ topic, value });

    this.flush();
  }

  async flush (force) {
    if (!force && this.flushing) {
      return;
    }

    this.flushing = true;
    await this.mutex.use(async () => {
      if (this.putItems.length === 0) {
        return;
      }

      debug('Persisting...');

      await new Promise(resolve => {
        let ws = fs.createWriteStream(this.file, { flags: 'a' });
        ws.on('close', resolve);

        let item;
        while ((item = this.putItems.shift())) {
          ws.write(JSON.stringify(item) + '\n');
        }
        ws.end();
      });
    });
    this.flushing = false;
  }

  fetch (limit = 100) {
    return this.mutex.use(async () => {
      if (this.fetchItems.length) {
        return this.fetchItems;
      }

      let items = [];
      let position = 0;
      await new Promise(async (resolve, reject) => {
        try {
          await stat(this.file);
        } catch (err) {
          if (err.code !== 'ENOENT') {
            return reject(err);
          }
        }

        let rs = fs.createReadStream(this.file);
        rs.on('readable', () => {
          let chunk;
          try {
            while ((chunk = rs.read())) {
              chunk.toString().split('\n').forEach(line => {
                items.push(JSON.parse(line));
                position += line.length + 1;

                if (items.length >= limit) {
                  rs.close();
                }
              });
            }
          } catch (err) {
            rs.close();
          }
        });
        rs.on('error', reject);
        rs.on('close', resolve);
      });

      this.fetchItems = items;
      this.fetchPosition = position;
    });
  }

  async commit () {
    if (this.fetchPosition === 0) {
      return;
    }

    await this.mutex.use(async () => {
      let tmpFile = this.file + '.tmp';

      await new Promise(resolve => {
        let rs = fs.createReadStream(this.file, { start: this.fetchPosition });
        let ws = fs.createWriteStream(tmpFile);
        rs.pipe(ws);
        ws.on('close', resolve);
      });

      this.fetchItems = [];
      this.fetchPosition = 0;

      await copyFile(tmpFile, this.file);
      await unlink(tmpFile);
    });
  }

  async close () {
    await this.flush(true);
  }
}

module.exports = Queue;
