const mkdirp = require('./mkdirp');
const path = require('path');
const fs = require('fs');
const debug = require('debug')('microed:lib:queue');
const { promisify } = require('util');
const { Mutex } = require('await-semaphore');
const [ open, close, read, write, copyFile, unlink ] = [
  promisify(fs.open),
  promisify(fs.close),
  promisify(fs.read),
  promisify(fs.write),
  promisify(fs.copyFile),
  promisify(fs.unlink),
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
  }

  async put ({ topic, value }) {
    await this.mutex.use(async () => {
      try {
        debug('Persisting...');
        let fd = await open(this.file, 'a');
        await write(fd, JSON.stringify({ topic, value }) + '\n');
        await close(fd);
      } catch (err) {
        debug('Persist caught err', err);
      }
    });
  }

  fetch (limit = 1000) {
    return this.mutex.use(async () => {
      if (this.fetchItems.length) {
        return this.fetchItems;
      }

      let items = [];
      let position = 0;
      let fd = await open(this.file, 'r');
      try {
        let line = '';
        while (1) {
          let buf = Buffer.alloc(1);
          let { bytesRead } = await read(fd, buf, 0, buf.length, position);
          let bufStr = buf.toString();

          if (bytesRead === 0) {
            break;
          }

          position += bytesRead;

          if (bufStr === '\n') {
            try {
              let item = {
                ...JSON.parse(line),
              };
              items.push(item);
              line = '';

              if (items.length >= limit) {
                break;
              }
            } catch (err) {
              break;
            }
          } else {
            line += bufStr;
          }
        }

        this.fetchItems = items;
        this.fetchPosition = position;

        return items;
      } catch (err) {
        debug('Persist caught err', err);
      } finally {
        await close(fd);
      }
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
    // noop
  }
}

module.exports = Queue;
