const fs = require('fs');
const path = require('path');

module.exports = function mkdirp (p) {
  if (p.charAt(0) !== '/') {
    throw new Error('Relative path: ' + p);
  }

  if (p === '/') {
    return;
  }

  if (exists(p)) {
    return;
  }

  let ps = path.normalize(p).split('/');
  try {
    let parentPath = ps.slice(0, -1).join('/') || '/';
    mkdirp(parentPath);
    fs.mkdirSync(p);
  } catch (err) {
    if (err.code === 'EEXIST') {
      return;
    }

    throw err;
  }
};

function exists (p) {
  try {
    fs.statSync(p);
    return true;
  } catch (err) {
    return false;
  }
}
