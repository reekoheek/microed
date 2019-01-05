const { spawn } = require('child_process');

module.exports = function rmdir (p) {
  return new Promise((resolve, reject) => {
    let proc = spawn('rm', [ '-rf', p ]);
    proc.on('close', resolve);
  });
};
