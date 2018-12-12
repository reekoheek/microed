const Microed = require('.');

module.exports = function microedMiddleware (options = {}) {
  let { producer } = options;
  let microed = producer || new Microed(options);

  return async (ctx, next) => {
    ctx.state.microed = microed;

    await next();
  };
};
