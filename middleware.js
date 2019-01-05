const Microed = require('.');

module.exports = function microedMiddleware ({ producer, options } = {}) {
  let microed = producer || new Microed(options);

  return async (ctx, next) => {
    ctx.state.microed = microed;

    await next();
  };
};
