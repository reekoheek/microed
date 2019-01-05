module.exports = function microedMiddleware ({ microed } = {}) {
  if (!microed) {
    throw new Error('Undefined microed instance');
  }

  return async (ctx, next) => {
    ctx.state.microed = microed;

    await next();
  };
};
