module.exports = function microedMiddleware ({ producer, consumer }) {
  return async (ctx, next) => {
    ctx.state.microedProducer = producer;
    ctx.state.microedConsumer = consumer;

    await next();
  };
};
