class Observer {
  constructor ({ topic, producer } = {}) {
    this.topic = topic;
    this.producer = producer;
  }

  async insert ({ query }, next) {
    let producer = this.getProducer(query);
    let topic = this.getTopic(query);
    let mode = 'insert';

    await next();

    query.rows.forEach(row => {
      producer.send(topic, { mode, row });
    });
  }

  async update ({ query }, next) {
    let producer = this.getProducer(query);
    let topic = this.getTopic(query);
    let mode = 'update';

    let rows = await query.clone().all();

    await next();

    rows.forEach(async row => {
      row = await query.session.factory(query.schema.name, row.id).single();
      producer.send(topic, { mode, row });
    });
  }

  async delete ({ query }, next) {
    let producer = this.getProducer(query);
    let topic = this.getTopic(query);
    let mode = 'delete';

    let rows = await query.clone().all();

    await next();

    rows.forEach(row => {
      producer.send(topic, { mode, row });
    });
  }

  getProducer (query) {
    let producer = this.producer || query.session.state.microedProducer;
    if (!producer) {
      throw new Error('Unspecifed producer, you might want to use producer middleware!');
    }

    return producer;
  }

  getTopic (query) {
    return this.topic || query.schema.name;
  }
}

module.exports = Observer;
