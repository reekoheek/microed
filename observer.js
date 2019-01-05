class Observer {
  constructor ({ topic, microed } = {}) {
    this.topic = topic;
    this.microed = microed;
  }

  async insert ({ query }, next) {
    let microed = this.getMicroed(query);
    let topic = this.getTopic(query);
    let mode = 'insert';

    await next();

    query.rows.forEach(row => {
      microed.send(topic, { mode, row });
    });
  }

  async update ({ query }, next) {
    let microed = this.getMicroed(query);
    let topic = this.getTopic(query);
    let mode = 'update';

    let rows = await query.clone().all();

    await next();

    rows.forEach(async row => {
      row = await query.session.factory(query.schema.name, row.id).single();
      microed.send(topic, { mode, row });
    });
  }

  async delete ({ query }, next) {
    let microed = this.getMicroed(query);
    let topic = this.getTopic(query);
    let mode = 'delete';

    let rows = await query.clone().all();

    await next();

    rows.forEach(row => {
      microed.send(topic, { mode, row });
    });
  }

  getMicroed (query) {
    let microed = this.microed || query.session.state.microed;
    if (!microed) {
      throw new Error('Unspecifed microed, you might want to use microed middleware!');
    }

    return microed;
  }

  getTopic (query) {
    return this.topic || query.schema.name;
  }
}

module.exports = Observer;
