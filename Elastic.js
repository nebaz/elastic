const elasticsearch = require('elasticsearch');

class Elastic {

  constructor(config) {
    this.client = new elasticsearch.Client({...config});
  }

  async insertOne(index, document) {
    let id = document._id.toString();
    delete document._id;
    try {
      await this.client.create({index: index.toLowerCase(), id, body: document});
      return true;
    } catch (e) {
      console.error(e);
      return false;
    }
  }

  async insertMany(index, documents) {
    let body = [];
    for (let document of documents) {
      let _id = document._id.toString();
      delete document._id;
      body.push({index: {'_index': index.toLowerCase(), _id}});
      body.push(document);
    }
    if (!body.length) return false;
    try {
      await this.client.bulk({body});
      return true;
    } catch (e) {
      console.error(e);
      return false;
    }
  }

  async deleteIndex(index) {
    try {
      await this.client.indices.delete({index: index.toLowerCase()});
      return true;
    } catch (e) {
      console.error(e);
      return false;
    }
  }
}

module.exports = Elastic;
