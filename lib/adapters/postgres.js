var pg = require('pg');
var Q = require('q');

module.exports = PostgresAdapter;

var pgConfig;

var parseValues = function (values) {
  if (!values) {
    return [];
  }
  return values.map(function (val) {
    if (Array.isArray(val)) {
      val = val.map(function (item) {
        if (Array.isArray(item)) {
          return parseValues(item);
        }
        else {
          if (item.replace) {
            item = item.replace(/,/g,'\\,');
            item = item.replace(/\"/g,'\\"');
          }
          return item;
        }
      });
      val = '{' + val.join(',') + '}';
    }
    return val;
  });
};

var executeTransaction = function (queries, cb) {
  var client, error;
  return Q.ninvoke(pg, 'connect', pgConfig)
    .then(function (c) {
      client = c;
      client.pauseDrain();
      return Q.ninvoke(client, 'query', 'BEGIN');
    })
    .then(function () {
      return Q.all(
        queries.map(function (query) {
          var values = parseValues(query[1]);
          return Q.ninvoke(client, 'query', query[0], values);
        })
      );
    })
    .then(function () {
      return Q.ninvoke(client, 'query', 'COMMIT');
    }, function (err) {
      error = err;
      return Q.ninvoke(client, 'query', 'ROLLBACK');
    })
    .then(function () {
      client.resumeDrain();
      if (error) {
        throw error;
      }
      else {
        return;
      }
    });
};

var executeStepTransaction = function (queries, cb) {
  var client, error;
  var promise = Q.ninvoke(pg, 'connect', pgConfig)
    .then(function (c) {
      client = c;
      client.pauseDrain();
      return Q.ninvoke(client, 'query', 'BEGIN');
    });

  queries.forEach(function (queryFn, idx) {
    promise = promise
      .then(function (result) {
        var query = queryFn(result);
        if (Array.isArray(query)) {
          var values = parseValues(query[1]);
          return Q.ninvoke(client, 'query', query[0], values);
        }
        else {
          //TODO: make this safe for callback style
          return query;
        }
      });
  });
  promise = promise
    .then(function (result) {
      return Q.ninvoke(client, 'query', 'COMMIT');
    }, function (err) {
      error = err;
      return Q.ninvoke(client, 'query', 'ROLLBACK');
    })
    .then(function () {
      client.resumeDrain();
      if (error) {
        throw error;
      }
      else {
        return;
      }
    });
  return promise.nodeify(cb);
};

var executeQuery = function (query, values, cb) {
  if (values && typeof values == 'function') {
    cb = values;
    values = undefined;
  }
  return Q.ninvoke(pg, 'connect', pgConfig)
    .then(function (client) {
      values = parseValues(values);
      return Q.ninvoke(client, 'query', query, values);
    })
    .nodeify(cb);
};


function PostgresAdapter (tableName, options) {
  this.tableName = tableName;
  if (options) {
    this.modelConstructor = options.modelConstructor;
    this.primaryKey = options.primaryKey || 'id';
  }
}

PostgresAdapter.prototype = Object.create(PostgresAdapter.prototype, {
  query: {
    value: function (query, values, cb) {
      return executeQuery(query, values, cb);
    }
  },

  transaction: {
    value: function (queries, cb) {
      return executeTransaction(queries, cb);
    }
  },

  stepTransaction: {
    value: function (queries, cb) {
      return executeStepTransaction(queries, cb);
    }
  },

  all: {
    value: function (cb) {
      var self = this;
      var query = 'SELECT * FROM ' + self.tableName;
      query += ' ORDER BY ' + self.primaryKey;
      return executeQuery(query)
        .then(function (result) {
          return result.rows.map(function (row) {
            return new self.modelConstructor(row);
          });
        })
        .nodeify(cb);
    }
  },

  findById: {
    value: function (id, cb) {
      var self = this;
      var query = 'SELECT * FROM ' + self.tableName +
        ' WHERE ' + self.primaryKey + ' = $1;';
      return executeQuery(query, [id])
        .then(function (result) {
          if (result.rows.length > 0) {
            return new self.modelConstructor(result.rows[0]);
          }
          else {
            return undefined;
          }
        });
    }
  },

  insertQuery: {
    value: function (target) {
      var values = [];
      var self = this;
      var query = 'INSERT INTO ' + self.tableName + '(';
      Object.keys(target).forEach(function (key, idx, keys) {
        var val = target[key];
        values.push(val);
        query += key;
        if (idx !== keys.length - 1) {
          query += ', ';
        }
        else {
          query += ')';
        }
      });
      query += ' VALUES(';
      values.forEach(function (val, idx) {
        var valId = idx + 1;
        query += '$' + valId;
        if (idx !== values.length - 1) {
          query += ', ';
        }
        else {
          query += ')';
        }
      });
      if (self.primaryKey) {
        query += ' RETURNING ' + self.primaryKey;
      }
      return [query, values];
    }
  },

  insert: {
    value: function (target, cb) {
      var self = this;
      var statement = self.insertQuery(target);
      var query = statement[0], values = statement[1];
      return executeQuery(query, values)
        .then(function (result) {
          return result.rows[0][self.primaryKey];
        })
        .nodeify(cb);
    }
  },

  updateQuery: {
    value: function (target) {
      var values = [];
      var self = this;
      var query = 'UPDATE ' + self.tableName + ' SET ';
      Object.keys(target).forEach(function (key, idx, keys) {
        var val = target[key];
        values.push(val);
        query += ' ' + key + ' = $' + values.length;
        if (idx !== keys.length -1) {
          query += ',';
        }
      });
      if (self.primaryKey) {
        values.push(target[self.primaryKey]);
        query += ' WHERE ' + self.primaryKey + ' = $' + values.length;
        query += ' RETURNING ' + self.primaryKey;
      }
      return [query, values];
    }
  },

  update: {
    value: function (target, cb) {
      var self = this;
      var statement = self.updateQuery(target);
      var query = statement[0], values = statement[1];
      return executeQuery(query, values)
        .then(function (result) {
          return result.rows[0][self.primaryKey];
        })
        .nodeify(cb);
    }
  },

  del: {
    value: function (id, cb) {
      var self = this;
      var query = 'DELETE FROM ' + self.tableName + ' WHERE ' +
        self.primaryKey + ' = $1';
      return executeQuery(query, [id], cb);
    }
  }
});

PostgresAdapter.configure = function (config) {
  pgConfig = config;
};

PostgresAdapter.addStatics = function (Model, adapter) {
  Model.query = function (query, values, cb) {
    return adapter.query(query, values, cb);
  };

  Model.transaction = function (queries, cb) {
    return adapter.transaction(queries, cb);
  };

  Model.stepTransaction = function (queries, cb) {
    return adapter.stepTransaction(queries, cb);
  };

  Model.all = function (cb) {
    return adapter.all(cb);
  };
  Model.findById = function (id, cb) {
    return adapter.findById(id, cb);
  };

  if (Model.fields) {
    Model.namespaceFields = function (selector, addedFields) {
      if (!selector) {
        selector = adapter.tableName;
      }

      var fields = Model.fields();
      if (addedFields) {
        addedFields.forEach(function (f) { fields.push(f); } );
      }

      return fields.map(function (field) {
        return selector + '.' + field + ' AS ' + adapter.tableName + '_' + field;
      });
    };
    Model.parseRow = function (row) {
      var result = {};
      Object.keys(row).forEach(function (key) {
        var namespace = new RegExp(adapter.tableName + '_(\\w+)');
        var match = key.match(namespace);
        if (match) {
          var newKey = match[1];
          if (Model.fields().indexOf(newKey) > -1) {
            result[newKey] = row[key];
          }
        }
      });
      return new Model(result);
    };
  }
};

PostgresAdapter.addDelegates = function (Model, adapter) {
  Model.prototype.save = function (cb) {
    if (this.id) {
      return adapter.update(this._attributes, cb);
    }
    else {
      return adapter.insert(this._attributes, cb);
    }
  };

  Model.prototype.delete = function (cb) {
    console.log('DELETE on PG adapter is deprecated');
    console.trace();
    return adapter.del(this.id);
  };

  Model.prototype.del = function (cb) {
    return adapter.del(this.id, cb);
  };

};

