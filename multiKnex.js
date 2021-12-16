var _ = require('lodash');

var knex = require('knex');
var async = require('async');

var MultiTransaction = require('./multiTransaction');

var adler32 = require('../utils').adler32;

var expand = function(data) {
    var counter = 0;
    return data.sql.replace(/\?/g, function() {
        var obj = data.bindings[counter++];
        return (obj instanceof Object) ? JSON.stringify(obj) : obj;
    });
};

var MultiKnex = module.exports = function(dbConfig, logger) {
    this.config = dbConfig;
    this.logger = logger;
    this.shards = this.shardClients(dbConfig);
    this.shardIds = Object.keys(this.shards);
    this.shardCount = this.shardIds.length;
};

MultiKnex.createConnectionString = function(config, database) {
    return [
        'postgres://', config.user, (config.password ? ':' + config.password : ''),
        '@', config.host, (config.port ? ':' + config.port : ''),
        '/', database
    ].join('');
};

MultiKnex.createKnexInstance = function(config, database, logger) {
    var instance = knex({
        client: 'pg',
        connection: MultiKnex.createConnectionString(config, database),
        pool: config.pool || {}
    });
    instance.on('query', function(data) {
        logger.info("EXECUTING ON " + database + " : " + expand(data));
    });
    return instance;
};

MultiKnex.prototype.shardFor = function(key) {
    return this.shardIds[adler32(key.toString()) % this.shardCount];
};

MultiKnex.prototype.shardsForKeys = function(keys) {
    var shardIds = keys.map(function(key) {
        return this.shardFor(key);
    }, this);
    return _.unique(shardIds);
};

MultiKnex.prototype.using = function(key) {
    return this.shards[this.shardFor(key)];
};

MultiKnex.prototype.usingShard = function(shardId) {
    return this.shards[shardId];
};

MultiKnex.prototype.execForShard = function(shardId, query, callback) {
    query(this.shards[shardId], callback);
};

MultiKnex.prototype.exec = function(query, callback) {
    this.execForShards(this.shardIds, query, callback);
};

MultiKnex.prototype.execForShards = function(shardIds, query, callback) {
    var queriesByShards = shardIds.reduce(function(memo, shardId) {
        memo[shardId] = query;
        return memo;
    }, {});

    this.execByShards(queriesByShards, callback);
};

MultiKnex.prototype.execByShards = function(queriesByShards, callback) {
    var self = this;
    async.map(Object.keys(queriesByShards), function(shardId, cb) {
        queriesByShards[shardId](self.shards[shardId], cb);
    }, function(err, results) {
        callback(err, results && [].concat.apply([], results));
    });
};

MultiKnex.prototype.execForKeys = function(keys, query, callback) {
    this.execForShards(this.shardsForKeys(keys), query, callback);
};

MultiKnex.prototype.execForKey = function(key, query, callback) {
    this.execForShard(this.shardFor(key), query, callback);
};

MultiKnex.prototype.transactionalExecForShard = function(shardId, query, callback) {
    var self = this;
    self.shards[shardId].transaction(function(trx) {
        trx.shardId = shardId;
        query(trx, function(err, result) {
            if (err) {
                self.logger.warn("ROLLING BACK TRANSACTION ON SHARD; SOURCE: " + shardId);
                t.rollback(err);
            } else {
                self.logger.info("COMMITING TRANSACTION ON SHARD");
                t.commit();
            }
        });
    }).exec(function(err, result) {
        callback(err, result);
    });
};

MultiKnex.prototype.transactionalExecForShards = function(shardIds, query, callback) {
    var queriesByShards = shardIds.reduce(function(memo, shardId) {
        memo[shardId] = query;
        return memo;
    }, {});

    this.transactionalExecByShards(queriesByShards, callback);
};

MultiKnex.prototype.transactionalExecForKey = function(key, query, callback) {
    this.transactionalExecForShard(this.shardFor(key), query, callback);
};

MultiKnex.prototype.transactionalExecForKeys = function(keys, query, callback) {
    this.transactionalExecForShards(this.shardsForKeys(keys), query, callback);
};

MultiKnex.prototype.transactionalExec = function(query, callback) {
    this.transactionalExecForShards(this.shardIds, query, callback);
};

MultiKnex.prototype.collectQueriesByShards = function(queriesByKeys) {
    var self = this;
    var queryArraysByShards = queriesByKeys.reduce(function(memo, queryByKeys) {
        self.shardsForKeys(queryByKeys.keys).forEach(function(shardId) {
            memo[shardId] = memo[shardId] || [];
            memo[shardId].push(queryByKeys.query);
        });
        return memo;
    }, {});

    var queryByShards = {};
    _.each(queryArraysByShards, function(queryArray, shardId) {
        queryByShards[shardId] = function(knex, callback) {
            async.mapSeries(queryArray, function(query, cb) {
                query(knex, cb);
            }, callback);
        };
    });

    return queryByShards;
};

MultiKnex.prototype.transactionalExecByKeys = function(queriesByKeys, callback) {
    this.transactionalExecByShards(this.collectQueriesByShards(queriesByKeys), callback);
};

MultiKnex.prototype.transaction = function(trxCallback, resultCallback) {
    var trx = new MultiTransaction(this, resultCallback);
    trxCallback(trx);
};

MultiKnex.prototype.transactionally = function(trxCallback, callback) {
    var result;
    this.transaction(function(trx) {
        trxCallback(trx, function(err, res) {
            if (err) {
                trx.rollback(err);
            } else {
                result = res;
                trx.commit();
            }
        });
    }, function(err) {
        callback(err, result);
    });
};

MultiKnex.prototype.transactionalExecByShards = function(queriesByShards, callback) {
    var self = this;
    var transactions = {};
    var waitCount = 0;
    var trxError = null;

    var shardCount = Object.keys(queriesByShards).length;

    var wait = function(shardId, err) {
        trxError = trxError || err;
        if (++waitCount == shardCount) {
            if (trxError) {
                self.logger.warn("ROLLING BACK TRANSACTION ON SHARDS; SOURCE: " + shardId);
                _.each(transactions, function(t) { t.rollback(trxError.toString()); });
            } else {
                self.logger.info("COMMITING TRANSACTION ON SHARDS");
                _.each(transactions, function(t) { t.commit(); });
            }
        }
    };

    async.map(Object.keys(queriesByShards), function(shardId, cb) {
        var transactionalResult;
        self.shards[shardId].transaction(function(trx) {
            transactions[shardId] = trx;
            trx.shardId = shardId;
            queriesByShards[shardId](trx, function(err, result) {
                transactionalResult = result;
                wait(shardId, err);
            });
        }).exec(function(err, result) {
            cb(err, transactionalResult);
        });
    }, function(err, results) {
        callback(err, results && [].concat.apply([], results));
    });
};

MultiKnex.prototype.shardClients = function(dbConfig) {
    var self = this;
    return _.reduce(dbConfig.shards, function(result, shard, shardId) {
        var db = dbConfig.db[shard.db_cfg];
        result[shardId] = MultiKnex.createKnexInstance(db, shard.db, self.logger);
        return result;
    }, {});
};

MultiKnex.prototype.appendQuery = function(query, append) {
    if (!query) {
        return append;
    }
    return function(knex, cb) {
        query(knex, function(err, result) {
            if (err) {
                return cb(err, result);
            }
            append(knex, cb);
        });
    };
};

MultiKnex.prototype.prependQuery = function(query, prepend) {
    if (!query) {
        return prepend;
    }
    return function(knex, cb) {
        prepend(knex, function(err, result) {
            if (err) {
                return cb(err, result);
            }
            query(knex, cb);
        });
    };
};
