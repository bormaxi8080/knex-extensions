var _ = require('lodash');
var async = require('async');
var EventEmitter = require('events').EventEmitter;
var util = require('util');

var MultiTransaction = module.exports = function(multiKnex, completeHandler) {
    this.multiKnex = multiKnex;
    this.transactions = {};
    this.openingTransactions = {};
    this.completeCount = 0;
    this.openCount = 0;
    this.trxError = null;
    this.completeHandler = completeHandler;
    this.isTransaction = true;
};

util.inherits(MultiTransaction, EventEmitter);

MultiTransaction.prototype.completedTransactions = function(shardId, err) {
    this.completeCount++;
    if (err) {
        this.trxError = this.trxError || {shard: shardId, err: err};
    }
    if (this.completeCount == this.openCount) {
        this.complete();
    }
};

MultiTransaction.prototype.openTransaction = function(shardId, callback) {
    var self = this;
    this.openingTransactions[shardId] = true;
    this.multiKnex.shards[shardId].transaction(function(trx) {
        trx.shardId = shardId;
        self.transactions[shardId] = trx;
        self.openCount++;
        self.emit('open', trx);
    }).exec(function(err) {
        self.completedTransactions(shardId, err);
    });
};

MultiTransaction.prototype.ensureTransaction = function(shardId, callback) {
    if (this.transactions[shardId]) {
        return callback(this.transactions[shardId]);
    }
    if (!this.openingTransactions[shardId]) {
        this.openTransaction(shardId);
    }
    this.on('open', function(trx) {
        if (trx.shardId === shardId) {
            callback(trx);
        }
    });
};

MultiTransaction.prototype.commit = function() {
    var self = this;
    this.multiKnex.logger.info("COMMITTING TRANSACTION ON ALL SHARDS");
    _.each(this.transactions, function(t) { t.commit(); });
};

MultiTransaction.prototype.complete = function() {
    this.removeAllListeners();
    this.completeHandler(this.trxError);
};

MultiTransaction.prototype.rollback = function(err) {
    var self = this;
    self.trxError = self.trxError || err || "Explicit rollback";
    this.multiKnex.logger.warn("ROLLING BACK TRANSACTION ON ALL SHARDS: " + self.trxError);
    _.each(self.transactions, function(t) { t.rollback(); });
};

MultiTransaction.prototype.shardFor = function(key) {
    return this.multiKnex.shardFor(key);
};

MultiTransaction.prototype.exec = function(query, callback) {
    this.execForShards(this.multiKnex.shardIds, query, callback);
};

MultiTransaction.prototype.transactionalExec = function(query, callback) {
    this.exec(query, callback);
};

MultiTransaction.prototype.execForShards = function(shardIds, query, callback) {
    var queriesByShards = shardIds.reduce(function(memo, shardId) {
        memo[shardId] = query;
        return memo;
    }, {});

    this.execByShards(queriesByShards, callback);
};

MultiTransaction.prototype.transactionalExecForShards = function(shardIds, query, callback) {
    this.execForShards(shardIds, query, callback);
};

MultiTransaction.prototype.execByShards = function(queriesByShards, callback) {
    var self = this;
    async.map(Object.keys(queriesByShards), function(shardId, cb) {
        self.execForShard(shardId, queriesByShards[shardId], cb);
    }, function(err, results) {
        callback(err, results && [].concat.apply([], results));
    });
};

MultiTransaction.prototype.transactionalExecByShards = function(queriesByShards, callback) {
    this.execByShards(queriesByShards, callback);
};

MultiTransaction.prototype.execForKeys = function(keys, query, callback) {
    this.execForShards(this.multiKnex.shardsForKeys(keys), query, callback);
};

MultiTransaction.prototype.transactionalExecForKeys = function(keys, query, callback) {
    this.execForKeys(keys, query, callback);
};

MultiTransaction.prototype.execForKey = function(key, query, callback) {
    this.execForShard(this.multiKnex.shardFor(key), query, callback);
};

MultiTransaction.prototype.transactionalExecForKey = function(key, query, callback) {
    this.execForKey(key, query, callback);
};

MultiTransaction.prototype.execForShard = function(shardId, query, callback) {
    var self = this;
    self.ensureTransaction(shardId, function(trx) {
        query(trx, function(err, result) {
            callback(err, result);
        });
    });
};
