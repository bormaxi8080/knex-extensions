'use strict';

const _ = require('lodash');
const async = require('async');

const EventEmitter = require('events').EventEmitter;

class KnexTransaction {
    constructor(knex, completeHandler) {
        this.knex = knex;
        this.transaction = null;
        this.openingTransaction = false;
        this.completeCount = 0;
        this.openCount = 0;
        this.trxError = null;
        this.completeHandler = completeHandler;
        this.eventEmitter = new EventEmitter();
    };

    completedTransactions(error) {
        this.completeCount++;
        if (error) {
            this.trxError = this.trxError || {error: error};
        }
        if (this.completeCount == this.openCount) {
            this.complete();
        }
    };

    openTransaction() {
        this.openingTransaction = true;
        this.knex.transaction((trx) => {
            this.transaction = trx;
            this.openCount++;
            this.eventEmitter.emit('open', trx);
        }).asCallback((error) => {
            this.completedTransactions(error);
        });
    };

    ensureTransaction(callback) {
        if (this.transaction) {
            return callback(this.transaction);
        }
        if (!this.openingTransaction) {
            this.openTransaction();
        }
        this.eventEmitter.on('open', (trx) => {
            callback(trx);
        });
    };

    commit() {
        this.transaction.commit();
    };

    complete() {
        this.eventEmitter.removeAllListeners();
        this.completeHandler(this.trxError);
    };

    rollback(error) {
        this.trxError = this.trxError || error || "Explicit rollback";
        this.transaction.rollback();
    };

    asCallback(query, callback) {
        this.ensureTransaction((trx) => {
            query(trx, (error, result) => {
                callback(error, result);
            });
        });
    }
}

module.exports = KnexTransaction;