'use strict';

const _ = require('lodash');

const Knex = require('knex');
const KnexTransaction = require('./KnexTransaction');

const knexDataToSql = (data) => {
    let counter = 0;
    return data.sql.replace(/\?/g, function() {
        const obj = data.bindings[counter++];
        return (obj instanceof Object) ? JSON.stringify(obj) : obj;
    });
};

class KnexWrapper {
    constructor(config, logger) {
        this.config = config;
        this.logger = logger;

        const databaseSettings = config.database;
        const knexConfig = {
            client: 'pg',
            connection: {
                host: databaseSettings.host,
                user: databaseSettings.user,
                password: databaseSettings.password,
                database: databaseSettings.databaseName
            }
        };

        this.knexConfig = knexConfig;
        this.knex = new Knex(knexConfig);

        this.knex.on('query', (data) => {
            this.logger.info("EXECUTING ON " + this.knexConfig.connection.database + " : " + knexDataToSql(data));
        });
    }

    asCallback(query, callback) {
        query(this.knex, callback);
    }

    transaction(trxCallback, resultCallback) {
        let trx = new KnexTransaction(this.knex, resultCallback);
        trxCallback(trx);
    }

    transactionally(trxCallback, resultCallback) {
        let result;
        this.transaction((trx) => {
            trxCallback(trx, (error, res) => {
                if (error) {
                    this.logger.warn("ROLLING BACK TRANSACTION: " + error);
                    trx.rollback(error);
                } else {
                    this.logger.info("COMMITING TRANSACTION");
                    result = res;
                    trx.commit();
                }
            });
        }, (error) => {
            resultCallback(error, result);
        });
    }
}

module.exports = KnexWrapper;
