// ----------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// ----------------------------------------------------------------------------
var mssql = require('mssql'),
    helpers = require('./helpers'),
    promises = require('../../utilities/promises'),
    errors = require('../../utilities/errors'),
    errorCodes = require('./errorCodes'),
    log = require('../../logger');

var connections = {};

module.exports = function (config, statement) {
    if(statement.noop)
        return promises.resolved();

    return getNewConnection()
        .then(executeRequest);
        
    function getNewConnection() {
        // if(connectionPromise){
        //     connectionPromise = undefined;
        //     log.verbose('Closing connection to '+ connection.config.database);
        //     return connection.close().then(getNewConnection);
        // }
        var connection = null;
        var connectionPromise = null;

        if (typeof connections[config.database] !== 'undefined' && config.database !== null) {
            connection = connections[config.database].connection;
            connectionPromise = connections[config.database].connectionPromise;
        }
        
        if(connection !== null){
            //check closed connection
            if(!connection.connected || connection.connecting){
                connection = null;
            }
        }

        if (connection === null) {
            log.verbose('Opening connection to ' + config.database);
            var configClone = JSON.parse(JSON.stringify(config));
            connection = new mssql.Connection(configClone);
            connectionPromise = connection.connect()
                .catch(function (err) {
                    connectionPromise = undefined;
                    throw err;
                });
            connections[config.database] = {};
            connections[config.database].connection = connection;
            connections[config.database].connectionPromise = connectionPromise;
        }

        return connectionPromise;
    }

    function executeRequest(connection) {
        var request = new mssql.Request(connection);

        request.multiple = statement.multiple;

        if(statement.parameters) statement.parameters.forEach(function (parameter) {
            var type = parameter.type || helpers.getMssqlType(parameter.value);
            if(type)
                request.input(parameter.name, type, parameter.value);
            else
                request.input(parameter.name, parameter.value);
        });

        log.silly('Executing SQL statement ' + statement.sql + ' with parameters ' + JSON.stringify(statement.parameters));

        return request.query(statement.sql)
            .then(function (results) {
                return statement.transform ? statement.transform(results) : results;
            })
            .catch(function (err) {
                if(err.number === errorCodes.UniqueConstraintViolation)
                    throw errors.duplicate('An item with the same ID already exists');

                if(err.number === errorCodes.InvalidDataType)
                    throw errors.badRequest('Invalid data type provided');

                return promises.rejected(err);
            });
    }
};
