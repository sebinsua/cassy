var PooledConnection = require('cassandra-client').PooledConnection,
    uuid = require('cassandra-client').UUID,
    _ = require('underscore'),
    async = require('async');

var _makeArray = function (o) {
    var arr = [];
    for (var name in o) {
        if (o[name]) {
            var value = o[name];
        } else {
            var value = [null];
        }
        arr.push(value);
    }
    return arr;
};

var _fillArray = function (value, len) {
    var arr = [];
    for (var i = 0; i < len; i++) {
        arr.push(value);
    }
    return arr;
};

var DatabaseAccessError = function (message) {
    this.code = 'DatabaseAccessError';
    this.message = message;
};
DatabaseAccessError.prototype = new Error();

var Cassy = function (config) {
    var cassyLogger = console;

    var con = new PooledConnection({
        hosts: _fillArray(config.cassandra.host + ':' + config.cassandra.port, 10),
        keyspace: config.cassandra.default_keyspace ? config.cassandra.default_keyspace : 'godofsports',
        timeout: 10000,
        query_timeout: 30000 // @todo: This is insane...! Think about a better way - or at least caching!
    });
    var connecting = false;

    // @todo: If config.caching === true, then setup redis, etc.
    //        Allow metadata to be bound to a a select statement, etc.
    //        Use the metadata to apply caches to execution of select statements.

    con.on('log', function (level, message, obj) {
        level = (level === 'info' || level === 'warn' || level === 'timing' || level === 'trace') ? 'debug' : level;
        cassyLogger.log(level, message, obj);
    });

    var generateUuid = function () {
        return uuid.fromTime(new Date().getTime());
    };

    var generateInsert = function (columnFamily, data, makeKey) {
        if (!makeKey) {
            makeKey = function (data) {
                if (data.id) {
                    var key = data.id;
                } else {
                    var key = generateUuid();
                }
                return key;
            };
        }

        var _columns = Object.keys(data);
        var _columnsString = _columns.map(function (e) {
            return "'" + e + "'";
        }).join(', ');
        var _questionMarksString = _columns.map(function (e) {
            return '?';
        }).join(', ');
        var statement = "INSERT INTO '" + columnFamily + "' ('KEY', " + _columnsString + ") VALUES (?, " + _questionMarksString + ")";

        var values = _makeArray(data);
        var key = makeKey(data);
        values.unshift(key);

        return {
            'statement': statement,
            'values': values,
            'key': key.toString()
        };
    };

    var generateUpdate = function (columnFamily, data, keys) {
        var _values = function (object) {
            var results = [];
            for (var property in object) {
                results.push(object[property]);
            }
            return results;
        };

        if (keys != undefined && Object.prototype.toString.call(keys) !== '[object Object]') {
            keys = {
                'KEY': keys
            };
        }

        var _columns = Object.keys(data);
        var _columnsData = _values(data);
        var _columnsString = _columns.map(function (e) {
            return "'" + e + "' = ?";
        }).join(', ');
        var statement = "UPDATE '" + columnFamily + "' SET " + _columnsString;

        if (keys != undefined) {
            var whereKeys = [];
            for (var name in keys) {
                var equality = ' = ?';
                if (Object.prototype.toString.call(keys[name]) == '[object Array]' && keys[name].length > 1) {
                    equality = ' IN (' + keys[name].map(function (e) {
                        return "'" + e + "'";
                    }).join(", ") + ')';
                    delete keys[name];
                }
                whereKeys.push(name + equality);
            }
            var wheres = whereKeys.join(' AND ');

            var keyValues = [];
            for (var name in keys) {
                keyValues.push(keys[name]);
            }

            statement += " WHERE " + wheres;
        }

        if (keyValues) {
            var values = _columnsData.concat(keyValues);
        } else {
            var values = _columnsData;
        }

        return {
            'statement': statement,
            'values': values,
            'key': _.map(keys, function (k) {
                return k.toString();
            })
        };
    };

    /**
     * Data should be in either this format:
     * {
     *     key1: increment_value,
     *     key2: increment_value,
     *     key3: increment_value
     * }
     * or this format:
     * {
     *     key1: { operator: '+', value: '25' }
     *     key2: { operator: '-', value: '15' }
     *     key3: { operator: '+', value: '625' }
     * }
     *
     * @param columnFamily
     * @param data
     * @param keys
     * @return {Object}
     */
    var generateCounterUpdate = function (columnFamily, data, keys) {
        if (keys != undefined && Object.prototype.toString.call(keys) !== '[object Object]') {
            keys = {
                'KEY': keys
            };
        }

        var _temp = _.map(data, function (value, key) {
            var item = {};
            if (Object.prototype.toString.call(value) !== '[object Object]') {
                item['key'] = key;
                item['operator'] = '+';
                item['value'] = value;
                return item;
            }
            else {
                value['key'] = key;
                if (value['operator'] == undefined) {
                    value['operator'] = '+';
                }
                return value;
            }
        });
        var _columnsData = _temp.map(function (e) {
            return e.value;
        });
        var _columnsString = _temp.map(function (e) {
            return "'" + e.key + "' = '" + e.key + "' " + e.operator + " ?";
        }).join(', ');
        var statement = "UPDATE '" + columnFamily + "' SET " + _columnsString;

        if (keys != undefined) {
            var whereKeys = [];
            for (var name in keys) {
                var equality = ' = ?';
                if (Object.prototype.toString.call(keys[name]) == '[object Array]' && keys[name].length > 1) {
                    equality = ' IN (' + keys[name].map(function (e) {
                        return "'" + e + "'";
                    }).join(", ") + ')';
                    delete keys[name];
                }
                whereKeys.push(name + equality);
            }
            var wheres = whereKeys.join(' AND ');

            var keyValues = [];
            for (var name in keys) {
                keyValues.push(keys[name]);
            }

            statement += " WHERE " + wheres;
        }

        if (keyValues) {
            var values = _columnsData.concat(keyValues);
        } else {
            var values = _columnsData;
        }

        return {
            'statement': statement,
            'values': values,
            'key': _.map(keys, function (k) {
                return k.toString();
            })
        };
    };

    var generateBatch = function (statements, batchSize) {
        if (Object.prototype.toString.call(statements) !== '[object Array]') {
            statements = [statements];
        }
        if (batchSize == null) {
            batchSize = 50;
        }

        /**
         Looks sort of like:

         BEGIN BATCH USING CONSISTENCY QUORUM
         INSERT INTO users (KEY, password, name) VALUES ('user2', 'ch@ngem3b', 'second user')
         UPDATE users SET password = 'ps22dhds' WHERE KEY = 'user2'
         INSERT INTO users (KEY, password) VALUES ('user3', 'ch@ngem3c')
         DELETE name FROM users WHERE key = 'user2'
         INSERT INTO users (KEY, password, name) VALUES ('user4', 'ch@ngem3c', 'Andrew')
         APPLY BATCH;
         */

        var batchCount = Math.ceil(statements.length / batchSize);
        var batchStatements = [];
        for (var batchIndex = 1; batchIndex <= batchCount; batchIndex++) {
            var firstStatementOfBatch = (batchIndex - 1) * batchSize;
            var lastStatementOfBatch = batchIndex * batchSize;

            var _tmpStatements = statements.slice(firstStatementOfBatch, lastStatementOfBatch);

            // Append all of the key lists together. Which may or may not be an array.
            var keys = [];
            var values = [];
            for (var s = 0; s < _tmpStatements.length; s++) {
                var _tmpKeys = [];
                if (Object.prototype.toString.call(_tmpStatements[s].key) !== '[object Array]') {
                    _tmpKeys = [_tmpStatements[s].key];
                } else {
                    _tmpKeys = _tmpStatements[s].key;
                }
                var _tmpValues = _tmpStatements[s].values;

                keys = keys.concat(_tmpKeys);
                values = values.concat(_tmpValues);
            }

            var _concatenatedStatements = _.reduce(_tmpStatements, function (prev, s) {
                return prev + s.statement + ";\n";
            }, '');
            var batchStatement = ("BEGIN BATCH USING CONSISTENCY QUORUM\n" +
                _concatenatedStatements +
                "APPLY BATCH;");

            batchStatements.push({
                'statement': batchStatement,
                'values': values,
                'key': keys
            });
        }

        return batchStatements;
    };

    var generateLookup = function (columnFamily, key, ids) {
        if (ids.length < 1) {
            return false;
        }
        var _columns = ids;
        var _columnsString = _columns.map(function (e) {
            if (_.isString(e)) {
                return "'" + e + "'";
            } else { // If a list of objects was passed in then, we're going to use the id as the column name.
                return "'" + e.id + "'";
            }
        }).join(', ');
        var _questionMarksString = _columns.map(function (e) {
            return '?';
        }).join(', ');
        var statement = "INSERT INTO '" + columnFamily + "' ('KEY', " + _columnsString + ") VALUES (?, " + _questionMarksString + ")";

        var values = _columns.map(function (c) {
            if (_.isString(c)) {
                return [null];
            } else { // If a list of objects was passed in then, we're going to use the value as the column value.
                return c.value ? c.value : '';
            }
        });
        values.unshift(key);

        return {
            'statement': statement,
            'values': values
        };
    };

    var generateDelete = function (columnFamily, keys, columns) {
        if (keys != undefined && Object.prototype.toString.call(keys) !== '[object Object]') {
            keys = {
                'KEY': keys
            };
        }

        var _columnsString = columns.map(function (e) {
            return "?";
        }).join(', ');
        var _columnsData = columns;
        var statement = "DELETE " + _columnsString + " FROM " + columnFamily;

        if (keys != undefined) {
            var whereKeys = [];
            for (var name in keys) {
                var equality = ' = ?';
                if (Object.prototype.toString.call(keys[name]) == '[object Array]' && keys[name].length > 1) {
                    equality = ' IN (' + keys[name].map(function (e) {
                        return "'" + e + "'";
                    }).join(", ") + ')';
                    delete keys[name];
                }
                whereKeys.push(name + equality);
            }
            var wheres = whereKeys.join(' AND ');

            var keyValues = [];
            for (var name in keys) {
                keyValues.push(keys[name]);
            }

            statement += " WHERE " + wheres;
        }

        if (keyValues) {
            var values = _columnsData.concat(keyValues);
        } else {
            var values = _columnsData;
        }

        return {
            'statement': statement,
            'values': values,
            'key': _.map(keys, function (k) {
                return k.toString();
            })
        };
    };

    var generateSelect = function (columnFamily, keys, columns) {
        if (columns != undefined && Object.prototype.toString.call(columns) === '[object Array]') {
            var _columnsString = columns.map(function (e) {
                return "?";
            }).join(', ');
        } else { // assume undefined.
            var _columnsString = '*';
            columns = [];
        }

        if (keys != undefined && Object.prototype.toString.call(keys) !== '[object Object]') {
            keys = {
                'KEY': keys
            };
        }

        var statement = "SELECT " + _columnsString + " FROM '" + columnFamily + "'";
        if (keys != undefined) {
            var whereKeys = [];
            for (var name in keys) {
                var equality;
                if (Object.prototype.toString.call(keys[name]) == '[object Array]') {
                    if (keys[name].length > 1) {
                        equality = ' IN (' + keys[name].map(function (e) {
                            return "'" + e + "'";
                        }).join(", ") + ')';
                        delete keys[name];
                    } else {
                        equality = ' = ?';
                    }

                    whereKeys.push(name + equality);
                }
                else if (typeof keys[name] === 'object') {
                    // else if an object then we can use the from (greater than)/until (less than) keys to create filtering.
                    var greaterThan = keys[name].greater_than ? keys[name].greater_than :
                        keys[name].from ? keys[name].from : null;
                    var lessThan = keys[name].less_than ? keys[name].less_than :
                        keys[name].until ? keys[name].until : null;
                    if (greaterThan) {
                        equality = ' > ?';

                        whereKeys.push(name + equality);
                    }
                    if (lessThan) {
                        equality = ' < ?';

                        whereKeys.push(name + equality);
                    }
                } else {
                    equality = ' = ?';

                    whereKeys.push(name + equality);
                }
            }
            var wheres = whereKeys.join(' AND ');

            var keyValues = [];
            for (var name in keys) {
                if (Object.prototype.toString.call(keys[name]) == '[object Array]' && keys[name].length == 1) {
                    keyValues.push(keys[name][0]);
                } else if (typeof keys[name] === 'object') {
                    var greaterThan = keys[name].greater_than ? keys[name].greater_than :
                        keys[name].from ? keys[name].from : null;
                    var lessThan = keys[name].less_than ? keys[name].less_than :
                        keys[name].until ? keys[name].until : null;
                    keyValues.push(greaterThan);
                    keyValues.push(lessThan);
                } else {
                    keyValues.push(keys[name]); // The reason this works is that I have handled the array with gigantic
                    // (potentially slow IN queries)
                }
            }

            if (wheres) {
                statement += " WHERE " + wheres + "";
            }
        }
        if (keyValues) {
            var values = columns.concat(keyValues);
        } else {
            var values = columns;
        }

        return {
            'statement': statement,
            'values': values
        }
    };

    var _generateDebugLine = function (statement, values) {
        var debugLine = statement;
        if (values != undefined && values.length > 0) {
            for (var iv = 0; iv < values.length; iv++) {
                var value = values[iv];
                debugLine = debugLine.replace(/((?!\?")\?)/, '"' + value + '"');
            }
        }

        return debugLine;
    };

    var _generateExecuteStatement = function (statement, values, key, successHandle, errorHandle) {
        return function _executeStatement(onSuccess) {
            var debugLine = _generateDebugLine(statement, values);
            var sqlHashId = Math.random().toString(36).substr(2, 8);

            cassyLogger.debug("Executing query (" + sqlHashId + "): " + debugLine);
            con.execute(statement, values, function (err, rows, metadata) {
                if (err) {
                    var message = "Query (" + sqlHashId + ") gave: " + err + " with the metadata: " + JSON.stringify(metadata);

                    cassyLogger.error(message);
                    if (errorHandle) {
                        errorHandle(err);
                    }
                    throw new DatabaseAccessError(message);
                } else {
                    cassyLogger.debug("Query (" + sqlHashId + ") was successful.");
                    if (successHandle) {
                        if (rows && (rows.colHash || rows.key)) {
                            successHandle(rows);
                        } else if (key) {
                            successHandle(key);
                        } else {
                            successHandle(rows);
                        }
                    }
                    onSuccess();
                }
            });
        };
    };

    var _isConnected = function () {
        if (con && con.connections.length > 0) {
            return _.some(con.connections, function (c) {
                return c.isHealthy() && c.connected;
            });
        }
        return false;
    };

    var _isConnecting = function () {
        return connecting;
    };

    var _executeStatements = function (execDataItems, successHandles, errorHandles) {
        var executionChain = _.zip(execDataItems, successHandles, errorHandles);
        var callbacks = executionChain.map(function (e) {
            return _generateExecuteStatement(e[0].statement, e[0].values, e[0].key, e[1], e[2]);
        });

        async.series(callbacks, function (err, results) {
            if (err) {
                var message = "Error while querying Cassandra: " + err + "\n Results was: " + results;
                cassyLogger.error(message);
                throw new DatabaseAccessError(message);
            }
        });
    };

    var execute = function (execDataItems, successHandles, errorHandles) {
        if (Object.prototype.toString.call(execDataItems) !== '[object Array]') {
            execDataItems = [execDataItems];
        }
        if (Object.prototype.toString.call(successHandles) !== '[object Array]') {
            if (successHandles != undefined) {
                successHandles = _fillArray(function () {
                }, execDataItems.length - 1).concat(successHandles);
            } else {
                successHandles = [];
            }
        }
        if (Object.prototype.toString.call(errorHandles) !== '[object Array]') {
            if (errorHandles != undefined) {
                errorHandles = _fillArray(function () {
                }, execDataItems.length - 1).concat(errorHandles);
            } else {
                errorHandles = [];
            }
        }
        if (execDataItems.length === 0) {
            if (Object.prototype.toString.call(_.last(successHandles)) === '[object Function]') {
                _.last(successHandles)(false);
            }
            return false;
        }

        process.nextTick(function () {
            if (_isConnected()) {
                _executeStatements(execDataItems, successHandles, errorHandles);
            } else if (_isConnecting()) {
                var waitForConnectedId = setTimeout(function () {
                    if (!_isConnecting()) {
                        clearTimeout(waitForConnectedId);
                        _executeStatements(execDataItems, successHandles, errorHandles);
                    }
                }, 500);
            } else {
                connecting = true;
                con.connect(function () {
                    connecting = false;
                    cassyLogger.debug("Connected to Cassandra. Ready to execute statements.");
                    _executeStatements(execDataItems, successHandles, errorHandles);
                });
            }
        });
    };

    var checkIfExists = function (columnFamily, key, booleanCallback) {
        var st = generateSelect(columnFamily, key);
        execute(st, function (rows) {
            var data = rows[0].colHash;
            if (rows.length == 0 || (Object.keys(rows[0].colHash).length === 0 && rows.length >= 1)) {
                // Note: It's fucking weird that it's coming back with a row for nothing... :(
                booleanCallback(false, data);
                return false;
            } else {
                booleanCallback(true, data);
                return true;
            }
        });
    };

    return {
        'generateUuid': generateUuid,
        'generateSelect': generateSelect,
        'generateInsert': generateInsert,
        'generateUpdate': generateUpdate,
        'generateCounterUpdate': generateCounterUpdate,
        'generateDelete': generateDelete,
        'generateBatch': generateBatch,
        'generateLookup': generateLookup,
        'execute': execute,
        'checkIfExists': checkIfExists
    };
};

module.exports = Cassy;
