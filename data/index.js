/**
 * Created by iweb on 6/21/2017.
 */
const mysql = require('mysql');
const request = require('request');



var connection = function () {
    return mysql.createConnection({
        host : 'localhost',
        user : 'root',
        password : '',
        database : 'test'
    })
}

/**
 * insert address table
 * @param conn
 * @param json
 */
var insertAddr = function (conn, json) {
    var addr = json.address === undefined ? null : json.address;
    var state =
}



var getStateId = function(conn, json) {
    var state = json.state === undefined ? null : json.state;

}

/**
 * insert locations
 * @param conn
 * @param json
 */
var insertState = function (conn, json) {
    var name = json.state === undefined ? null : json.state;
    conn.query('insert into locations set', {name : name, level : 1}, function (error, results, fields) {
        if (error) {
            console.log('INSERT ERROR : ' + name);
            return null;
        }
        return results[0].id;
    });
}

/**
 *
 */
var insertCity = function (conn, json) {
    var name = json.city === undefined ? null : json.city;
    

}

