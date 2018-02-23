require('date-utils');

var aws = require('aws-sdk');
var mysql = require('mysql');

var mysql_host = process.env.DB_ENDPOINT;
var mysql_dbname = process.env.DB_NAME;
var mysql_tablename = process.env.TABLE_NAME;
var mysql_user = process.env.DB_USER;
var mysql_password = process.env.DB_PASSWD;
var s3_backetname =process.env.BACKET_NAME;
var s3_prefix =process.env.S3_PREFIX;

var connection = null;

var dt = new Date();
var now_data = dt.toFormat("YYYYMMDDHH24MISS");

function createSingleConnection() {
    connection = mysql.createConnection({
        host     : mysql_host,
        user     : mysql_user,
        password : mysql_password,
        database : mysql_dbname
    });

    connection.on('error', (err) => {
        if (err.code === 'PROTOCOL_CONNECTION_LOST') {
            createSingleConnection();
            console.log(`Reconnected`);
        } else {
            throw err;
        }
    });
}

function json2csv(json) {
    var header = Object.keys(json[0]).join('\t') + "\n";

    var body = json.map(function(d){
        return Object.keys(d).map(function(key) {
            return d[key];
        }).join('\t');
    }).join("\n");

    return header + body;
}

function tsvUpload(tsvdata) {
  var s3 = new aws.S3();

  var params = {
    Bucket: s3_backetname,
    Key: s3_prefix + now_data + '.tsv',
    Body: tsvdata
  };

  s3.upload(params, function(err, data) {
    if (err) {
      console.log("Error");
      context.fail(err);
      throw err;
    } else {
      console.log(data);
    }
  });
}

createSingleConnection();

exports.handler = function(event, context){
    var sql ="SELECT * FROM " + mysql_dbname + "." + mysql_tablename;

    connection.query(sql, function(err, rows, fields) {
        if (err) {
            console.log("Error");
            context.fail(err);
            throw err;
        } else {
            console.log("Success");
            tsvdata = json2csv(rows);
            tsvUpload(tsvdata);
        }
    });

};
