const mqtt = require('mqtt');
const mysql = require('mysql');

let topic = "busdata"
var opts = {
    username: 'distronix',
    password: 'D@1357902468',
}

var connection = mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: 'toor',
    database: 'bus_log',
    port: 3306
});

connection.connect((err) => {
    if (err) throw err;
    console.log("Connected");
})


const client = new mqtt.connect('mqtt://pis.distronix.in', opts);

client.on('connect', function() {
    console.log("Connected");
});

client.subscribe(topic, function(err){
    if(err){
        console.log("error");
    }
});

String.prototype.replaceAt = function(index, replacement) {
    if (index >= this.length) {
        return this.valueOf();
    }
 
    var chars = this.split('');
    chars[index] = replacement;
    return chars.join('');
}

client.on('message', function(topic, message){
    var txt = message.toString();
    // txt = txt.replaceAt(0, '[');
    // txt = txt.replaceAt(txt.length-1, ']');
    const obj = JSON.parse(txt);
    // print(Object.keys(obj));
    for(let i = 0; i<Object.keys(obj).length; i++){
        // print(obj[Object.keys(obj)[i]]["latitude"]);
        var sql = `INSERT into bus_data(latitude, longitude, speed, vehicleRegNo, routeCode, eventMessages, 
            pointSequence, objectId, timestamp) values(` + obj[Object.keys(obj)[i]]["latitude"] + 
            `, ` + obj[Object.keys(obj)[i]]["longitude"] + `, ` + obj[Object.keys(obj)[i]]["speed"] + `, '`
            + obj[Object.keys(obj)[i]]["vehicleRegNo"] + `', '` + obj[Object.keys(obj)[i]]["routeCode"] + `', '`
             + obj[Object.keys(obj)[i]]["eventMessages"] + `', `
            + obj[Object.keys(obj)[i]]["pointSequence"] + `, '` +  obj[Object.keys(obj)[i]]["objectId"] + 
            `', CURRENT_TIMESTAMP)`;

        // print(sql);
        connection.query(sql, (err, result) => {
            if (err) throw err;
            console.log("Data Ingested");
        });
    };
    // print(txt);
    // var sql = "INSERT INTO bus_data(latitude, longitude, speed, dataProvider, vechicleRegNo, routeCode, tripId, eventMessages, pointSequence, objectId, timestamp, beanType) values"
});

