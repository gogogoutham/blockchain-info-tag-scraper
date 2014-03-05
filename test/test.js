// 1. Parse step only
var fs = require("fs"),
    tag = require(__dirname + "/../lib/blockchain-info-tag.js");
var html = fs.readFileSync(__dirname + "/example.html", { encoding : "utf-8" });
tag.parse(html, function(parseErr, data) {
    console.log("First 5 tags are %o", data.slice(0,5));
    console.log("Last 5 tags are %o", data.slice(-5));
    console.log("Length of tag list is %o", data.length);
}, true, {time : (new Date()).toISOString().slice(0,-5).replace("T",' ') + "+00"});

// 2. Full Request w/ modest parse (test of request step)
var tag = require(__dirname + "/../lib/blockchain-info-tag.js");
tag.request({ offset : 2200 }, function(reqErr, result) {
    if( reqErr ) {
        console.log("Error requesting tag information");
        throw reqErr;
    }
    console.log(result);
    tag.parse(result, function(parseErr, data) {
        console.log("First 5 tags are %o", data.slice(0,5));
        console.log("Length of tag list is %o", data.length);
    });
});

// 3. Full Request, Parse, and DB load
var pgloader = require(__dirname + "/lib/pgloader.js"),
    tag = require(__dirname + "/lib/blockchain-info-tag.js");
var dbConfFile = __dirname + "/../.pgpass",
    outTable = 'blockchain_info_tag',
    outTableAll = 'blockchain_info_tag_all';
var dbUrl;
(function() {
    var config = (fs.readFileSync(dbConfFile, { encoding : "ascii" })).trim().split(":"),
    hostname = config[0],
    port = config[1],
    database = config[2],
    username = config[3],
    password = config[4];
    dbUrl = "postgres://" + username + ":" + password + "@" + hostname + ":" + port + "/" + database;
})();
tag.request({ offset : 2200 }, function(reqErr, result) {
    if( reqErr ) {
        console.log("Error requesting tag information");
        throw reqErr;
    }

    tag.parse(result, function(parseErr, data) {
        console.log("First 5 tags are %o", data.slice(0,5));
        console.log("Last 5 tags are %o", data.slice(-5));
        console.log("Length of tag list is %o", data.length);

        pgloader.run(dbUrl, 
            outTable, 
            ['address','tag','link','verified','time'], 
            ['address'], 
            data, 
            pgloader.SQL_REPLACE);

        pgloader.run(dbUrl, 
            outTableAll, 
            ['address','tag','link','verified','time'], 
            ['address','tag','link','verified'], 
            data, 
            pgloader.SQL_REPLACE);
    }, true, {time : (new Date()).toISOString().slice(0,-5).replace("T",' ') + "+00"});
});