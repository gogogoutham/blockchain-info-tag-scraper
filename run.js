// #!/usr/bin/node

// Requirements
var pgloader = require(__dirname + "/lib/pgloader.js"),
    tag = require(__dirname + "/lib/blockchain-info-tag.js"),
    util = require(__dirname + "/lib/util.js"),
    fs = require("fs"),
    async = require("async"),
    debug = require('debug')('blockchain-info-tag-scraper');

// Configuration
var dbConfFile = __dirname + "/.pgpass",
    dataDir = __dirname + "/data",
    outTable = 'blockchain_info_tag',
    outTableAll = 'blockchain_info_tag_all';

// Establish time
var runTime = new Date(),
    runTimeSql = runTime.toISOString().slice(0,-5).replace("T",' ') + "+00",
    runTimeUnix = runTime.getTime();

// Parse Postgres configuration
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

// Task generator for DB load
var dbLoadTaskGenerator = function(data, loadAll) {
    return function(cb) {

        if( loadAll ) {
            keyFields = ['address','tag','link','verified'];
            table = outTableAll;
        } else {
            keyFields = ['address'];
            table = outTable;
        }

        var retval = pgloader.run(dbUrl, 
            table, 
            ['address','tag','link','verified','time'], 
            keyFields, 
            data, 
            pgloader.SQL_REPLACE);

        if( retval instanceof Error ) {
            cb(retval);
        } else {
            cb(null, retval);
        } 
    };
};

// Core scraping logic
var scrape = function(numSeen, lastCount, callback) {
    // Request tag page
    tag.request({ offset : numSeen }, function(reqErr, html) {
        if( reqErr ) {
            callback(reqErr);
        }
        debug("Pulled HTML request for offset " + numSeen);


        // Save file asynchronously
        util.saveFile(
            dataDir + "/" + "offset_" + String("0000000" + numSeen).slice(-7) + "_on_" + runTimeUnix + ".html",
            html,
            function(err, result) {
                if(err) {
                    callback(new Error("Could not save file for offset " + numSeen));
                }
                debug("Saved HTML output for offset " + numSeen);
            }
        );

        // Parse data
        tag.parse(html, function(parseErr, data) {
        
            if( parseErr ) {
                callback(new Error("Could not parse file for offset " + numSeen));
            }
            debug("Parsed HTML output for offset " + numSeen);

            // Scrape again if it looks like the number of tags listed isn't decreasing
            var curCount = data.length;
            if( isNaN(parseInt(lastCount)) || curCount >= lastCount ) {
                debug("Launching scraper for offset " + (numSeen + curCount));
                scrape(numSeen + curCount, curCount, callback);
            }

            // Load into two different tables in parallel
            async.parallel([
                dbLoadTaskGenerator(data, false),
                dbLoadTaskGenerator(data, true)
            ], function(err, result) {
                if( err ) {
                    callback(new Error("Could not complete database load for offset " + numSeen));
                }
                debug("Finished database load for offset " + numSeen);
                callback(null, true);
            });
        }, true, { time : runTimeSql });
    });
};

// Core scraper call
scrape(0, null, function(err, result) {
    if( err instanceof Error ) {
        throw err;
    }
});