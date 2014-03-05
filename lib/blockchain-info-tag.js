// Requirements
var request = require("request"),
    cheerio = require("cheerio");

// Configuration variables
var tagUrl = "https://blockchain.info/tags";

// Gets a web page from the blockchain info tag URL and proceeds accordingly
exports.request = function(options, callback) {
    request.get({ // Specification of full request
        url : tagUrl,
        qs : options
    }, function(error, response, html){ //Callback to deal with response
        if (!error && response.statusCode == 200) {
            callback(null, html);    
        } else {
            callback(error);
        }
    }); 
};

// Parses the html content coming from a blockchain.info request
exports.parse = function(html, callback, toLower, rowPadding) {
    var headers = [],
        data = [],
        doc = cheerio.load(html);

    // Pull in the list of headers
    doc(".container th").each( function(index, element) {
        headers.push(doc(this).text().trim());
    });

    // Set the header case to lower if specified
    var setCase = function(string) {
        if( toLower ) {
            return string.toLowerCase();
        }
        else {
            return string;
        }
    };

    // Populate the data and add on padding if specified
    doc(".container tr").each( function(i, tr) {
        var datum = {};
        doc(this).children().each( function(j, td) {
            if( headers[j] != "Verified" ) {
                datum[setCase(headers[j])] = doc(this).text().trim();
            } else {
                if ( doc(this).html().indexOf("src=\"/Resources/green_tick.png\"") > -1 ) {
                    datum[setCase("Verified")] = "1";
                } else {
                    datum[setCase("Verified")] = "0";
                }
            }
            // Add in padding if necessary
            if( rowPadding ) {
                for( padProp in rowPadding ) {
                    datum[padProp] = rowPadding[padProp];
                }
            }
        });
        data.push(datum);
    });

    // Return
    callback(null, data);
};