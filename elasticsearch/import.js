var elasticsearch = require('elasticsearch');
var csv = require('csv-parser');
var fs = require('fs');

var esClient = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'error'
});

var calls = [];
fs.createReadStream('../911.csv')
    .pipe(csv())
    .on('data', data => {
      calls.push({
        'title': data.title,
        'timeStamp': data.timeStamp,
        'neighborhood': data.twp,
        'address': data.address,
        'zip': data.zip,
        'description': data.description,
        'category': data.title.substring(0, data.title.indexOf(':')),
        'location': {
          'lat': data.lat,
          'lon': data.lng
        }
      })
  
    })
    .on('end', () => {

      esClient.bulk({
        index: '911',
        type: 'calls',
        body: calls.reduce((list, call) => {
          list.push({'index': {}});
          list.push(call);
          return list;
        }, [])
      }, (err, resp) => {
        if(err) {
          console.log(err)
        }
      })
    });
