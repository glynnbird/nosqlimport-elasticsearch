var async = require('async'),
  URL = require('url'),
  elasticsearch = require('elasticsearch'),
  debug = require('debug')('nosqlimport');

debug('Using mongodb nosqlimport writer');

var writer = function(opts) {

  var stream = require('stream'),
    buffer = [ ],
    client = null,
    index = null,
    written = totalfailed = 0,
    buffer_size = 500;
    parallelism = 5;
    
  var parsed = URL.parse(opts.url);
  var index = parsed.pathname.replace(/^\//,'');
  if (index.length === 0) {
    throw('URL must contain path to index e.g. http://localhost:9200/myindex')
  }
  parsed.path = parsed.pathname = '/';
  opts.url = URL.format(parsed);

  var creds = { url: opts.url};

  client = new elasticsearch.Client(creds);

  debug('Elasticsearch URL: ' + opts.url.replace(/\/\/.+@/g, "//****:****@"));
  debug('Elasticsearch Index: ' + index);


  // process the writes in bulk as a queue
  var q = async.queue(function(payload, cb) {
    var docs = payload.docs;
    var commands = []
    for (var i in docs) {
      var command = { index: { _index: index, _type: opts.database}};
      commands.push(command);
      commands.push(docs[i]);
    }
    client.bulk({body: commands}, function(err, data) {
      if (err) {
        writer.emit('writeerror', err);
      } else {
        written += docs.length;
        writer.emit("written", { documents: docs.length, failed: 0, total: written, totalfailed: totalfailed});
        debug({ documents: docs.length, failed: 0, total: written, totalfailed: totalfailed});
      }
      cb(null);
    });
  }, parallelism);
  
  
  // write the contents of the buffer to CouchDB in blocks of 500
  var processBuffer = function(flush, callback) {
  
    if (flush || buffer.length>= buffer_size) {
      var toSend = buffer.splice(0, buffer.length);
      buffer = [];
      q.push({docs: toSend});
      
      // wait until the buffer size falls to a reasonable level
      async.until(
        
        // wait until the queue length drops to twice the paralellism 
        // or until empty
        function() {
          if(flush) {
            return q.idle() && q.length() ==0
          } else {
            return q.length() <= parallelism * 2
          }
        },
        
        function(cb) {
          setTimeout(cb,100);
        },
        
        function() {
          if (flush) {
            writer.emit("writecomplete", { total: written , totalfailed: totalfailed});
          }
          callback();
        });


    } else {
      callback();
    }
  }

  var writer = new stream.Transform( { objectMode: true } );

  // take an object
  writer._transform = function (obj, encoding, done) {

    // add to the buffer, if it's not an empty object
    if (obj && typeof obj === 'object' && Object.keys(obj).length>0) {
      buffer.push(obj);
    }

    // optionally write to the buffer
    this.pause();
    processBuffer(false,  function() {
      done();
    });

  };

  // called when we need to flush everything
  writer._flush = function(done) {
    processBuffer(true, function() {
      done();
    });
  };
  
  return writer;
};

module.exports = {
  writer: writer
}