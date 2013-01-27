var util = require('util');
var url = require('url');
var path = require('path');
var async = require('async');
var fs = require('fs');
var mapnik = require('mapnik');
var Pool = require('generic-pool').Pool;
var vector = require('node-vector-server');
var mercator = new(require('sphericalmercator'))();


if (process.platform !== 'win32') {
    // Increase number of threads to 1.5x the number of logical CPUs.
    var threads = Math.ceil(Math.max(4, require('os').cpus().length * 1.5));
    require('eio').setMinParallel(threads);
}

var cache = {};

exports = module.exports = OSciMSource;
util.inherits(OSciMSource, process.EventEmitter);
function OSciMSource(uri, callback) {
    this._uri = uri = normalizeURI(uri);

    if (uri.protocol && uri.protocol !== 'oscim:') {
        throw new Error('Only the oscim protocol is supported');
    }

    // Try to retrieve the cached object instead of constructing a new one.
    var key = url.format(uri);
    if (!cache[key]) {
        cache[key] = this;
        this._cacheKey = key;
        this._open();
    }
    var source = cache[key];

    // Defer the callback until this object is opened.
    if (!source.open) {
        source.once('open', callback);
    } else {
        callback(null, source);
    }
    return undefined;
}

OSciMSource.registerProtocols = function(tilelive) {
    tilelive.protocols['oscim:'] = OSciMSource;
};

OSciMSource.prototype._open = function() {
    var source = this;
    source.setMaxListeners(0);
    async.waterfall([
        source._loadXML.bind(source),
        source._createPool.bind(source),
        source._populateInfo.bind(source)
    ], function(err) {
        if (err) {
            delete cache[source._cacheKey];
            process.nextTick(function() {
                source.emit('open', err);
            });
        } else {
            source.open = true;
            source.emit('open', null, source);
        }
    });
};

OSciMSource.prototype._loadXML = function(callback) {
    var source = this;
    source._base = path.resolve(path.dirname(source._uri.pathname));

    if (source._uri.xml) {
        // This is a string-based map file. Pass it on literally.
        source._xml = source._uri.xml;
        callback(null);
    } else {
        // Load XML from file.
        var filename = path.resolve(source._uri.pathname);
        fs.readFile(filename, 'utf8', function(err, xml) {
            if (!err) {
                source._xml = xml;
            }
            callback(err);
        });
    }
};

OSciMSource.prototype._createPool = function(callback) {
    var source = this;
    this._pool = Pool({
        create: function(callback) {
            var map = new mapnik.Map(256, 256);
            var opts = { strict: false, base: source._base + '/' };
            map.fromString(source._xml, opts, function(err, map) {
                if (err) return callback(err);
                callback(null, map);
            });
        },
        destroy: function() {
            // noop
        },
        max: require('os').cpus().length
    });

    callback(null);
};

OSciMSource.prototype._populateInfo = function(callback) {
    var source = this;
    var id = path.basename(this._uri.pathname, path.extname(this._uri.pathname));

    this._pool.acquire(function(err, map) {
        if (err) return callback(err);

        var info = { id: id, name: id, minzoom: 0, maxzoom: 22 };

        var p = map.parameters;
        for (var key in p) info[key] = p[key];
        if (p.bounds) info.bounds = p.bounds.split(',').map(parseFloat);
        if (p.center) info.center = p.center.split(',').map(parseFloat);
        if (p.minzoom) info.minzoom = parseInt(p.minzoom, 10);
        if (p.maxzoom) info.maxzoom = parseInt(p.maxzoom, 10);
        if (p.interactivity_fields) info.interactivity_fields = p.interactivity_fields.split(',');

        if (!info.bounds || info.bounds.length !== 4)
            info.bounds = [ -180, -85.05112877980659, 180, 85.05112877980659 ];

        if (!info.center || info.center.length !== 3) info.center = [
            (info.bounds[2] - info.bounds[0]) / 2 + info.bounds[0],
            (info.bounds[3] - info.bounds[1]) / 2 + info.bounds[1],
            2 ];

        source._info = info;
        source._pool.release(map);
        callback(null);
    });
};

OSciMSource.prototype.getInfo = function(callback) {
    if (this._info) callback(null, this._info);
    else callback(new Error('Info is unavailable'));
};

// Render handler for a given tile request.
OSciMSource.prototype.getTile = function(z, x, y, callback) {
    z = +z; x = +x; y = +y;
    if (isNaN(z) || isNaN(x) || isNaN(y)) {
        return callback(new Error('Invalid coordinates: ' + z + '/' + x + '/' + y));
    }

    var max = (1 << z);
    if (x >= max || x < 0 || y >= max || y < 0) {
        return callback(new Error('Coordinates out of range: ' + z + '/' + x + '/' + y));
    }

    var source = this;
    this._pool.acquire(function(err, map) {
        if (err) {
            console.warn(err.stack);
            return callback(err);
        }

        var bbox = mercator.bbox(x, y, z, false, '900913');
        map.extent = bbox;
        vector.render(map, function(err, output) {
            process.nextTick(function() {
                source._pool.release(map);
            });

            if (err) {
                console.warn(err.stack);
                return callback(err);
            }

            // Prefix this result with the length.
            var result = new Buffer(output.length + 4);
            result.writeUInt32BE(output.length, 0);
            output.copy(result, 4);
            callback(null, result);
        });
    });
};

OSciMSource.prototype.close = function(callback) {
    // https://github.com/coopernurse/node-pool/issues/17#issuecomment-6565795
    if (this._pool) {
        var pool = this._pool;
        pool.drain(function () {
            pool.destroyAllNow(function() {
                callback();
            });
        });
    } else {
        callback(null);
    }
};

// Serialization for tilelive.js state restoration.
OSciMSource.prototype.toJSON = function() {
    return url.format(this._uri);
};



function normalizeURI(uri) {
    if (typeof uri === 'string') uri = url.parse(uri, true);
    if (uri.hostname === '.' || uri.hostname == '..') {
        uri.pathname = uri.hostname + uri.pathname;
        delete uri.hostname;
        delete uri.host;
    }
    uri.pathname = path.resolve(uri.pathname);
    return uri;
}
