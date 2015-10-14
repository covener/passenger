/*
 *  Phusion Passenger - https://www.phusionpassenger.com/
 *  Copyright (c) 2010-2015 Phusion
 *
 *  "Phusion Passenger" is a trademark of Hongli Lai & Ninh Bui.
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in
 *  all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 *  THE SOFTWARE.
 */

var log;
var express;
var routerThis;
var ustLog;

var createNamespace = require('continuation-local-storage').createNamespace;
var clStore = createNamespace('passenger-request-ctx');

exports.initPreLoad = function(logger, appRoot, ustLogger) {
	log = logger;
	ustLog = ustLogger;

	try {
		express = require(appRoot + "/node_modules/express");
	} catch (e) {
		// express not present, no need to instrument.
		log.debug("Not instrumenting Express (probably not used): " + e);
		return;
	}
	
	try {
		log.info("==== Instrumentation [Express] ==== initialize");
		log.debug("hook application.init, to be the first in the use() line..");

		express.application.initOrig = express.application.init;
		express.application.init = function() {
				log.debug("Express application.init() called, chain and then be the first to use()..");
				var rval = express.application.initOrig.apply(this, arguments);

				this.use(logRequest);
				return rval;
			};

		log.debug("Express tap: router.use, to be as late as possible in the use() line, but before any other error handlers..");
		express.Router.useOrig = express.Router.use;
		express.Router.use = function() {
			// Express recognizes error handlers by #params = 4
			if (arguments.length == 2 && arguments[1].length == 4) {
				express.Router.useOrig.apply(this, [logException]);
			}

			express.Router.useOrig.apply(this, arguments);

			routerThis = this;
		};
	} catch (e) {
		log.error("Unable to instrument Express due to error: " + e);
	}
}

exports.initPostLoad = function() {
	if (!express) {
		return;
	}

	log.debug("add final error handler..");
	try {
		express.Router.useOrig.apply(routerThis, [logException]);
	} catch (e) {
		log.error("Unable to fully instrument Express due to error: " + e);
	}
}

function logRequest(req, res, next) {
	log.verbose("==== Instrumentation [Express] ==== REQUEST [" + req.method + " " + req.url + "]");

	var logBuf = [];
	logBuf.push("Got request for: " + req.url);

	clStore.bindEmitter(req);
	clStore.bindEmitter(res);

	var attachToTxnId = ustLog.getTxnIdFromRequest(req);

	ustLog.logToUstTransaction("requests", logBuf, attachToTxnId);

	clStore.run(function() {
		clStore.set("attachToTxnId", attachToTxnId);
		next();
	});
}

function logException(err, req, res, next) {
	// We may have multiple exception handlers in the routing chain, ensure only the first one actually logs.
	if (!res.hasLoggedException) {
		log.verbose("==== Instrumentation [Express] ==== EXCEPTION + TRACE FOR [" + req.url + "]");

		var logBuf = [];
		logBuf.push("Request transaction ID: " + ustLog.getTxnIdFromRequest(req));
		logBuf.push("Message: " + new Buffer(err.message).toString('base64'));
		logBuf.push("Class: " + err.name);
		logBuf.push("Backtrace: " + new Buffer(err.stack).toString('base64'));
		//logBuf.push("Controller action: ?");

		ustLog.logToUstTransaction("exceptions", logBuf);

		res.hasLoggedException = true;
	}
	next(err);
}

// DEBUG
//var Module = require('module')

//Module.loadOrig = Module._load;
//Module._load = function(request, parent, isMain) { log.info("load: [" + request + ", " +parent+","+isMain+ "]"); return Module.loadOrig(request, parent, isMain); };

//http.createServerOrig = http.createServer;
//http.createServer = createServerTap;

//var appTap;
//function createServerTap(listener) {
	//log.info("CREATE SERVER TAP got "+ listener);
	//appTap = listener;
	//appTap.use(logException);

	//http.createServer = http.createServerOrig;

	//return http.createServer(listener);
//}