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
var net = require('net');
var os = require("os");
var nbo = require('network-byte-order');
var codify = require('codify');
var microtime = require('microtime');

var ustRouterAddress; // standalone = "localhost:9344";
var ustRouterUser;
var ustRouterPass;
var ustGatewayKey;

var nodeName = os.hostname();
var appGroupName;

var routerConn;
var routerState = 0;
var pendingTxnBuf = [];

var inspection = false;

// some kind of discard timer? failure to connect?

function changeState(newRouterState) {
	log.debug("routerState: " + routerState + " -> " + newRouterState);
	routerState = newRouterState;
}

exports.init = function(logger, routerAddress, routerUser, routerPass, gatewayKey, groupName) {
	log = logger;
	if (routerState != 0) {
		log.error("Trying to init when routerState not 0! (ignoring)");
		return;
	}
	ustRouterAddress = routerAddress;
	ustRouterUser = routerUser;
	ustRouterPass = routerPass;
	ustGatewayKey = gatewayKey;
	appGroupName = groupName;
	
	// createConnection doesn't understand the "unix:" prefix, but it does understand the path that follows.
	if (ustRouterAddress.indexOf("unix:") == 0) {
		ustRouterAddress = ustRouterAddress.substring(5);
	}
	log.debug("initialize ustrouter_connector with [routerAddress:" + ustRouterAddress + "] [user:" + ustRouterUser + "] [pass:" + ustRouterPass + "] [key:" + 
		ustGatewayKey + "] [app:" + appGroupName + "]");

	if (!ustRouterAddress || !ustRouterUser || !ustRouterPass || !ustGatewayKey || !appGroupName) {
		log.verbose("Union Station logging disabled (incomplete configuration).");
		return;
	}
	
	beginConnection();
}

exports.isEnabled = function() {
	return routerState > 0;
}

function beginConnection() {
	changeState(1);

	routerConn = net.createConnection(ustRouterAddress);

	routerConn.on("connect", function() { changeState(2); });
	routerConn.on("error", onError);
	routerConn.on("end", function() { changeState(0); });

	routerConn.on("data", onData);
}

function LogTransaction(cat) {
	this.timestamp = microtime.now();
	this.category = cat;
	this.txnId = "";
	this.logBuf = [];
	this.state = 1;
}

function tryWriteLogs() {
	log.debug("tryWriteLogs");
	if (routerState == 0) {
		// it disconnected or crashed somehow, reconnect
		beginConnection();
		return;
	} else if (routerState != 5) {
		return; // we're not idle
	}

	// we have an authenticated, active connection; see what we can send
	if (pendingTxnBuf.length == 0) {
		return; // no pending
	}
	
	if (pendingTxnBuf[0].state == 1) {
		// still need to open the txn
		changeState(6); // expect ok/txnid in onData()..
		log.debug("open transaction(" + pendingTxnBuf[0].txnId + ")");
		writeLenArray(routerConn, "openTransaction\0" + pendingTxnBuf[0].txnId + "\0" + appGroupName + "\0" + nodeName + "\0" + 
			pendingTxnBuf[0].category +	"\0" + codify.toCode(pendingTxnBuf[0].timestamp) + "\0" + ustGatewayKey + "\0true\0true\0\0");
	} else {
		// txn is open, log the data & close
		log.debug("log & close transaction(" + pendingTxnBuf[0].txnId + ")");
		txn = pendingTxnBuf.shift();
		for (i = 0; i < txn.logBuf.length; i++) {
			writeLenArray(routerConn, "log\0" + txn.txnId + "\0" + codify.toCode(txn.timestamp) + "\0");
			writeLenString(routerConn, txn.logBuf[i]);
		}

		changeState(7); // expect ok in onData()..
		writeLenArray(routerConn, "closeTransaction\0" + txn.txnId + "\0" + codify.toCode(microtime.now()) + "\0true\0");
	}	
}

exports.getTxnIdFromRequest = function(req) {
	return req.headers['passenger-txn-id'];
}

exports.logToUstTransaction = function(category, lineArray, txnIfContinue) {
	logTxn = new LogTransaction(category);
	
	if (txnIfContinue) {
		logTxn.txnId = txnIfContinue;
	}
	logTxn.logBuf = lineArray;
	pendingTxnBuf.push(logTxn);
	
	tryWriteLogs();
}

var readBuf = "";
// N.B. newData may be partial!
function readLenArray(newData) {
	readBuf += newData;
	log.silly("read: total len = " + readBuf.length);
	log.silly(new Buffer(readBuf));
	if (readBuf.length < 2) {
	log.silly("need more header data..");
		return null; // expecting at least length bytes
	}
	lenRcv = nbo.ntohs(new Buffer(readBuf), 0);
	log.silly("read: lenRCv = " + lenRcv);
	if (readBuf.length < 2 + lenRcv) {
	log.silly("need more payload data..");
		return null; // not fully read yet
	}
	resultStr = readBuf.substring(2, lenRcv + 2);
	readBuf = readBuf.substring(lenRcv + 2); // keep any bytes read beyond length for next read
	
	return resultStr.split("\0");
//	setTimeout(function () { log.silly('timeout..'); c.end() }, 100);
}

function writeLenString(c, str) {
	len = new Buffer(4);
	nbo.htonl(len, 0, str.length);
	c.write(len);
	c.write(str);
}

function writeLenArray(c, str) {
	len = new Buffer(2);
	nbo.htons(len, 0, str.length);
	c.write(len);
	c.write(str);
}

function onError(e) {
	if (routerState == 1) {
		log.error("Unable to connect to ustrouter at [" + ustRouterAddress +"], Union Station logging disabled.");
	} else {
		log.error("Unexpected error in ustrouter connection: " + e + ", Union Station logging disabled.");
	}
	changeState(0);
}

function onData(data) {
	log.silly(data);
	rcvString = readLenArray(data);
	if (!rcvString) {
		return;
	}
	log.silly("got: [" + rcvString + "]");

	if (routerState == 2) { // expect version 1
		if ("version" !== rcvString[0] || "1" !== rcvString[1]) {
			log.error("Unsupported ustrouter version: [" + rcvString + "], Union Station logging disabled.");
			changeState(0);
			// TODO: close?
			return;
		}
		changeState(3);

		writeLenString(routerConn, ustRouterUser);
		writeLenString(routerConn, ustRouterPass);
	} else if (routerState == 3) { // expect OK from auth
		if ("status" != rcvString[0] || "ok" != rcvString[1]) {
			log.error("Error authenticating to ustrouter: unexpected [" + rcvString + "], Union Station logging disabled.");
			changeState(0);
			// TODO: close?
			return;
		}
		changeState(4);

		writeLenArray(routerConn, "init\0" + nodeName + "\0");
	} else if (routerState == 4) { // expect OK from init
		if ("status" != rcvString[0] || "ok" != rcvString[1]) {
			log.error("Error initializing ustrouter connection: unexpected [" + rcvString + "], Union Station logging disabled.");
			changeState(0);
			// TODO: close?
			return;
		}
	
		changeState(5);
		tryWriteLogs();
	 } else if (routerState == 5) { // not expecting anything
	 	log.warn("unexpected data receive state (5)");
	 	tryWriteLogs();
	} else if (routerState == 6) { // expect OK transaction open
		if ("status" != rcvString[0] || "ok" != rcvString[1]) {
			log.error("Error opening ustrouter transaction: unexpected [" + rcvString + "], Union Station logging disabled.");
			changeState(0);
			// TODO: close?
			return;
		}
		
		pendingTxnBuf[0].state = 2;
		if (pendingTxnBuf[0].txnId.length == 0) {
			log.debug("use rcvd back txnId: " + rcvString[2]);
			pendingTxnBuf[0].txnId = rcvString[2]; // fill in the txn from the ustrouter reply
		}

		changeState(5);
		tryWriteLogs();
	} else if (routerState == 7) { // expect OK transaction close
		if ("status" != rcvString[0] || "ok" != rcvString[1]) {
			log.error("Error closing ustrouter transaction: unexpected [" + rcvString + "], Union Station logging disabled.");
			changeState(0);
			// TODO: close?
			return;
		}
		
		changeState(5);
		tryWriteLogs();
	}
}
