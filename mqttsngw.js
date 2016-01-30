var SerialPort = require("serialport").SerialPort;
var net = require('net');
var Connection = require('mqtt-connection');
var mqttsn = require('mqttsn-packet');

var MQTTSN_SerialGW = function(serialPath, rate, brokerHost, brokerPort, id){
	if(rate == undefined)
		rate = 115200;
	if(brokerHost == undefined)
		brokerHost = 'localhost';
	if(brokerPort == undefined)
		brokerPort = 1883;
	if(id == undefined)
		id = 137;
	this.serialPath = serialPath;
	this.serialRate = rate;
	this.brokerHost = brokerHost;
	this.brokerPort = brokerPort;
	if(id < 0 || id > 255)
		throw "Invalid gateway id provided! It must be between 0 - 255"; 
	this.gatewayID = id;
	this.topicTable = new Array();
	this.msgIdTable = new Array();
};

MQTTSN_SerialGW.prototype.connect = function(){
	var self = this;
	this.serialPort = new SerialPort(this.serialPath, {
		baudrate: this.serialRate
	});
	this.stream = net.createConnection(this.brokerPort, this.brokerHost, function(){
		console.log("Connected to MQTT Broker!");
	});
	
	var onSerialData = function(data){
		self.parser.parse(data);
	};
	var onConnack = function(packet){
		if(packet.returnCode == 0)
			self.clientConnected = true;
		var connack = mqttsn.generate({cmd: 'connack', returnCode: (packet.returnCode == 0? 'Accepted' : packet.returnCode == 3? 'Rejected: congestion' : 'Rejected: not supported')});
		self.serialPort.write(connack,function(){
			console.log("Forwarding CONNACK to client: "+connack.toString('hex'));
		});
	};
	var onPublish = function(packet){
		var topicId = self.topicTable.indexOf(packet.topic)+1;
		if(topicId == 0){
			console.log("PUBLISH topic id not found!");
			return;
		}
		var publish = mqttsn.generate({cmd: 'publish', qos: packet.qos, retain: packet.retain, topicIdType: 'pre-defined', topicId: topicId, msgId: packet.messageId, payload: packet.payload});
		self.serialPort.write(publish,function(){
			console.log("Forwarding PUBLISH to client: "+publish.toString('hex'));
		});
	};
	var onPuback = function(packet){
	  	var topicId = self.msgIdTable[packet.messageId];
	  	if(topicId == null)
	  		console.log("Topic ID not found for message ID: "+data.readUInt16BE(2));
	  	else{
			var puback = mqttsn.generate({cmd: 'puback', topicId: topicId, msgId: packet.messageId, returnCode: 'Accepted'});
			self.serialPort.write(puback,function(){
				console.log("Forwarding PUBACK to client: "+puback.toString('hex'));
			});
		}
	};
	var onPubrec = function(packet){
		var pubrec = mqttsn.generate({cmd: 'pubrec', msgId: packet.messageId});
		self.serialPort.write(pubrec,function(){
			console.log("Forwarding PUBREC to client: "+pubrec.toString('hex'));
		});
	};
	var onPubrel = function(packet){
		var pubrel = mqttsn.generate({cmd: 'pubrel', msgId: packet.messageId});
		self.serialPort.write(pubrel,function(){
			console.log("Forwarding PUBREL to client: "+pubrel.toString('hex'));
		});
	};
	var onPubcomp = function(packet){
		var pubcomp = mqttsn.generate({cmd: 'pubcomp', msgId: packet.messageId});
		self.serialPort.write(pubcomp,function(){
			console.log("Forwarding PUBCOMP to client: "+pubcomp.toString('hex'));
		});
	};
	var onSuback = function(packet){
	  	if(self.msgIdTable[packet.messageId] == null)
	  		console.log("Topic ID not found for message ID: "+data.readUInt16BE(2));
	  	else{
			var suback = mqttsn.generate({cmd: 'suback', qos: (packet.granted < 3? packet.granted : 0), topicId: self.msgIdTable[packet.messageId], msgId: packet.messageId, returnCode: (packet.granted < 3? 'Accepted' : 'Rejected: not supported')});
			self.serialPort.write(suback,function(){
				console.log("Forwarding SUBACK to client: "+suback.toString('hex'));
			});
		}
	};
	var onUnsuback = function(packet){
		var unsuback = mqttsn.generate({cmd: 'unsuback', msgId: packet.messageId});
		self.serialPort.write(unsuback,function(){
			console.log("Forwarding UNSUBACK to client: "+unsuback.toString('hex'));
		});
	};
	var onPingResp = function(packet){
		var pingresp = mqttsn.generate({cmd: 'pingresp'});
		self.serialPort.write(pingresp,function(){
			console.log("Forwarding PINGRESP to client");
		});
	};
	
	this.conn = Connection(this.stream);
	this.conn.on('connack', onConnack);
	this.conn.on('publish', onPublish);
	this.conn.on('puback', onPuback);
	this.conn.on('pubrec', onPubrec);
	this.conn.on('pubrel', onPubrel);
	this.conn.on('pubcomp', onPubcomp);
	this.conn.on('suback', onSuback);
	this.conn.on('unsuback', onUnsuback);
	this.conn.on('pingresp', onPingResp);
	this.serialPort.on('data', onSerialData);
	this.clientConnected = false;
	this.parser = mqttsn.parser();

	this.parser.on('packet',function(packet){
		console.log(new Date().toLocaleTimeString()+':'+Math.round(new Date().getMilliseconds()*60/1000)+'\tMQTTSN Message: '+JSON.stringify(packet));
		switch(packet.cmd){
			case 'advertise':
				console.log("Gateway advertisement received! Gateway ID: "+packet.gwId);
				break;
			case 'searchgw':
				var gwinfo = mqttsn.generate({cmd: 'gwinfo', gwId: self.gatewayID});
				self.serialPort.write(gwinfo, function(){
					console.log("SearchGW replied, GWINFO: "+gwinfo.toString('hex'));
				});
				break;
			case 'connect':
				connectCache = {
					protocolId: "MQTT",
					protocolVersion: 4,
					keepalive: packet.duration,
					clientId: packet.clientId,
					clean: packet.cleanSession
				};
				if(packet.will){
					var willTopicReq = mqttsn.generate({cmd: 'willtopicreq'});
					self.serialPort.write(willTopicReq, function(){
						console.log("Sending will topic request, WILLTOPICREQ: "+willTopicReq.toString('hex'));
					});
				}else self.conn.connect(connectCache,function(){
					console.log("Sent CONNECT to broker from "+(packet.clientId != null? "ClientID: "+packet.toString('utf8',4) : "Anonymous Client"));
					connectCache = null;
				});
				break;
			case 'willtopic':
				if(packet.willTopic == null){
					if(connectCache != null){	//no will topic but tries to connect with will?
						var connack = mqttsn.generate({cmd: 'connack', returnCode: 'Rejected: not supported'});
						self.serialPort.write(connack,function(){
							console.log("Connection rejected due to invalid will topic, CONNACK: "+connack.toString('hex'));
						});
					}else{	//doesn't support delete will topic from server using empty WILLTOPIC
						var willTopicResp = mqttsn.generate({cmd: 'willtopicresp', returnCode: 'Rejected: not supported'});
						self.serialPort.write(willTopicResp,function(){
							console.log("Will topic rejected due to empty WILLTOPIC, WILLTOPICRESP: "+willTopicResp.toString('hex'));
						});
					}
				}else if(connectCache == null){
					console.log("Will Topic received, but we didn't request it?");
				}else{
					connectCache.will = {
						topic: packet.willTopic,
						payload: null,
						qos: packet.qos,
						retain: packet.retain
					};
					var willMsgReq = mqttsn.generate({cmd: 'willmsgreq'});
					self.serialPort.write(willMsgReq, function(){
						console.log("Sending Will Message request, WILLMSGREQ: "+willMsgReq.toString('hex'));
					});
				}
				break;
			case 'willmsg':
				if(connectCache == null){
					console.log("Will Message received, but we didn't request it?");
				}else if(packet.willMsg  == null){
					var connack = mqttsn.generate({cmd: 'connack', returnCode: 'Rejected: not supported'});
					self.serialPort.write(connack,function(){
						console.log("Connection rejected due to invalid Will Message, CONNACK: "+connack.toString('hex'));
					});
				}else{
					connectCache.will.payload = packet.willMsg;
					console.log("Connect Cache: "+JSON.stringify(connectCache));
					self.conn.connect(connectCache,function(){
						console.log("Sent CONNECT to broker from "+connectCache.clientId != null? "ClientID: "+connectCache.clientId : "Anonymous Client");
						connectCache = null;
					});
				}
				break;
			case 'register':
				var topicId = null;
				for(var i = 0 ; i < self.topicTable.length && topicId == null; i++)	//Create topicId if not topic table
					if(self.topicTable[i] == packet.topicName)
						topicId = i+1;
				if(topicId == null){
					topicId = self.topicTable.push(packet.topicName);
					console.log("REGISTERED topic "+packet.topicName+" to id: "+topicId );
				}
				var regack = mqttsn.generate({cmd: 'regack', topicId: topicId, msgId: packet.msgId, returnCode: 'Accepted'});
				self.serialPort.write(regack, function(){
					console.log("REGACK replied to the client: "+regack.toString('hex'));
				});
				break;
			case 'publish':
				var topicName = packet.topicIdType == 'pre-defined'? self.topicTable[packet.topicId-1] : packet.topicIdType == 'short topic'? packet.topicId : packet.topicName;
				if(topicName == null){
					console.log("Topic not found for id "+packet.topicId);
					break;
				}
				if(topicName == "DB"){
					console.log('DEBUG: '+packet.payload.toString('utf8'));
				}
				if(!self.clientConnected)
					break;
				self.conn.publish({
					topic: topicName,
					payload: packet.payload,
					qos: packet.qos,
					messageId: packet.msgId,
					retain: packet.retain
				}, function(){
					console.log("PUBLISH (topic: "+topicName+", payload: "+packet.payload.toString('hex')+") sent to broker! ");
				});
				break;
			case 'puback':
				self.conn.puback({messageId: packet.msgId}, function(){
					console.log("PUBACK sent to broker!");
				});
				break;
			case 'pubrec':
				self.conn.pubrec({messageId: packet.msgId}, function(){
					console.log("PUBREC sent to broker!");
				});
				break;
			case 'pubrel':
				self.conn.pubrel({messageId: packet.msgId}, function(){
					console.log("PUBREL sent to broker!");
				});
				break;
			case 'pubcomp':
				self.conn.pubcomp({messageId: packet.msgId}, function(){
					console.log("PUBCOMP sent to broker!");
				});
				break;
			case 'subscribe':
				if(packet.topicIdType == 'pre-defined' && self.topicTable[packet.topicId] == undefined){
					var suback = mqttsn.generate({cmd: 'suback', qos: packet.qos, topicId: packet.topicId, msgId: packet.msgId, returnCode: 'Rejected: invalid topic ID'});
					self.serialPort.write(suback, function(){
						console.log("Rejected due to invalid topic id! SUBACK: "+suback.toString('hex'));
					});
				}else{
					var topicName = packet.topicIdType == 'pre-defined'? self.topicTable[packet.topicId-1] : packet.topicIdType == 'short topic'? packet.topicId : packet.topicName;
					var topicId = packet.topicIdType == 'normal'? null : packet.topicId;
					for(var i = 0 ; i < self.topicTable.length && topicId == null; i++)
						if(self.topicTable[i] == topicName)
							topicId = i+1;
					if(topicId == null)
						topicId = self.topicTable.push(topicName);
					self.msgIdTable[packet.msgId] = topicId;
					self.conn.subscribe({
						dup: packet.dup,
						messageId: packet.msgId,
						subscriptions: [{topic: topicName, qos: packet.qos}]
					}, function(){
						console.log("SUBSCRIBE (topic: "+topicName+")sent to broker! MessageID: "+packet.msgId);
					});
				}
				break;
			case 'suback':
				self.conn.suback({
					granted: packet.qos,
					messageId: packet.msgId
				}, function(){
					console.log("SUBACK sent to broker! Message ID: "+packet.msgId);
				});
				break;
			case 'unsubscribe':
				if(packet.topicIdType == 'pre-defined' && self.topicTable[packet.topicId] == undefined){
					console.log("Invalid UNSUBSCRIBE due to invalid topic id!");
				}else{
					var topicName = packet.topicIdType == 'pre-defined'? self.topicTable[packet.topicId-1] : packet.topicIdType == 'short topic'? packet.topicId : packet.topicName;
					var topicId = packet.topicIdType == 'normal'? null : packet.topicId;
					for(var i = 0 ; i < self.topicTable.length && topicId == null; i++)
						if(self.topicTable[i] == topicName)
							topicId = i+1;
					if(topicId == null)
						topicId = self.topicTable.push(topicName);
					self.msgIdTable[packet.msgId] = topicId;
					self.conn.unsubscribe({
						dup: packet.dup,
						messageId: packet.msgId,
						unsubscriptions: [ topicName ]
					}, function(){
						console.log("UNSUBSCRIBE (topic: "+topicName+")sent to broker! MessageID: "+packet.msgId);
					});
				}
				break;
			case 'unsuback':
				self.conn.unsuback({
					messageId: packet.msgId
				}, function(){
					console.log("UNSUBACK sent to broker! Message ID: "+packet.msgId);
				});
				break;
			case 'pingreq':
				self.conn.pingreq({},function(){
					console.log("PINGREQ sent to broker!");
				});
				break;
			case 'disconnect':
				if(packet.duration != null){
					console.log("Sleeping not support atm...");
				}else{
					self.conn.disconnect({},function(){
						console.log("DISCONNECT sent to broker!");
					});
				}
				break;
			default:
				console.log("Unsupported packet type received: "+packet.cmd);
		}
	});
}

MQTTSN_SerialGW.prototype.disconnect = function(){
	this.serialPort.close(function(err){
		console.log("Closed serial connection.");
	});
	this.conn.disconnect({},function(){
		console.log("Disconnected from broker!");
	});
}

module.exports = MQTTSN_SerialGW;