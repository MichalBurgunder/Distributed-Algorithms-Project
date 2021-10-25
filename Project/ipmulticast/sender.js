const MULTICAST_PORT = process.env.APP_MULTICAST_PORT;
const MULTICAST_ADDR = process.env.APP_MULTICAST_ADDR;

const id = process.argv[2] || 0;
const dgram = require("dgram");
const sender = dgram.createSocket("udp4");

sender.bind(function () {
  sender.setBroadcast(true);
  sender.setMulticastTTL(128);
  sender.addMembership(MULTICAST_ADDR);
});

setInterval(broadcast, 1000);

function broadcast() {
  var message = Buffer.from(new Date().toLocaleTimeString());
  sender.send(message, 0, message.length, MULTICAST_PORT, MULTICAST_ADDR);
  console.log("Sender [" + id + '] sent "' + message + '" to the wire...');
}
