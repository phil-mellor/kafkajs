const KEEP_ALIVE_DELAY = 60000 // in ms

/**
 * @returns {import("../../types").ISocketFactory}
 */
module.exports = () => {
  const net = require('net')
  const tls = require('tls')

  return ({ host, port, ssl, onConnect }) => {
    console.log(`Socket factory: ${host} ${port} ${JSON.stringify(ssl)}`)
    console.log(JSON.stringify(Object.assign({ host, port, servername: host }, ssl)))

    const socket = ssl
      ? tls.connect(
          {
            ...Object.assign({ host, port, servername: host }, ssl),
          },
          onConnect
        )
      : net.connect({ host, port }, onConnect)

    socket.setKeepAlive(true, KEEP_ALIVE_DELAY)

    return socket
  }
}
