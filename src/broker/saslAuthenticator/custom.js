const { KafkaJSSASLAuthenticationError } = require('../../errors')

module.exports = class CustomAuthenticator {
  constructor(connection, logger, saslAuthenticate) {
    this.connection = connection
    this.logger = logger.namespace('SASLCustomAuthenticator')
    this.saslAuthenticate = saslAuthenticate
  }

  async authenticate() {
    const { sasl } = this.connection
    console.log(JSON.stringify(sasl))
    console.log(sasl.protocol)
    console.log(sasl.protocol?.request)
    console.log(typeof sasl.protocol?.request)
    console.log(sasl.protocol?.response)
    console.log(typeof sasl.protocol?.response)
    if (!sasl.protocol || typeof sasl.protocol.request !== 'function') {
      throw new KafkaJSSASLAuthenticationError(
        'SASL Custom: Invalid protocol request or response handler'
      )
    }
    if (
      !sasl.protocol.authExpectResponse &&
      (!sasl.protocol.response || typeof sasl.protocol.response !== 'function')
    ) {
      throw new KafkaJSSASLAuthenticationError(
        'SASL Custom: Invalid protocol request or response handler'
      )
    }

    const request = await sasl.protocol.request(this.connection)
    const response = sasl.protocol.authExpectResponse ? sasl.protocol.response() : undefined

    const { host, port } = this.connection
    const broker = `${host}:${port}`

    try {
      this.logger.debug('Authenticate with SASL CUSTOM', { broker })
      await this.saslAuthenticate({ request, response })
      this.logger.debug('SASL CUSTOM authentication successful', { broker })
    } catch (e) {
      const error = new KafkaJSSASLAuthenticationError(
        `SASL CUSTOM authentication failed: ${e.message}`
      )
      this.logger.error(error.message, { broker })
      throw error
    }
  }
}
