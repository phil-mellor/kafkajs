const { KafkaJSSASLAuthenticationError } = require('../../errors')

module.exports = class CustomAuthenticator {
  constructor(connection, logger, saslAuthenticate) {
    this.connection = connection
    this.logger = logger.namespace('SASLCustomAuthenticator')
    this.saslAuthenticate = saslAuthenticate
  }

  async authenticate() {
    const { sasl } = this.connection
    if (
      !sasl.protocol ||
      typeof sasl.protocol.request !== 'function' ||
      typeof sasl.protocol.response !== 'function'
    ) {
      throw new KafkaJSSASLAuthenticationError(
        'SASL Custom: Invalid protocol request or response handler'
      )
    }

    const request = sasl.protocol.request()
    const response = sasl.protocol.response

    const { host, port } = this.connection
    const broker = `${host}:${port}`

    try {
      this.logger.debug('Authenticate with SASL PLAIN', { broker })
      await this.saslAuthenticate({ request, response })
      this.logger.debug('SASL PLAIN authentication successful', { broker })
    } catch (e) {
      const error = new KafkaJSSASLAuthenticationError(
        `SASL CUSTOM authentication failed: ${e.message}`
      )
      this.logger.error(error.message, { broker })
      throw error
    }
  }
}
