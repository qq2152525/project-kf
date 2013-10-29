express = require 'express'
app = module.exports = express.createServer()

app.configure ()->
  app.use(express.logger())

app.configure 'development', ()->
  app.use express.errorHandler
    dumpExceptions: true
    showStack: true

app.configure 'production', ()->
  app.use(express.errorHandler())

require('./config/routes')(app)

app.listen(8885)
