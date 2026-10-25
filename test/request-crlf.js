'use strict'

const assert = require('node:assert')
const { tspl } = require('@matteo.collina/tspl')
const { createServer } = require('node:http')
const { test, after } = require('node:test')
const { request, errors } = require('..')
const { once } = require('node:events')

test('should validate content-type CRLF Injection', async (t) => {
  t = tspl(t, { plan: 2 })

  const server = createServer((req, res) => {
    t.fail('should not receive any request')
    res.statusCode = 200
    res.end('hello')
  })

  after(() => server.close())

  server.listen(0)

  await once(server, 'listening')
  try {
    await request(`http://localhost:${server.address().port}`, {
      method: 'GET',
      headers: {
        'content-type': 'application/json\r\n\r\nGET /foo2 HTTP/1.1'
      }
    })
    t.fail('request should fail')
  } catch (e) {
    t.ok(e instanceof errors.InvalidArgumentError)
    t.strictEqual(e.message, 'invalid content-type header')
  }
  await t.completed
})

test('should validate blob body content-type CRLF Injection', async () => {
  let receivedRequest = false
  const server = createServer({ joinDuplicateHeaders: true }, (req, res) => {
    receivedRequest = true
    res.statusCode = 200
    res.end('hello')
  })

  after(() => server.close())

  server.listen(0)

  await once(server, 'listening')

  class MaliciousBlob extends Blob {
    get type () {
      return 'text/html\r\nX-Injected: true'
    }
  }

  await assert.rejects(
    request(`http://localhost:${server.address().port}/endpoint`, {
      method: 'POST',
      body: new MaliciousBlob(['hello'])
    }),
    (e) => e instanceof errors.InvalidArgumentError && e.message === 'invalid content-type header'
  )
  assert.strictEqual(receivedRequest, false)
})

test('should validate non-string blob body content-type CRLF Injection', async () => {
  let receivedRequest = false
  const server = createServer({ joinDuplicateHeaders: true }, (req, res) => {
    receivedRequest = true
    res.statusCode = 200
    res.end('hello')
  })

  after(() => server.close())

  server.listen(0)

  await once(server, 'listening')

  // A blob-like whose `type` is not a string: it must be stringified before
  // being validated, otherwise the CRLF sequence is only introduced later when
  // the header block is serialized.
  const maliciousBlobLike = {
    size: 5,
    stream () {
      return new Blob(['hello']).stream()
    },
    get type () {
      return { toString: () => 'text/html\r\nX-Injected: true' }
    },
    get [Symbol.toStringTag] () {
      return 'Blob'
    }
  }

  await assert.rejects(
    request(`http://localhost:${server.address().port}/endpoint`, {
      method: 'POST',
      body: maliciousBlobLike
    }),
    (e) => e instanceof errors.InvalidArgumentError && e.message === 'invalid content-type header'
  )
  assert.strictEqual(receivedRequest, false)
})
