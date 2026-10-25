'use strict'

const assert = require('node:assert')
const { once } = require('node:events')
const { createServer } = require('node:net')
const { test } = require('node:test')
const { Client, errors } = require('..')

function readBody (body) {
  return new Promise((resolve, reject) => {
    let data = ''
    body.setEncoding('latin1')
    body.on('data', chunk => { data += chunk })
    body.on('end', () => resolve(data))
    body.on('error', reject)
  })
}

test('should not reuse an idle socket with buffered unsolicited response bytes', async (t) => {
  let responses = 0

  const server = createServer((socket) => {
    socket.on('data', () => {
      if (responses++ === 0) {
        socket.write(
          'HTTP/1.1 200 OK\r\n' +
          'Connection: keep-alive\r\n' +
          'Keep-Alive: timeout=300\r\n' +
          'Content-Length: 9\r\n' +
          '\r\n' +
          '/request1' +
          'HTTP/1.1 200 OK\r\n' +
          'Poison-Free-Socket: true\r\n' +
          'Connection: keep-alive\r\n' +
          'Keep-Alive: timeout=300\r\n' +
          'Content-Length: 0\r\n' +
          '\r\n'
        )
      } else {
        socket.end(
          'HTTP/1.1 200 OK\r\n' +
          'Connection: close\r\n' +
          'Content-Length: 9\r\n' +
          '\r\n' +
          '/request2'
        )
      }
    })
  })
  t.after(() => server.close())

  server.listen(0, '127.0.0.1')
  await once(server, 'listening')

  const client = new Client(`http://127.0.0.1:${server.address().port}`, {
    keepAliveTimeout: 300e3
  })
  t.after(() => client.close())

  // The poisoned socket must be torn down before the second request is
  // dispatched, otherwise the request races the teardown and may be written
  // onto the socket that is already going away.
  const disconnected = once(client, 'disconnect')

  const response1 = await client.request({ path: '/request1', method: 'GET' })
  assert.strictEqual(await readBody(response1.body), '/request1')

  await disconnected

  const response2 = await client.request({ path: '/request2', method: 'GET' })
  assert.strictEqual(response2.headers['poison-free-socket'], undefined)
  assert.strictEqual(await readBody(response2.body), '/request2')
})

test('should not attribute an unsolicited response to a queued request', async (t) => {
  let connections = 0
  let firstDisconnectError = null

  const server = createServer((socket) => {
    if (connections++ === 0) {
      socket.once('data', () => {
        // The legitimate response to request 1, immediately followed by an
        // unsolicited second response. Request 2 is still sitting unsent at the
        // head of the client queue when these bytes reach the parser, so the
        // unsolicited response is parsed while `kRunning === 0` with a request
        // available to attribute it to.
        socket.write(
          'HTTP/1.1 200 OK\r\n' +
          'Connection: keep-alive\r\n' +
          'Keep-Alive: timeout=300\r\n' +
          'Content-Length: 9\r\n' +
          '\r\n' +
          '/request1' +
          'HTTP/1.1 200 OK\r\n' +
          'Poison-Free-Socket: true\r\n' +
          'Connection: keep-alive\r\n' +
          'Keep-Alive: timeout=300\r\n' +
          'Content-Length: 0\r\n' +
          '\r\n'
        )
      })
    } else {
      socket.once('data', () => {
        socket.end(
          'HTTP/1.1 200 OK\r\n' +
          'Connection: close\r\n' +
          'Content-Length: 9\r\n' +
          '\r\n' +
          '/request2'
        )
      })
    }
  })
  t.after(() => server.close())

  server.listen(0, '127.0.0.1')
  await once(server, 'listening')

  const client = new Client(`http://127.0.0.1:${server.address().port}`, {
    keepAliveTimeout: 300e3,
    pipelining: 1
  })
  t.after(() => client.close())

  client.on('disconnect', (origin, targets, err) => {
    if (firstDisconnectError === null) {
      firstDisconnectError = err
    }
  })

  // Both requests are queued up front. With `pipelining: 1` the second one is
  // not written while the first is in flight, so it is still queued when the
  // trailing unsolicited response is parsed.
  const [response1, response2] = await Promise.all([
    client.request({ path: '/request1', method: 'GET' }),
    client.request({ path: '/request2', method: 'GET' })
  ])

  assert.strictEqual(await readBody(response1.body), '/request1')

  // Request 2 must be answered by a fresh connection, never by the bytes the
  // server pushed unsolicited onto the first one.
  assert.strictEqual(response2.headers['poison-free-socket'], undefined)
  assert.strictEqual(await readBody(response2.body), '/request2')

  // The unsolicited response must have been rejected outright by the
  // bad-response guard. Without the guard it is handed to the queued request
  // and blows undici's own `assert(this.timeoutType === TIMEOUT_HEADERS)`.
  assert.ok(firstDisconnectError, 'expected the poisoned socket to be torn down')
  assert.strictEqual(firstDisconnectError.code, 'UND_ERR_SOCKET')
  assert.strictEqual(firstDisconnectError.message, 'bad response')
})

test('should validate an idle keep-alive socket before writing the next request', async (t) => {
  const serverSockets = []

  const server = createServer((socket) => {
    const connection = serverSockets.push(socket)

    socket.on('error', () => {})
    socket.on('data', (chunk) => {
      const path = chunk.toString('latin1').split(' ')[1]
      socket.write(
        'HTTP/1.1 200 OK\r\n' +
        'Connection: keep-alive\r\n' +
        'Keep-Alive: timeout=300\r\n' +
        `X-Connection: ${connection}\r\n` +
        `Content-Length: ${path.length}\r\n` +
        '\r\n' +
        path
      )
    })
  })
  t.after(() => {
    for (const socket of serverSockets) {
      socket.destroy()
    }
    server.close()
  })

  server.listen(0, '127.0.0.1')
  await once(server, 'listening')

  const client = new Client(`http://127.0.0.1:${server.address().port}`, {
    keepAliveTimeout: 300e3
  })
  t.after(() => client.destroy())

  const disconnects = []
  client.on('disconnect', (origin, targets, err) => {
    disconnects.push(err)
  })

  const response1 = await client.request({ path: '/request1', method: 'GET' })
  assert.strictEqual(await readBody(response1.body), '/request1')
  assert.strictEqual(response1.headers['x-connection'], '1')
  assert.strictEqual(serverSockets.length, 1)

  // The keep-alive socket is now idle. Make an unsolicited response reach the
  // client's kernel buffer without letting the event loop read it yet, then
  // reuse the socket for the next request from the same tick.
  const response2 = await new Promise((resolve, reject) => {
    setTimeout(() => {
      serverSockets[0].write(
        'HTTP/1.1 200 OK\r\n' +
        'Poison-Free-Socket: true\r\n' +
        'Connection: keep-alive\r\n' +
        'Keep-Alive: timeout=300\r\n' +
        'Content-Length: 0\r\n' +
        '\r\n'
      )
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 100)
      client.request({ path: '/request2', method: 'GET' }).then(resolve, reject)
    }, 50)
  })

  assert.strictEqual(response2.headers['poison-free-socket'], undefined)
  assert.strictEqual(response2.headers['x-connection'], '2')
  assert.strictEqual(await readBody(response2.body), '/request2')

  assert.strictEqual(disconnects.length, 1)
  assert.ok(disconnects[0] instanceof errors.SocketError)
  assert.strictEqual(disconnects[0].message, 'bad response')
})
