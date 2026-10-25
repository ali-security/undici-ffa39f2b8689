'use strict'

// WebSocket reads its receive limits (maxPayloadSize, maxFragments) from the
// dispatcher's `webSocketOptions`. A dispatcher that doesn't provide any must
// get the default limits, not none.
//
// Each test waits for the close code the server observes, so nothing depends
// on timing. If a message gets through instead, the client closes normally
// and the test fails on the close code rather than hanging.

const { test } = require('node:test')
const assert = require('node:assert')
const { once } = require('node:events')
const { createDeflateRaw, constants } = require('node:zlib')
const { WebSocketServer } = require('ws')
const { Agent, WebSocket } = require('../..')

// A dispatcher that only implements `dispatch`, so it has no webSocketOptions.
function customDispatcher (t) {
  const agent = new Agent()
  t.after(() => agent.close())
  return { dispatch: (opts, handler) => agent.dispatch(opts, handler) }
}

// Starts a server that runs `send(ws)` for each connection and resolves
// `closed` with the close code and reason it receives.
async function startServer (t, send) {
  const server = new WebSocketServer({ port: 0, perMessageDeflate: true })
  t.after(() => server.close())
  await once(server, 'listening')

  const closed = new Promise((resolve) => {
    server.on('connection', (ws) => {
      ws.on('close', (code, reason) => resolve({ code, reason: reason.toString() }))
      send(ws)
    })
  })

  return { url: `ws://127.0.0.1:${server.address().port}`, closed }
}

// Resolves with the size of the first message, closing the connection.
function connect (t, url, dispatcher) {
  const ws = new WebSocket(url, { dispatcher })
  t.after(() => ws.close())
  return new Promise((resolve) => {
    ws.onmessage = (event) => {
      resolve(event.data.size)
      ws.close()
    }
  })
}

function sendFragments (count) {
  return (ws) => {
    const fragment = Buffer.from('a')
    for (let i = 0; i < count; i++) {
      ws.send(fragment, { binary: true, compress: false, fin: i === count - 1 })
    }
  }
}

// One compressed frame that inflates to just over the 128 MB default, while
// only about 128 KB goes over the wire (a decompression bomb).
function sendDecompressionBomb (ws) {
  const deflate = createDeflateRaw()
  const chunks = []
  deflate.on('data', (chunk) => chunks.push(chunk))
  const megabyte = Buffer.alloc(1024 * 1024)
  for (let i = 0; i < 129; i++) {
    deflate.write(megabyte)
  }
  deflate.flush(constants.Z_SYNC_FLUSH, () => {
    // permessage-deflate drops the trailing 00 00 ff ff of the sync flush.
    const stream = Buffer.concat(chunks)
    const payload = stream.subarray(0, stream.length - 4)

    // Server -> client binary frame, FIN=1, RSV1=1 (compressed), 64-bit length
    const header = Buffer.alloc(10)
    header[0] = 0xC2
    header[1] = 0x7f
    header.writeUInt32BE(0, 2)
    header.writeUInt32BE(payload.length, 6)
    ws._socket.write(Buffer.concat([header, payload]))
  })
}

test('a dispatcher without webSocketOptions gets the default maxFragments', async (t) => {
  // One more than the default of 131072.
  const server = await startServer(t, sendFragments(131073))

  connect(t, server.url, customDispatcher(t))
  assert.deepStrictEqual(await server.closed, { code: 1008, reason: 'Too many message fragments' })
})

test('a dispatcher without webSocketOptions gets the default maxPayloadSize for decompression', async (t) => {
  const server = await startServer(t, sendDecompressionBomb)

  connect(t, server.url, customDispatcher(t))
  const { code } = await server.closed
  assert.strictEqual(code, 1009)
})
