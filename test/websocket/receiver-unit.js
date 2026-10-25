'use strict'

const { test } = require('node:test')
const assert = require('node:assert')
const { ByteParser } = require('../../lib/web/websocket/receiver')
const { states } = require('../../lib/web/websocket/constants')
const { kController, kResponse, kReadyState } = require('../../lib/web/websocket/symbols')

const invalidFrame = Buffer.from([0x82, 0x7F, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01])

test('ByteParser rejects 64-bit payload lengths with a non-zero upper word', (t) => {
  const calls = {
    abort: 0,
    close: 0
  }

  const handler = {
    [kReadyState]: states.CONNECTING,
    [kController]: {
      abort: () => {
        calls.abort += 1
      }
    },
    [kResponse]: null,
    dispatchEvent: () => {},
    onSocketClose: () => {
      calls.close += 1
    },
    closeState: new Set()
  }

  const parser = new ByteParser(handler)

  parser.write(invalidFrame)

  return new Promise((resolve) => {
    setImmediate(() => {
      assert.strictEqual(calls.abort, 1)
      parser.destroy()
      resolve()
    })
  })
})

test('ByteParser rejects 64-bit payload lengths that do not fit in 31 bits', async (t) => {
  const cases = [
    // upper word overflows when shifted, producing a negative length
    [0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01],
    // upper word overflows when shifted, producing a length of 0
    [0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05],
    // small non-zero upper word must not be truncated into a small length
    [0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00],
    // lower word alone exceeds 2^31 - 1
    [0x00, 0x00, 0x00, 0x00, 0x80, 0x00, 0x00, 0x00],
    [0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF]
  ]

  for (const length of cases) {
    const calls = { abort: 0, errors: [] }

    const handler = {
      [kReadyState]: states.CONNECTING,
      [kController]: {
        abort: () => {
          calls.abort += 1
        }
      },
      [kResponse]: null,
      dispatchEvent: (event) => {
        calls.errors.push(event.message)
      }
    }

    const parser = new ByteParser(handler)

    assert.doesNotThrow(() => parser.write(Buffer.from([0x82, 0x7F, ...length])))

    await new Promise((resolve) => setImmediate(resolve))

    assert.strictEqual(calls.abort, 1, `length ${Buffer.from(length).toString('hex')}`)
    assert.deepStrictEqual(calls.errors, ['Received payload length > 2^31 bytes.'])
    parser.destroy()
  }
})
