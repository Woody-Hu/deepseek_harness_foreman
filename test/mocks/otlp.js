/**
 * Mock OTLP/HTTP logs collector (for tests): receives the OTLP JSON exports of
 * dsh's session-telemetry-otel and flattens them into a record list for
 * assertions.
 * Endpoint: POST /v1/logs (application/json; gzip content-encoding tolerated).
 */
import { createServer } from 'node:http'
import { gunzipSync } from 'node:zlib'

/** Flatten an OTLP AnyValue into a plain JS value. */
function anyValue(value) {
  if (value === null || value === undefined) return value
  const key = Object.keys(value)[0]
  if (key === undefined) return value
  if (key === 'stringValue') return value.stringValue
  if (key === 'intValue' || key === 'doubleValue') return Number(value[key])
  if (key === 'boolValue') return value.boolValue
  if (key === 'bytesValue') return '<bytes>'
  if (key === 'arrayValue') return value.arrayValue.values.map(anyValue)
  if (key === 'kvlistValue') {
    const out = {}
    for (const item of value.kvlistValue.values ?? []) out[item.key] = anyValue(item.value)
    return out
  }
  return value
}

/** Parse an OTLP/JSON logs request body into a record array. */
export function parseOtlpLogs(body) {
  const records = []
  for (const resourceLog of body.resourceLogs ?? []) {
    const resource = {}
    for (const attr of resourceLog.resource?.attributes ?? []) resource[attr.key] = anyValue(attr.value)
    for (const scopeLog of resourceLog.scopeLogs ?? []) {
      for (const record of scopeLog.logRecords ?? []) {
        const attributes = {}
        for (const attr of record.attributes ?? []) attributes[attr.key] = anyValue(attr.value)
        records.push({
          resource,
          scope: scopeLog.scope?.name ?? '',
          timeUnixNano: record.timeUnixNano,
          severityNumber: record.severityNumber,
          severityText: record.severityText,
          body: anyValue(record.body),
          attributes,
        })
      }
    }
  }
  return records
}

/**
 * Start the mock collector.
 * @returns {Promise<{server: import('node:http').Server, port: number, records: object[], rawBodies: string[], requests: object[], close(): Promise<void>}>}
 */
export function startMockOtlpCollector() {
  const records = []
  const rawBodies = []
  const requests = [] // headers of each export request (verifies exporter.headers passthrough)
  const server = createServer((request, response) => {
    if (request.method !== 'POST') {
      response.writeHead(404).end()
      return
    }
    const chunks = []
    request.on('data', (chunk) => { chunks.push(chunk) })
    request.on('end', () => {
      requests.push({ headers: { ...request.headers } })
      let buffer = Buffer.concat(chunks)
      if ((request.headers['content-encoding'] ?? '') === 'gzip') buffer = gunzipSync(buffer)
      const text = buffer.toString('utf8')
      rawBodies.push(text)
      let body
      try { body = JSON.parse(text) } catch {
        response.writeHead(400).end('bad json')
        return
      }
      records.push(...parseOtlpLogs(body))
      response.writeHead(200).end()
    })
  })
  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address()
      resolve({
        server,
        port,
        records,
        rawBodies,
        requests,
        close: () => new Promise((done) => { server.close(() => { done() }) }),
      })
    })
  })
}
