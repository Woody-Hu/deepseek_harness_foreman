/**
 * Control-plane client — the HTTP surface foreman talks to in the cloud:
 * an artifact store (bucketed object storage) plus a system message bus.
 *
 * Endpoints:
 *   PUT    /storage/<bucket>/<key>   upload an artifact (raw bytes; bucket = agentId)
 *   GET    /storage/<bucket>/<key>   download an artifact ("restore from remote object storage" before a run)
 *   DELETE /storage/<bucket>/<key>   delete an artifact (checkpoint retention drops expired packs)
 *   POST   /bus/events               publish a bus event (e.g. sandbox.reclaim-requested)
 *
 * A control-plane handle is any object exposing `baseUrl`; the in-repo mock
 * server (test/mocks/control-plane.js) implements the same surface for
 * keyless end-to-end runs.
 */

/**
 * Upload an artifact to the control-plane object store.
 * @param {{ baseUrl: string }} controlPlane
 * @param {string} bucket
 * @param {string} key
 * @param {Buffer} buffer
 */
export async function uploadArtifact(controlPlane, bucket, key, buffer) {
  const response = await fetch(`${controlPlane.baseUrl}/storage/${bucket}/${key}`, {
    method: 'PUT',
    body: buffer,
  })
  if (!response.ok) throw new Error(`artifact upload failed: ${key} -> ${response.status}`)
}

/**
 * Download an artifact from the control-plane object store.
 * @param {{ baseUrl: string }} controlPlane
 * @param {string} bucket
 * @param {string} key
 * @returns {Promise<Buffer>}
 */
export async function downloadArtifact(controlPlane, bucket, key) {
  const response = await fetch(`${controlPlane.baseUrl}/storage/${bucket}/${key}`)
  if (!response.ok) throw new Error(`artifact download failed: ${key} -> ${response.status}`)
  return Buffer.from(await response.arrayBuffer())
}

/**
 * Delete an artifact from the control-plane object store (checkpoint retention
 * evicts expired packs).
 * @param {{ baseUrl: string }} controlPlane
 * @param {string} bucket
 * @param {string} key
 */
export async function deleteArtifact(controlPlane, bucket, key) {
  const response = await fetch(`${controlPlane.baseUrl}/storage/${bucket}/${key}`, {
    method: 'DELETE',
  })
  if (!response.ok) throw new Error(`artifact delete failed: ${key} -> ${response.status}`)
}

/**
 * Publish an event to the system message bus.
 * @param {{ baseUrl: string }} controlPlane
 * @param {object} event
 */
export async function publishBusEvent(controlPlane, event) {
  const response = await fetch(`${controlPlane.baseUrl}/bus/events`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(event),
  })
  if (!response.ok) throw new Error(`bus publish failed: ${response.status}`)
}
