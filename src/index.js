/**
 * Foreman public API.
 *
 * Foreman is a cloud sandbox runner for DeepSeek Harness (dsh): it restores a
 * workspace and session from object storage, launches dsh inside the sandbox,
 * forwards events/traces to the cloud, intercepts secrets, and publishes the
 * run's artifacts back to storage. See README.md for the architecture.
 */
export { Foreman, SseGateway, makeWorkdir } from './foreman.js'
export { SdkChannel } from './channels/sdk-channel.js'
export { WebChannel } from './channels/web-channel.js'
export { CheckpointKeeper, buildChangePack, applyChangePack, extractChangePack, packKey, INDEX_KEY } from './core/checkpoint.js'
export { GitWorkspace } from './core/git-workspace.js'
export {
  fileManifest, diffManifests, packageWorkspace, extractArchive,
  archiveDirectory, changesFromSessionEvents,
} from './core/workspace.js'
export { isSecretOrExcludedPath, redactText, redactFileBuffer, redactJson } from './core/redact.js'
export { uploadArtifact, downloadArtifact, deleteArtifact, publishBusEvent } from './control-plane.js'
export { createEventFormatter, renderSseLine } from './events/formats.js'
export { registerProtocol, resolveProtocol, listProtocols } from './events/protocols/registry.js'
export { loadForemanConfig, resolveConfigPath } from './config.js'
export { createEventBus } from './events/event-bus.js'
export { TraceShipper } from './observability/trace-shipper.js'
export { createSnapshotSink } from './storage/snapshot-sink.js'
