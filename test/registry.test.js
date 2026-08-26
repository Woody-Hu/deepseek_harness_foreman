/**
 * Channel registry unit tests (ADR-0009/ADR-0012): canonical id resolution,
 * legacy aliases, fail-loud unknown ids, capability declarations, factory
 * instantiation, and config-file validation of harness.channel.
 */
import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { CHANNELS, resolveChannelId, channelEntry, createChannel } from '../src/channels/registry.js'
import { SdkChannel } from '../src/channels/sdk-channel.js'
import { WebChannel } from '../src/channels/web-channel.js'
import { CodexChannel } from '../src/channels/codex-channel.js'
import { loadForemanConfig } from '../src/config.js'

// ---------------------------------------------------------------- id resolution

test('registry: canonical ids are the three harness channels (ADR-0009)', () => {
  assert.deepEqual([...CHANNELS].sort(), ['codex', 'dsh-sdk', 'dsh-web'])
})

test('registry: canonical ids resolve to themselves; legacy aliases map canonically', () => {
  assert.equal(resolveChannelId('dsh-sdk'), 'dsh-sdk')
  assert.equal(resolveChannelId('dsh-web'), 'dsh-web')
  assert.equal(resolveChannelId('codex'), 'codex')
  assert.equal(resolveChannelId('stdio'), 'dsh-sdk') // legacy alias
  assert.equal(resolveChannelId('web'), 'dsh-web') // legacy alias
})

test('registry: unknown channel fails loud with the accepted values listed', () => {
  assert.throws(() => resolveChannelId('claude-code'), (error) => {
    assert.match(error.message, /unknown channel 'claude-code'/)
    assert.match(error.message, /dsh-sdk, dsh-web, codex/)
    assert.match(error.message, /stdio, web/)
    return true
  })
  assert.throws(() => resolveChannelId(undefined), /unknown channel 'undefined'/)
})

// ---------------------------------------------------------------- capabilities

test('registry: entries declare their capabilities (hitl / compositionFile / sessionRoot)', () => {
  const sdk = channelEntry('dsh-sdk')
  assert.equal(sdk.compositionFile, 'cordis.yml')
  assert.equal(sdk.hitl, undefined)
  assert.ok(sdk.sessionRoot('/w').endsWith(join('.sessions')))

  const web = channelEntry('web') // via alias
  assert.equal(web.id, 'dsh-web')
  assert.equal(web.compositionFile, 'web-patch.yml')
  assert.equal(web.hitl, true)

  const codex = channelEntry('codex')
  assert.equal(codex.compositionFile, undefined) // no dsh composition config
  assert.equal(codex.hitl, undefined)
  assert.ok(codex.sessionRoot('/w').endsWith(join('.codex')))
})

// ---------------------------------------------------------------- instantiation

test('registry: createChannel instantiates the right class per id with uniform ctx', () => {
  const ctx = {
    options: { repoRoot: '/repo', workdir: '/w', sessionId: 's1', modelEnv: {}, envExtra: {} },
    config: { harness: { codex: { model: 'gpt-5.1-codex' } } },
    handlers: { onEvent: () => {}, onStatus: () => {} },
    workspaceDir: '/w/workspace',
    sessionRoot: '/w/.sessions',
    configPath: '/w/cordis.yml',
    telemetry: {},
  }
  assert.ok(createChannel('dsh-sdk', ctx).channel instanceof SdkChannel)
  const web = createChannel('dsh-web', ctx)
  assert.ok(web.channel instanceof WebChannel)
  assert.equal(web.hitl, true)
  const codex = createChannel('codex', ctx)
  assert.ok(codex.channel instanceof CodexChannel)
  assert.equal(codex.hitl, false)
  assert.equal(codex.channel.options.model, 'gpt-5.1-codex') // config-file wiring reaches the channel
  assert.throws(() => createChannel('nope', ctx), /unknown channel 'nope'/)
})

// ---------------------------------------------------------------- config validation

test('config: harness.channel accepts canonical ids and legacy aliases; typos fail loud', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'foreman-registry-'))
  try {
    const write = (name, harness) => writeFile(join(dir, name), JSON.stringify({ harness }))
    await write('ok-codex.json', { channel: 'codex' })
    await write('ok-alias.json', { channel: 'stdio' })
    await write('typo.json', { channel: 'codex-server' })

    assert.equal((await loadForemanConfig(join(dir, 'ok-codex.json'))).harness.channel, 'codex')
    assert.equal((await loadForemanConfig(join(dir, 'ok-alias.json'))).harness.channel, 'stdio')
    await assert.rejects(loadForemanConfig(join(dir, 'typo.json')), (error) => {
      assert.match(error.message, /unknown channel 'codex-server'/)
      assert.match(error.message, /dsh-sdk, dsh-web, codex/)
      return true
    })
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
})
