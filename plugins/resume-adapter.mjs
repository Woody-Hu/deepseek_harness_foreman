/**
 * foreman resume-adapter — adds session-resume capability to the SDK stdio
 * channel (runner-bundled adapter layer).
 *
 * Background: dsh's SDK JSON-RPC narrow protocol only exposes the
 * initialize/session/prompt/shutdown methods, and the server calls
 * agents.create for every sessionId; when sessionRoot already holds
 * persisted logs for that id it throws "id collision" (the web/ApiProxy mode
 * natively supports cold resume via agents.resume instead).
 *
 * This plugin wraps agents.create: if the requested sessionId already has
 * logs (with a cwd) in the persistence backend, it reroutes to
 * agents.resume — same-id session resume across sandboxes. Fresh sessions
 * behave unchanged.
 *
 * This is a deployment-side adapter (a cordis.yml local relative-path
 * plugin); it modifies no dsh source code.
 */
export const name = 'foreman-resume-adapter'
export const inject = ['agents', 'sessionPersistence']

export function apply(ctx) {
  const registry = ctx.agents
  const originalCreate = registry.create.bind(registry)
  registry.create = async (options) => {
    const persistence = ctx.get('sessionPersistence')
    if (persistence !== undefined && options.sessionId !== undefined) {
      const listed = await persistence.list()
      const persisted = listed.find(
        (meta) => String(meta.id) === String(options.sessionId) && meta.cwd !== undefined,
      )
      if (persisted !== undefined) {
        return await registry.resume({
          resumeSessionId: options.sessionId,
          ...(options.agentOptions === undefined ? {} : { agentOptions: options.agentOptions }),
        })
      }
    }
    return await originalCreate(options)
  }
}
