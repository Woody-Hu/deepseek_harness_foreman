/**
 * RunProfiler — span-based profiling for one foreman run (ADR-0013).
 *
 * The profiler answers "where did the sandbox's wall clock go" with three
 * primitives:
 *   span(name, fn)   wrap a sync/async operation -> { name, start, end,
 *                    durationMs } on a monotonic clock; hierarchical dotted
 *                    names (prepare.workspace.extract nests conceptually)
 *   count(name, n)   cumulative counters (event types, uploads, packs)
 *   gauge(name, v)   last-write-wins values (payload bytes, pack count)
 *
 * report() produces the immutable snapshot: spans + counters + gauges + the
 * derived throughput metrics of the performance model
 * (docs/design/performance-modeling.md):
 *
 *   T_run  = P + B + Σ(Eᵢ + commitᵢ) + C + U + O
 *   usefulWorkRatio = ΣE / T_run      turnThroughput = turns / T_run
 *   warmupMs = P + B                 saveCostMs = U
 *
 * Only names and numbers are recorded — never payload content — so no new
 * redaction surface is introduced.
 */

const now = () => performance.now()

export class RunProfiler {
  constructor({ channel, sessionId, agentId } = {}) {
    this.meta = { channel, sessionId, agentId }
    this.startedAt = new Date()
    this.origin = now()
    this.spans = []
    this.counters = {}
    this.gauges = {}
    this.turns = [] // per-turn records (the source of the derived ΣE / Σcommit)
    this.activeTurn = undefined // the turn whose events are currently streaming
    this.endedAt = undefined
  }

  /**
   * Wrap an operation in a span. Sync and async functions are both supported;
   * the span records monotonic start/end (ms since profiler creation).
   * @template T
   * @param {string} name dotted hierarchical span name (e.g. 'turn.3.execute')
   * @param {() => T} fn
   * @returns {Promise<T> | T}
   */
  span(name, fn) {
    const start = now()
    const finish = (value) => {
      this.spans.push({ name, start, end: now(), durationMs: now() - start })
      return value
    }
    try {
      const result = fn()
      if (result !== null && typeof result?.then === 'function') {
        return result.then(finish, (error) => { finish(); throw error })
      }
      return finish(result)
    } catch (error) {
      finish()
      throw error
    }
  }

  /** Record a span from externally measured boundaries (start/end = monotonic ms since origin). */
  spanFrom(name, start, end) {
    this.spans.push({ name, start, end, durationMs: end - start })
  }

  /** Increment a counter (event types, uploads, packs...). */
  count(name, n = 1) {
    this.counters[name] = (this.counters[name] ?? 0) + n
  }

  /** Set a gauge value (last write wins). */
  gauge(name, value) {
    this.gauges[name] = value
  }

  /**
   * Open a per-turn record and make it the active turn (stream events are
   * attributed to it until endTurn). Called at prompt dispatch; execute /
   * commit / first-event-latency fields are filled by the orchestrator's
   * instrumentation.
   */
  beginTurn(turn) {
    const record = {
      turn,
      dispatchedAt: now(),
      firstEventAt: undefined,
      events: 0,
      executeMs: undefined,
      commitMs: undefined,
    }
    this.turns.push(record)
    this.activeTurn = record
    return record
  }

  /**
   * A session event on the stream: count it by type and attribute it to the
   * active turn (first-event latency + per-turn count).
   */
  noteEvent(type) {
    this.count(`event.${type}`)
    const turn = this.activeTurn
    if (turn !== undefined) {
      turn.events += 1
      if (turn.firstEventAt === undefined) turn.firstEventAt = now()
    }
  }

  /** Close the active turn record (execute/commit durations land here). */
  endTurn(record, { executeMs, commitMs } = {}) {
    if (executeMs !== undefined) record.executeMs = executeMs
    if (commitMs !== undefined) record.commitMs = commitMs
    if (this.activeTurn === record) this.activeTurn = undefined
  }

  /** Close the run (idempotent; freezes the endedAt timestamp). */
  end() {
    if (this.endedAt === undefined) this.endedAt = new Date()
    return this
  }

  /** Total duration of one dotted span family (exact or prefix match at a dot boundary). */
  #sumSpans(prefix) {
    return this.spans
      .filter((span) => span.name === prefix || span.name.startsWith(`${prefix}.`))
      .reduce((total, span) => total + span.durationMs, 0)
  }

  /** The derived throughput metrics (docs/design/performance-modeling.md §2). */
  derived() {
    const executionMs = this.turns.reduce((total, turn) => total + (turn.executeMs ?? 0), 0)
    const commitMs = this.turns.reduce((total, turn) => total + (turn.commitMs ?? 0), 0)
    const prepareMs = this.#sumSpans('prepare')
    const bootMs = this.#sumSpans('start.channel')
    const collectMs = this.#sumSpans('collect')
    const publishMs = this.#sumSpans('publish')
    const runWallMs = now() - this.origin // live wall clock at the call site
    const turns = this.turns.length
    const events = Object.entries(this.counters)
      .filter(([name]) => name.startsWith('event.'))
      .reduce((total, [, value]) => total + value, 0)
    return {
      runWallMs,
      executionMs,
      commitMs,
      prepareMs,
      bootMs,
      collectMs,
      publishMs,
      usefulWorkRatio: runWallMs > 0 ? executionMs / runWallMs : undefined,
      turnThroughputPerSec: runWallMs > 0 ? (turns * 1000) / runWallMs : undefined,
      warmupMs: prepareMs + bootMs,
      saveCostMs: publishMs,
      eventRatePerSec: executionMs > 0 ? (events * 1000) / executionMs : undefined,
      events,
      turns,
      turnDetails: this.turns.map((turn) => ({
        turn: turn.turn,
        executeMs: turn.executeMs,
        commitMs: turn.commitMs,
        firstEventLatencyMs: turn.firstEventAt !== undefined ? turn.firstEventAt - turn.dispatchedAt : undefined,
        events: turn.events,
      })),
    }
  }

  /** The full snapshot — the content of the profile.json artifact. */
  report() {
    this.end()
    return {
      schema: 1,
      meta: this.meta,
      startedAt: this.startedAt.toISOString(),
      endedAt: this.endedAt.toISOString(),
      derived: this.derived(),
      spans: this.spans.map((span) => ({ ...span, durationMs: Math.round(span.durationMs * 1000) / 1000 })),
      counters: this.counters,
      gauges: this.gauges,
    }
  }
}
