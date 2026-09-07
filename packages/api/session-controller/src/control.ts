/** Live Session queue, jobs, and projection state with reconnect baselines. */

import type { Context } from '@deepseek-ai/cordis'
import type { Agent } from '@deepseek-ai/dsh-agent'
import { Deque } from '@deepseek-ai/dsh-deque'
import type { JobSnapshot } from '@deepseek-ai/dsh-jobs'
import type {
  Session, SessionEvent, SessionEventMap, SessionId, UserMessage,
} from '@deepseek-ai/dsh-session'
import type { JsonValue } from '@deepseek-ai/dsh-util-values'
import type {
  SessionControlBaseline,
  SessionControlFrame,
  SessionJob,
  SessionProjectionBaseline,
  SessionProjectionValues,
  SessionQueuedItem,
} from './types.ts'

const PROJECTION_BROADCAST_INTERVAL_MS = 16

type ProjectionFrame = Extract<SessionControlFrame, { readonly type: 'projection' }>

interface ProjectionSlot {
  readonly sessionId: SessionId
  readonly key: string
}

/**
 * Ordered newest-value set for whole projection replacements. Replacing one
 * key moves it to the end so draining preserves the order of last occurrence
 * across different projection keys.
 */
class ProjectionAccumulator {
  private readonly ordered = new Map<ProjectionSlot, ProjectionFrame>()
  private readonly bySession = new Map<SessionId, Map<string, ProjectionSlot>>()

  push(frame: ProjectionFrame): boolean {
    let byKey = this.bySession.get(frame.sessionId)
    const existing = byKey?.get(frame.key)
    if (existing !== undefined) {
      const previous = this.ordered.get(existing)
      if (previous !== undefined && frame.seq <= previous.seq) return false
      this.ordered.delete(existing)
      this.ordered.set(existing, frame)
      return true
    }

    const slot: ProjectionSlot = { sessionId: frame.sessionId, key: frame.key }
    if (byKey === undefined) {
      byKey = new Map()
      this.bySession.set(frame.sessionId, byKey)
    }
    byKey.set(frame.key, slot)
    this.ordered.set(slot, frame)
    return true
  }

  drain(): ProjectionFrame[] {
    if (this.ordered.size === 0) return []
    const frames = [...this.ordered.values()]
    this.ordered.clear()
    this.bySession.clear()
    return frames
  }
}

/** Owns the Host-wide Session control stream. */
export class SessionControlController {
  private readonly streams = new Set<ControlQueue>()
  private readonly projectionBatch = new ProjectionAccumulator()
  private projectionFlushTimer: ReturnType<typeof setTimeout> | undefined

  /** @param ctx - Host context carrying live Agent, projection, and jobs services. */
  constructor(private readonly ctx: Context) {
    ctx.on('session/event', (session, event) => { this.onSessionEvent(session, event) })
    ctx.sessionProjections.onChanged((session, key, value, seq) => {
      this.broadcast({
        type: 'projection',
        sessionId: session.id,
        key,
        value: value as JsonValue,
        seq,
      })
    })
    ctx.inject(['jobs'], (jobsCtx) => {
      jobsCtx.jobs.onJobsChanged((owner) => { this.onJobsChanged(owner) })
    })
    ctx.on('session/created', (session) => {
      const jobs = this.jobsFor(this.ctx.agents.get(session.id))
      if (jobs.length > 0) this.broadcast({ type: 'jobs', sessionId: session.id, jobs })
    })
    ctx.effect(() => () => {
      // Publish replacements already admitted before ending live streams.
      this.flushProjectionBatch()
      for (const stream of this.streams) stream.end()
      this.streams.clear()
    }, 'session-controller.control')
  }

  /**
   * Open one generation of Host-wide live control state.
   * @param signal - Remote stream cancellation.
   * @returns one complete baseline followed by live replacement frames.
   */
  async *control(signal: AbortSignal): AsyncIterable<SessionControlFrame> {
    signal.throwIfAborted()
    // Pending frames predate this stream; its baseline already represents the
    // registry's current cut, while existing streams still need those frames.
    this.flushProjectionBatch()
    const queue = new ControlQueue()
    this.streams.add(queue)
    try {
      yield { type: 'baseline', value: this.baseline() }
      yield* queue.iterate(signal)
    } finally {
      this.streams.delete(queue)
      queue.end()
    }
  }

  private baseline(): SessionControlBaseline {
    const sessions = this.ctx.sessions.list()
    const queues = Object.create(null) as Record<SessionId, readonly SessionQueuedItem[]>
    const jobs = Object.create(null) as Record<SessionId, readonly SessionJob[]>
    for (const session of sessions) {
      const agent = this.ctx.agents.get(session.id)
      queues[session.id] = agent?.session === session ? queueItems(agent) : []
      jobs[session.id] = this.jobsFor(agent)
    }
    return {
      queues,
      jobs,
      projections: this.projectionBaseline(sessions),
    }
  }

  private projectionBaseline(
    sessions: readonly Session[],
  ): Readonly<Record<SessionId, SessionProjectionBaseline>> {
    const blocks = Object.create(null) as Record<SessionId, SessionProjectionBaseline>
    for (const session of sessions) {
      const snapshot = this.ctx.sessionProjections.snapshot(session)
      blocks[session.id] = {
        asOfSeq: snapshot.asOfSeq,
        // Every projection definition validates its value before snapshot publication.
        values: snapshot.values as SessionProjectionValues,
      }
    }
    return blocks
  }

  private onSessionEvent(session: Session, event: SessionEvent): void {
    if (event.type !== 'agent/inbox/spliced') return
    const agent = this.ctx.agents.get(session.id)
    if (agent?.session !== session) return
    this.broadcast({
      type: 'queue',
      sessionId: session.id,
      items: queueItems(agent, event.data),
    })
  }

  private onJobsChanged(owner: Agent | undefined): void {
    if (owner !== undefined) {
      this.broadcast({ type: 'jobs', sessionId: owner.id, jobs: this.jobsFor(owner) })
      return
    }
    for (const session of this.ctx.sessions.list()) {
      this.broadcast({
        type: 'jobs',
        sessionId: session.id,
        jobs: this.jobsFor(this.ctx.agents.get(session.id)),
      })
    }
  }

  private jobsFor(agent: Agent | undefined): SessionJob[] {
    const jobs = this.ctx.get('jobs')
    return jobs === undefined ? [] : jobs.list(agent).map(jobView)
  }

  private broadcast(frame: SessionControlFrame): void {
    if (frame.type === 'projection') {
      // With no live consumer the next baseline is authoritative, so transport
      // does not retain projection-only state.
      if (this.streams.size === 0) return
      if (!this.projectionBatch.push(frame)) return
      if (this.projectionFlushTimer === undefined) {
        this.projectionFlushTimer = setTimeout(() => {
          this.projectionFlushTimer = undefined
          this.flushProjectionBatch()
        }, PROJECTION_BROADCAST_INTERVAL_MS)
      }
      return
    }

    // Queue/jobs frames are ordering barriers. Every earlier projection
    // replacement is published before the barrier itself.
    this.flushProjectionBatch()
    this.broadcastNow(frame)
  }

  private flushProjectionBatch(): void {
    const timer = this.projectionFlushTimer
    if (timer !== undefined) {
      clearTimeout(timer)
      this.projectionFlushTimer = undefined
    }
    for (const frame of this.projectionBatch.drain()) this.broadcastNow(frame)
  }

  private broadcastNow(frame: SessionControlFrame): void {
    for (const stream of this.streams) stream.push(frame)
  }
}

type ControlQueueItem =
  | { readonly kind: 'frame'; readonly frame: SessionControlFrame }
  | { readonly kind: 'projections'; readonly segment: ProjectionAccumulator }

class ControlQueue {
  private readonly buffer = new Deque<ControlQueueItem>()
  /** Pending projection-only tail segment; a queue/jobs frame ends the segment. */
  private tailProjectionSegment: ProjectionAccumulator | undefined
  /** Bounded by the number of distinct projection keys in one drained segment. */
  private drainingProjections: readonly ProjectionFrame[] | undefined
  private drainingProjectionIndex = 0
  private wake: (() => void) | undefined
  private done = false

  push(frame: SessionControlFrame): void {
    if (this.done) return

    if (frame.type === 'projection') {
      let segment = this.tailProjectionSegment
      if (segment === undefined) {
        segment = new ProjectionAccumulator()
        this.tailProjectionSegment = segment
        this.buffer.pushBack({ kind: 'projections', segment })
      }
      segment.push(frame)
    } else {
      // A non-projection frame is an ordering barrier: later replacements may
      // not coalesce into the segment preceding it.
      this.tailProjectionSegment = undefined
      this.buffer.pushBack({ kind: 'frame', frame })
    }

    const wake = this.wake
    this.wake = undefined
    wake?.()
  }

  end(): void {
    if (this.done) return
    this.done = true
    const wake = this.wake
    this.wake = undefined
    wake?.()
  }

  async *iterate(signal: AbortSignal): AsyncIterable<SessionControlFrame> {
    const onAbort = (): void => { this.end() }
    signal.addEventListener('abort', onAbort, { once: true })
    try {
      while (!this.done && !signal.aborted) {
        const frame = this.shift()
        if (frame !== undefined) {
          yield frame
          continue
        }
        await new Promise<void>((resolve) => { this.wake = resolve })
      }
      while (this.hasPending() && !signal.aborted) {
        const frame = this.shift()
        if (frame !== undefined) yield frame
      }
    } finally {
      signal.removeEventListener('abort', onAbort)
      this.end()
    }
  }

  private hasPending(): boolean {
    return this.drainingProjections !== undefined || this.buffer.size > 0
  }

  private shift(): SessionControlFrame | undefined {
    const draining = this.drainingProjections
    if (draining !== undefined) {
      const frame = draining[this.drainingProjectionIndex]
      this.drainingProjectionIndex += 1
      if (this.drainingProjectionIndex >= draining.length) {
        this.drainingProjections = undefined
        this.drainingProjectionIndex = 0
      }
      if (frame !== undefined) return frame
    }

    while (this.buffer.size > 0) {
      const item = this.buffer.popFront() as ControlQueueItem
      if (item.kind === 'frame') return item.frame

      if (item.segment === this.tailProjectionSegment) this.tailProjectionSegment = undefined
      const frames = item.segment.drain()
      if (frames.length === 0) continue
      if (frames.length > 1) {
        this.drainingProjections = frames
        this.drainingProjectionIndex = 1
      }
      return frames[0]
    }
    return undefined
  }
}

function queueItems(
  agent: Agent,
  splice?: SessionEventMap['agent/inbox/spliced'],
): SessionQueuedItem[] {
  const project = (target: 'next-turn' | 'next-step'): readonly UserMessage[] => {
    const messages = target === 'next-turn' ? agent.inbox.nextTurn : agent.inbox.nextStep
    return splice?.target === target
      ? messages.toSpliced(splice.start, splice.removedCount ?? 0, ...splice.inserted)
      : messages
  }
  return [
    ...project('next-turn').map(message => ({
      id: message.id,
      placement: 'queued' as const,
      ...promptRpcId(message),
      message: { id: message.id, content: message.content as unknown as JsonValue[] },
    })),
    ...project('next-step').map(message => ({
      id: message.id,
      placement: message.source.kind === 'user' ? 'steering' as const : 'context' as const,
      ...promptRpcId(message),
      message: { id: message.id, content: message.content as unknown as JsonValue[] },
    })),
  ]
}

/** Prompt-RPC identity carried by a browser-submitted message's user source. */
function promptRpcId(message: UserMessage): Pick<SessionQueuedItem, 'rpcId'> {
  const source = message.source
  return source.kind === 'user' && 'rpcId' in source ? { rpcId: source.rpcId } : {}
}

function jobView(job: JobSnapshot): SessionJob {
  return {
    id: job.id,
    kind: job.kind,
    label: job.label,
    status: job.status,
    ...(job.detail === undefined ? {} : { detail: job.detail }),
    startedAt: job.startedAt,
    ...(job.finishedAt === undefined ? {} : { finishedAt: job.finishedAt }),
  }
}
