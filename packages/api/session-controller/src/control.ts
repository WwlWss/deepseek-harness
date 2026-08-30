/** Live Session queue, jobs, and projection state with reconnect baselines. */

import type { Context } from '@deepseek-ai/cordis'
import type { Agent } from '@deepseek-ai/dsh-agent'
import type { JobSnapshot } from '@deepseek-ai/dsh-jobs'
import type {
  JsonValue, Session, SessionEvent, SessionEventMap, SessionId, UserMessage,
} from '@deepseek-ai/dsh-session'
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

interface ProjectionBatchNode {
  frame: ProjectionFrame
  previous?: ProjectionBatchNode | undefined
  next?: ProjectionBatchNode | undefined
}

/**
 * Host-side projection publication batch. Projection frames are whole-value,
 * higher-seq-wins replacements, so only the newest pending value per
 * (sessionId, key) needs to cross the transport during one short batch window.
 */
class ProjectionBatch {
  private head: ProjectionBatchNode | undefined
  private tail: ProjectionBatchNode | undefined
  private readonly pending = new Map<SessionId, Map<string, ProjectionBatchNode>>()

  push(frame: ProjectionFrame): boolean {
    let byKey = this.pending.get(frame.sessionId)
    const previous = byKey?.get(frame.key)
    if (previous !== undefined) {
      if (frame.seq <= previous.frame.seq) return false
      this.unlink(previous)
    }

    const node: ProjectionBatchNode = { frame, previous: this.tail }
    if (this.tail === undefined) this.head = node
    else this.tail.next = node
    this.tail = node

    if (byKey === undefined) {
      byKey = new Map()
      this.pending.set(frame.sessionId, byKey)
    }
    byKey.set(frame.key, node)
    return true
  }

  shift(): ProjectionFrame | undefined {
    const node = this.head
    if (node === undefined) return undefined
    this.unlink(node)

    const { frame } = node
    const byKey = this.pending.get(frame.sessionId)
    if (byKey?.get(frame.key) === node) {
      byKey.delete(frame.key)
      if (byKey.size === 0) this.pending.delete(frame.sessionId)
    }
    return frame
  }

  private unlink(node: ProjectionBatchNode): void {
    const { previous, next } = node
    if (previous === undefined) this.head = next
    else previous.next = next
    if (next === undefined) this.tail = previous
    else next.previous = previous
    node.previous = undefined
    node.next = undefined
  }
}

/** Owns the Host-wide Session control stream. */
export class SessionControlController {
  private readonly streams = new Set<ControlQueue>()
  private readonly projectionBatch = new ProjectionBatch()
  private projectionFlushTimer: ReturnType<typeof setTimeout> | undefined

  /** @param ctx - Host context carrying live Agent, projection, and jobs services. */
  constructor(private readonly ctx: Context) {
    ctx.on('session/event', (session, event) => { this.onSessionEvent(session, event) })
    ctx.inject(['sessionProjections'], (projectionCtx) => {
      projectionCtx.sessionProjections.onChanged((session, key, value, seq) => {
        this.broadcast({
          type: 'projection',
          sessionId: session.id,
          key,
          value: value as JsonValue,
          seq,
        })
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
      // Preserve graceful teardown: replacements already admitted before
      // disposal are published before every stream is ended.
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
    // Do not let a newly opened stream inherit pre-opening pending frames: its
    // baseline already materializes the registry's current projection cut.
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
    const registry = this.ctx.get('sessionProjections')
    const blocks = Object.create(null) as Record<SessionId, SessionProjectionBaseline>
    for (const session of sessions) {
      const snapshot = registry?.snapshot(session)
      blocks[session.id] = snapshot === undefined
        ? { asOfSeq: session.seq - 1, values: {} }
        : {
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
      // With no live consumer the next control baseline is authoritative, so
      // retaining or timing a transport-only replacement would be wasted work.
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

    // queue/jobs frames are ordering barriers. Flush every earlier projection
    // replacement before broadcasting the barrier itself.
    this.flushProjectionBatch()
    this.broadcastNow(frame)
  }

  private flushProjectionBatch(): void {
    const timer = this.projectionFlushTimer
    if (timer !== undefined) {
      clearTimeout(timer)
      this.projectionFlushTimer = undefined
    }
    while (true) {
      const frame = this.projectionBatch.shift()
      if (frame === undefined) return
      this.broadcastNow(frame)
    }
  }

  private broadcastNow(frame: SessionControlFrame): void {
    for (const stream of this.streams) stream.push(frame)
  }
}

interface ControlQueueNode {
  frame: SessionControlFrame
  previous?: ControlQueueNode | undefined
  next?: ControlQueueNode | undefined
}

class ControlQueue {
  private head: ControlQueueNode | undefined
  private tail: ControlQueueNode | undefined
  /** Latest pending node per projection inside the projection-only tail segment. */
  private readonly pendingProjections = new Map<SessionId, Map<string, ControlQueueNode>>()
  private wake: (() => void) | undefined
  private done = false

  push(frame: SessionControlFrame): void {
    if (this.done) return

    if (frame.type === 'projection') {
      let byKey = this.pendingProjections.get(frame.sessionId)
      const previous = byKey?.get(frame.key)
      if (previous !== undefined) {
        const previousFrame = previous.frame as ProjectionFrame
        if (frame.seq <= previousFrame.seq) return
        this.unlink(previous)
      }
      const node = this.append(frame)
      if (byKey === undefined) {
        byKey = new Map()
        this.pendingProjections.set(frame.sessionId, byKey)
      }
      byKey.set(frame.key, node)
    } else {
      // A non-projection frame is an ordering barrier. Never coalesce a later
      // projection across queue/jobs traffic that the consumer has not seen.
      this.pendingProjections.clear()
      this.append(frame)
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
      while (this.head !== undefined && !signal.aborted) {
        const frame = this.shift()
        if (frame !== undefined) yield frame
      }
    } finally {
      signal.removeEventListener('abort', onAbort)
      this.end()
    }
  }

  private append(frame: SessionControlFrame): ControlQueueNode {
    const node: ControlQueueNode = { frame, previous: this.tail }
    if (this.tail === undefined) this.head = node
    else this.tail.next = node
    this.tail = node
    return node
  }

  private shift(): SessionControlFrame | undefined {
    const node = this.head
    if (node === undefined) return undefined
    this.unlink(node)

    const frame = node.frame
    if (frame.type === 'projection') {
      const byKey = this.pendingProjections.get(frame.sessionId)
      if (byKey?.get(frame.key) === node) {
        byKey.delete(frame.key)
        if (byKey.size === 0) this.pendingProjections.delete(frame.sessionId)
      }
    }
    return frame
  }

  private unlink(node: ControlQueueNode): void {
    const { previous, next } = node
    if (previous === undefined) this.head = next
    else previous.next = next
    if (next === undefined) this.tail = previous
    else next.previous = previous
    node.previous = undefined
    node.next = undefined
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
