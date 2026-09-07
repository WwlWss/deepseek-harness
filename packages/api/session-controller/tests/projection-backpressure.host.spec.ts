import { Context } from '@deepseek-ai/cordis'
import AgentRegistry, { Inbox } from '@deepseek-ai/dsh-agent'
import type { Agent } from '@deepseek-ai/dsh-agent'
import { createUserMessage } from '@deepseek-ai/dsh-llm'
import SessionStore, { SessionId } from '@deepseek-ai/dsh-session'
import SessionProjectionRegistry from '@deepseek-ai/dsh-session-projection'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { SessionControlController } from '../src/control.ts'
import type { SessionControlFrame } from '../src/types.ts'

async function harness(): Promise<SessionControlController> {
  const ctx = new Context()
  await ctx.plugin(SessionStore)
  await ctx.plugin(AgentRegistry)
  await ctx.plugin(SessionProjectionRegistry)
  const session = ctx.sessions.create(SessionId('queue-session'))
  const inbox = new Inbox(session, { inserted: () => {}, discarded: () => {}, claimed: () => {} })
  const agent = { id: session.id, session, inbox, status: 'running', ctx } as Agent
  ctx.agents.register(agent)
  return new SessionControlController(ctx)
}

function projectionFrame(sessionId: SessionId, key: string, seq: number): SessionControlFrame {
  return { type: 'projection', sessionId, key, value: { seq }, seq }
}

function queueFrame(sessionId: SessionId): SessionControlFrame {
  const message = createUserMessage({ content: [{ type: 'text', text: 'barrier' }], source: { kind: 'user' } })
  return {
    type: 'queue', sessionId,
    items: [{ id: message.id, placement: 'queued', message: { id: message.id, content: [] } }],
  }
}

function broadcast(control: SessionControlController, frame: SessionControlFrame): void {
  ;(control as unknown as { broadcast(frame: SessionControlFrame): void }).broadcast(frame)
}

function broadcastNow(control: SessionControlController, frame: SessionControlFrame): void {
  ;(control as unknown as { broadcastNow(frame: SessionControlFrame): void }).broadcastNow(frame)
}

async function nextFrame(iterator: AsyncIterator<SessionControlFrame>): Promise<SessionControlFrame> {
  const next = await iterator.next()
  if (next.done || next.value === undefined) throw new Error('missing control frame')
  return next.value
}

afterEach(() => {
  vi.useRealTimers()
})

describe('projection transport backpressure', () => {
  it('bounds a slow consumer to the newest same-key projection inside a queue segment', async () => {
    const control = await harness()
    const abort = new AbortController()
    const iterator = control.control(abort.signal)[Symbol.asyncIterator]()
    await iterator.next()
    const sessionId = SessionId('queue-session')

    for (let seq = 1; seq <= 100_000; seq += 1) {
      broadcastNow(control, projectionFrame(sessionId, 'subagentTiming', seq))
    }
    broadcastNow(control, queueFrame(sessionId))

    expect(await nextFrame(iterator)).toMatchObject({
      type: 'projection', sessionId, key: 'subagentTiming', seq: 100_000,
    })
    expect(await nextFrame(iterator)).toMatchObject({ type: 'queue', sessionId })

    abort.abort()
    await iterator.next()
  })

  it('rate-limits a fast consumer with the 16 ms host publication window', async () => {
    vi.useFakeTimers()
    const control = await harness()
    const abort = new AbortController()
    const iterator = control.control(abort.signal)[Symbol.asyncIterator]()
    await iterator.next()
    const sessionId = SessionId('queue-session')
    const waiting = iterator.next()

    for (let seq = 1; seq <= 100_000; seq += 1) {
      broadcast(control, projectionFrame(sessionId, 'subagentTiming', seq))
    }

    await vi.advanceTimersByTimeAsync(15)
    let settled = false
    void waiting.then(() => { settled = true })
    await Promise.resolve()
    expect(settled).toBe(false)

    await vi.advanceTimersByTimeAsync(1)
    await expect(waiting).resolves.toMatchObject({
      done: false,
      value: { type: 'projection', sessionId, key: 'subagentTiming', seq: 100_000 },
    })

    abort.abort()
    await iterator.next()
  })

  it('keeps last-occurrence order across keys and never crosses a queue barrier', async () => {
    const control = await harness()
    const abort = new AbortController()
    const iterator = control.control(abort.signal)[Symbol.asyncIterator]()
    await iterator.next()
    const sessionId = SessionId('queue-session')

    broadcastNow(control, projectionFrame(sessionId, 'a', 1))
    broadcastNow(control, projectionFrame(sessionId, 'b', 2))
    broadcastNow(control, projectionFrame(sessionId, 'a', 3))
    broadcastNow(control, queueFrame(sessionId))
    broadcastNow(control, projectionFrame(sessionId, 'a', 4))

    expect(await nextFrame(iterator)).toMatchObject({ type: 'projection', key: 'b', seq: 2 })
    expect(await nextFrame(iterator)).toMatchObject({ type: 'projection', key: 'a', seq: 3 })
    expect(await nextFrame(iterator)).toMatchObject({ type: 'queue' })
    expect(await nextFrame(iterator)).toMatchObject({ type: 'projection', key: 'a', seq: 4 })

    abort.abort()
    await iterator.next()
  })

  it('rejects stale or equal replacements while a same-key value is pending', async () => {
    const control = await harness()
    const abort = new AbortController()
    const iterator = control.control(abort.signal)[Symbol.asyncIterator]()
    await iterator.next()
    const sessionId = SessionId('queue-session')

    broadcastNow(control, projectionFrame(sessionId, 'subagentTiming', 10))
    broadcastNow(control, projectionFrame(sessionId, 'subagentTiming', 9))
    broadcastNow(control, projectionFrame(sessionId, 'subagentTiming', 10))
    broadcastNow(control, queueFrame(sessionId))

    expect(await nextFrame(iterator)).toMatchObject({ type: 'projection', seq: 10 })
    expect(await nextFrame(iterator)).toMatchObject({ type: 'queue' })

    abort.abort()
    await iterator.next()
  })
})
