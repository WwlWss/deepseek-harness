import { SessionSeq } from '@deepseek-ai/dsh-session/types'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { ProjectionValueStore } from '../src/client/sessions/projection-store.ts'

declare module '@deepseek-ai/dsh-session-projection/types' {
  interface SessionProjectionMap {
    'test/frame-marks': { mark: number }
  }
}

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('projection observer frame batching', () => {
  it('keeps rows synchronous while publishing high-frequency browser changes once per frame', () => {
    const frames: Array<(time: number) => void> = []
    vi.stubGlobal('requestAnimationFrame', (callback: (time: number) => void) => {
      frames.push(callback)
      return frames.length
    })

    const store = new ProjectionValueStore()
    let keyTicks = 0
    let anyTicks = 0
    store.faceOf('test/frame-marks').subscribe(() => { keyTicks += 1 })
    store.subscribeAny(() => { anyTicks += 1 })

    for (let seq = 1; seq <= 1_000; seq += 1) {
      store.apply('test/frame-marks', { mark: seq }, SessionSeq(seq))
    }

    expect(store.get('test/frame-marks')).toEqual({ mark: 1_000 })
    expect(keyTicks).toBe(0)
    expect(anyTicks).toBe(0)
    expect(frames).toHaveLength(2)

    for (const frame of frames.splice(0)) frame(0)
    expect(keyTicks).toBe(1)
    expect(anyTicks).toBe(1)
  })
})
