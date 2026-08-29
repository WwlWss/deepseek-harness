import { afterEach, describe, expect, it, vi } from 'vitest'

const POINTER_SIZE = 8

interface FakePtr {
  kind: 'dialog' | 'item' | 'name' | 'vtable' | 'fn'
  owner?: FakePtr
  slot?: number
  text?: string
}

interface Harness {
  namePtr: FakePtr | null | undefined
  freed: unknown[]
  released: string[]
  string16: ReturnType<typeof vi.fn>
}

function installKoffi(path: string | null | undefined, decodeError?: Error): Harness {
  const dialogPtr: FakePtr = { kind: 'dialog' }
  const itemPtr: FakePtr = { kind: 'item' }
  const namePtr: FakePtr | null | undefined = typeof path === 'string'
    ? { kind: 'name', text: path }
    : path
  const freed: unknown[] = []
  const released: string[] = []
  const outBuffers = new Map<unknown, FakePtr>()

  const dispatch = (self: FakePtr, slot: number, args: unknown[]): number => {
    if (self.kind === 'dialog') {
      switch (slot) {
        case 20:
          ;(args[0] as unknown[])[0] = itemPtr
          return 0
        case 2:
          released.push('dialog')
          return 0
        default:
          return 0
      }
    }
    if (self.kind === 'item') {
      switch (slot) {
        case 5:
          ;(args[1] as unknown[])[0] = namePtr
          return 0
        case 2:
          released.push('item')
          return 0
        default:
          return 0
      }
    }
    throw new Error(`unexpected COM receiver: ${self.kind}`)
  }

  const string16 = vi.fn((ptr: unknown) => {
    expect(ptr).toBe(namePtr)
    if (decodeError !== undefined) throw decodeError
    return (ptr as FakePtr).text as string
  })

  const decode = Object.assign(
    (value: unknown, offsetOrType: unknown): unknown => {
      if (offsetOrType === 'void *') {
        if (outBuffers.has(value)) return outBuffers.get(value)
        if ((value as FakePtr).kind === 'dialog' || (value as FakePtr).kind === 'item') {
          return { kind: 'vtable', owner: value as FakePtr } satisfies FakePtr
        }
      }
      if (typeof offsetOrType === 'number') {
        const vtable = value as FakePtr
        if (vtable.kind !== 'vtable' || vtable.owner === undefined) {
          throw new Error('expected a vtable pointer with an owner')
        }
        return { kind: 'fn', owner: vtable.owner, slot: offsetOrType / POINTER_SIZE } satisfies FakePtr
      }
      throw new Error(`unexpected decode request: ${String(offsetOrType)}`)
    },
    { string16 },
  )

  vi.doMock('koffi', () => ({
    default: {
      load: (dll: string) => ({
        func: (_convention: string, name: string) => {
          switch (`${dll}/${name}`) {
            case 'ole32.dll/CoInitializeEx': return () => 0
            case 'ole32.dll/CoUninitialize': return () => undefined
            case 'ole32.dll/CoCreateInstance': return (...args: unknown[]) => {
              outBuffers.set(args[4], dialogPtr)
              return 0
            }
            case 'ole32.dll/CoTaskMemFree': return (ptr: unknown) => { freed.push(ptr) }
            case 'kernel32.dll/GetCurrentThreadId': return () => 1
            case 'user32.dll/SetThreadDpiAwarenessContext': return () => ({})
            default: return () => 1
          }
        },
      }),
      proto: (declaration: string) => declaration,
      pointer: (type: unknown) => type,
      sizeof: () => POINTER_SIZE,
      decode,
      call: (fn: FakePtr, _proto: unknown, self: FakePtr, ...args: unknown[]) => {
        return dispatch(self, fn.slot as number, args)
      },
      register: () => ({}),
      unregister: () => undefined,
    },
  }))

  return { namePtr, freed, released, string16 }
}

async function loadBindings() {
  return await import('../src/win32-dialog-bindings.ts')
}

afterEach(() => {
  vi.doUnmock('koffi')
  vi.resetModules()
})

describe('Win32 display-name decoding', () => {
  it('uses Koffi string16 decoding without constructing an external memory view', async () => {
    const path = 'C:\\fixture\\安卓开发\\emoji-😀\\' + '长'.repeat(300)
    const harness = installKoffi(path)
    const { loadWin32DialogBindings } = await loadBindings()
    const dialog = (await loadWin32DialogBindings()).createFolderDialog()

    expect(dialog.resultPath()).toEqual({ hr: 0, path })
    expect(harness.string16).toHaveBeenCalledOnce()
    expect(harness.freed).toEqual([harness.namePtr])
    expect(harness.released).toEqual(['item'])
  })

  it('frees the COM string and releases the shell item when decoding throws', async () => {
    const harness = installKoffi('C:\\unreadable', new Error('decode failed'))
    const { loadWin32DialogBindings } = await loadBindings()
    const dialog = (await loadWin32DialogBindings()).createFolderDialog()

    expect(() => dialog.resultPath()).toThrow('decode failed')
    expect(harness.freed).toEqual([harness.namePtr])
    expect(harness.released).toEqual(['item'])
  })

  it.each([
    ['null', null],
    ['undefined', undefined],
  ] as const)('rejects a successful GetDisplayName with a %s path pointer', async (_label, pointer) => {
    const harness = installKoffi(pointer)
    const { loadWin32DialogBindings } = await loadBindings()
    const dialog = (await loadWin32DialogBindings()).createFolderDialog()

    expect(() => dialog.resultPath()).toThrow('IShellItem::GetDisplayName succeeded with a null path pointer')
    expect(harness.string16).not.toHaveBeenCalled()
    expect(harness.freed).toEqual([])
    expect(harness.released).toEqual(['item'])
  })
})