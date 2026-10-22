import type {
  ChannelStorage,
  StorageListOptions,
  StorageTransaction,
} from "../../src/cloudflare/types";

/**
 * A test double for Durable Object storage: an ordered key-value map with the platform's semantics that the channel object
 * relies on (values are copied in and out like structured clones, `list` is ordered with prefix/start/end/limit/reverse, a
 * transaction that throws leaves nothing behind, at most 128 keys per batch, one alarm). It counts rows read and written and can
 * be told to fail, so the tests can check costs and error paths. It is a double of the platform, not of the logic under test;
 * the workerd smoke (`npm run smoke:realtime-cloudflare`) is the real check.
 */
export class MemoryStorage implements ChannelStorage {
  readonly #rows = new Map<string, unknown>();
  #alarm: number | null = null;
  readonly counters = { rowsRead: 0, rowsWritten: 0, rowsDeleted: 0, alarmsSet: 0, calls: 0 };
  /** Called with the new alarm time (or null) after each change. */
  onAlarm: ((at: number | null) => void) | undefined;
  /** Delay every call by this many timer ticks, so concurrent callers interleave. */
  yieldEvery = 0;
  #failures: {
    remaining: number;
    match: (operation: string, keys: readonly string[]) => boolean;
  }[] = [];

  /** Makes the next `times` calls whose operation (and keys) match fail. */
  failNext(
    times: number,
    match: (operation: string, keys: readonly string[]) => boolean = () => true,
  ): void {
    this.#failures.push({ remaining: times, match });
  }

  get alarmAt(): number | null {
    return this.#alarm;
  }

  /** A copy of everything stored, for assertions. */
  snapshot(): Map<string, unknown> {
    return new Map([...this.#rows].map(([key, value]) => [key, structuredClone(value)]));
  }

  keys(prefix = ""): string[] {
    return [...this.#rows.keys()].filter((key) => key.startsWith(prefix)).sort();
  }

  async #enter(operation: string, keys: readonly string[]): Promise<void> {
    this.counters.calls += 1;
    for (let at = 0; at < this.yieldEvery; at += 1) await Promise.resolve();
    if (this.yieldEvery > 0) await new Promise((resolve) => setTimeout(resolve, 0));
    await Promise.resolve();
    for (const failure of this.#failures)
      if (failure.remaining > 0 && failure.match(operation, keys)) {
        failure.remaining -= 1;
        throw new Error(`injected storage failure in ${operation}`);
      }
  }

  async get<T>(key: string): Promise<T | undefined>;
  async get<T>(keys: readonly string[]): Promise<Map<string, T>>;
  async get<T>(keys: string | readonly string[]): Promise<T | undefined | Map<string, T>> {
    if (typeof keys === "string") {
      await this.#enter("get", [keys]);
      if (!this.#rows.has(keys)) return undefined;
      this.counters.rowsRead += 1;
      return structuredClone(this.#rows.get(keys)) as T;
    }
    if (keys.length > 128) throw new Error("get: at most 128 keys");
    await this.#enter("get", keys);
    const out = new Map<string, T>();
    for (const key of [...keys].sort())
      if (this.#rows.has(key)) {
        this.counters.rowsRead += 1;
        out.set(key, structuredClone(this.#rows.get(key)) as T);
      }
    return out;
  }

  async put(entries: Readonly<Record<string, unknown>>): Promise<void> {
    const keys = Object.keys(entries);
    if (keys.length > 128) throw new Error("put: at most 128 entries");
    await this.#enter("put", keys);
    for (const key of keys) {
      this.#rows.set(key, structuredClone(entries[key]));
      this.counters.rowsWritten += 1;
    }
  }

  async delete(keys: readonly string[]): Promise<number> {
    if (keys.length > 128) throw new Error("delete: at most 128 keys");
    await this.#enter("delete", keys);
    let deleted = 0;
    for (const key of keys)
      if (this.#rows.delete(key)) {
        deleted += 1;
        this.counters.rowsDeleted += 1;
      }
    return deleted;
  }

  async list<T>(options: StorageListOptions = {}): Promise<Map<string, T>> {
    await this.#enter("list", []);
    let keys = [...this.#rows.keys()].sort();
    if (options.prefix !== undefined) {
      const { prefix } = options;
      keys = keys.filter((key) => key.startsWith(prefix));
    }
    if (options.start !== undefined) {
      const { start } = options;
      keys = keys.filter((key) => key >= start);
    }
    if (options.end !== undefined) {
      const { end } = options;
      keys = keys.filter((key) => key < end);
    }
    if (options.reverse === true) keys.reverse();
    if (options.limit !== undefined) keys = keys.slice(0, options.limit);
    const out = new Map<string, T>();
    for (const key of keys) {
      this.counters.rowsRead += 1;
      out.set(key, structuredClone(this.#rows.get(key)) as T);
    }
    return out;
  }

  async transaction<T>(closure: (transaction: StorageTransaction) => Promise<T>): Promise<T> {
    await this.#enter("transaction", []);
    const before = new Map(this.#rows);
    const counters = { ...this.counters };
    const transaction: StorageTransaction = {
      put: async (entries) => {
        await this.put(entries);
      },
      delete: (keys) => this.delete(keys),
    };
    try {
      return await closure(transaction);
    } catch (error) {
      this.#rows.clear();
      for (const [key, value] of before) this.#rows.set(key, value);
      Object.assign(this.counters, counters, { calls: this.counters.calls });
      throw error;
    }
  }

  async getAlarm(): Promise<number | null> {
    await this.#enter("getAlarm", []);
    return this.#alarm;
  }

  async setAlarm(scheduledTime: number): Promise<void> {
    await this.#enter("setAlarm", []);
    this.#alarm = scheduledTime;
    this.counters.alarmsSet += 1;
    this.onAlarm?.(scheduledTime);
  }

  async deleteAlarm(): Promise<void> {
    await this.#enter("deleteAlarm", []);
    this.#alarm = null;
    this.onAlarm?.(null);
  }

  async deleteAll(): Promise<void> {
    await this.#enter("deleteAll", []);
    this.#rows.clear();
    this.#alarm = null;
    this.onAlarm?.(null);
  }

  /** The runtime clears the alarm before it calls `alarm()`. */
  consumeAlarm(): void {
    this.#alarm = null;
  }
}
