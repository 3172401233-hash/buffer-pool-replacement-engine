import { Mutex } from './concurrency.js';
import { EvictionError } from './errors.js';
import { LruReplacer } from './replacers.js';

/**
 * 页句柄：对外暴露页内容与元数据，不直接暴露内部 frame，
 * 调用者通过 buffer pool 的 pin/unpin/modify/flush 操作改变状态。
 */
class PageHandle {
  constructor(frame) {
    this.id = frame.page.id;
    this.data = frame.page.data;
    this.lsn = frame.page.lsn;
    this.isDirty = frame.dirty;
    this.pinCount = frame.pinCount;
    this.flushing = frame.flushing;
  }
}

/**
 * 缓冲池管理器。
 *
 * 不变量：
 * 1. pinCount > 0（被 pin）或 flushing === true（正在回写）的页绝不会进入 replacer，
 *    因此不可能被选为 victim，杜绝“回写途中 frame 被重新分配”的版本混用。
 * 2. 同一页的并发 fetch 只有一次磁盘读取（single-flight），其余调用者等待并命中。
 * 3. 脏页写回前，对应 WAL 记录必须已顺序提交（页 lsn <= WAL durableLsn）。
 */
export class BufferPoolManager {
  #capacity;
  #disk;
  #wal;
  #replacer;
  #metaLock = new Mutex();
  #frames;
  #pageTable = new Map();
  #inflight = new Map();
  #stats = {
    hits: 0,
    misses: 0,
    evictions: 0,
    evictionFailures: 0,
    writebacks: 0,
    flushes: 0,
  };

  constructor({ capacity, disk, wal, replacer }) {
    if (!Number.isInteger(capacity) || capacity <= 0) {
      throw new RangeError('capacity must be a positive integer');
    }
    if (!disk || typeof disk.readPage !== 'function' || typeof disk.writePage !== 'function') {
      throw new TypeError('disk must implement readPage() and writePage()');
    }
    this.#capacity = capacity;
    this.#disk = disk;
    this.#wal = wal ?? null;
    this.#replacer = replacer ?? new LruReplacer();
    this.#frames = Array.from({ length: capacity }, () => null);
  }

  get capacity() {
    return this.#capacity;
  }

  /**
   * 读取页：命中直接 pin；未命中则淘汰 victim（必要时先回写）后从磁盘读入。
   * 并发读取同一页时只产生一次物理 I/O。
   */
  async fetchPage(pageId) {
    return this.#metaLock.runExclusive(async () => {
      const frameIndex = this.#pageTable.get(pageId);
      if (frameIndex !== undefined) {
        this.#stats.hits += 1;
        return this.#pinResident(frameIndex);
      }
      const pending = this.#inflight.get(pageId);
      if (pending) {
        await pending;
        this.#stats.hits += 1;
        return this.#pinResident(this.#pageTable.get(pageId));
      }
      this.#stats.misses += 1;
      const loading = this.#loadIntoFrame(pageId);
      this.#inflight.set(pageId, loading);
      try {
        return await loading;
      } finally {
        this.#inflight.delete(pageId);
      }
    });
  }

  /** 未命中路径：找 frame、淘汰旧页、磁盘读取、建立映射并 pin。 */
  async #loadIntoFrame(pageId) {
    const frameIndex = this.#acquireFrame(pageId);
    let page;
    try {
      page = await this.#disk.readPage(pageId);
    } catch (error) {
      // 读取失败：frame 已被清空，归还为空 frame 供后续使用。
      this.#frames[frameIndex] = null;
      throw error;
    }
    this.#frames[frameIndex] = {
      page,
      pinCount: 1,
      dirty: false,
      flushing: false,
    };
    this.#pageTable.set(pageId, frameIndex);
    return this.#toHandle(frameIndex);
  }

  /** 返回一个空闲 frame；缓冲池满时通过 replacer 淘汰 victim。 */
  #acquireFrame(pageId) {
    const freeIndex = this.#frames.findIndex((frame) => frame === null);
    if (freeIndex !== -1) return freeIndex;

    const victimId = this.#replacer.victim();
    if (victimId === undefined) {
      this.#stats.evictionFailures += 1;
      throw new EvictionError(pageId);
    }
    const victimIndex = this.#pageTable.get(victimId);
    const victim = this.#frames[victimIndex];
    this.#replacer.remove(victimId);

    if (victim.dirty) {
      // 同步标记 flushing：即便 I/O 被 await，其他调用者也拿不到这个 frame。
      victim.flushing = true;
      this.#writebackFrame(victimIndex);
    }
    this.#stats.evictions += 1;
    this.#pageTable.delete(victimId);
    this.#frames[victimIndex] = null;
    return victimIndex;
  }

  /** 脏页回写：先确保 WAL 已顺序提交到页的 lsn，再写数据页。 */
  async #writebackFrame(frameIndex) {
    const frame = this.#frames[frameIndex];
    if (!frame || !frame.dirty) return;
    if (this.#wal && frame.page.lsn > this.#wal.durableLsn) {
      throw new Error(
        `WAL gap: page ${frame.page.id} lsn ${frame.page.lsn} exceeds durable lsn ${this.#wal.durableLsn}`,
      );
    }
    await this.#disk.writePage(frame.page);
    frame.dirty = false;
    frame.flushing = false;
    this.#stats.writebacks += 1;
  }

  #pinResident(frameIndex) {
    const frame = this.#frames[frameIndex];
    frame.pinCount += 1;
    this.#replacer.pin(frame.page.id);
    return this.#toHandle(frameIndex);
  }

  /** 对已在缓冲池中的页手动加 pin（不改变命中/未命中统计）。 */
  async pinPage(pageId) {
    return this.#metaLock.runExclusive(() => {
      const frameIndex = this.#pageTable.get(pageId);
      if (frameIndex === undefined) {
        throw new Error(`page ${pageId} is not resident in the buffer pool`);
      }
      return this.#pinResident(frameIndex);
    });
  }

  /**
   * 解除一次 pin。pinCount 归零后页重新进入 replacer 成为淘汰候选；
   * 正在回写的页要等回写结束后才会重新成为候选。
   */
  async unpinPage(pageId, { dirty = false } = {}) {
    return this.#metaLock.runExclusive(() => {
      const frameIndex = this.#pageTable.get(pageId);
      if (frameIndex === undefined) {
        throw new Error(`page ${pageId} is not resident in the buffer pool`);
      }
      const frame = this.#frames[frameIndex];
      if (frame.pinCount <= 0) {
        throw new Error(`page ${pageId} pinCount is already zero`);
      }
      if (dirty) frame.dirty = true;
      frame.pinCount -= 1;
      if (frame.pinCount === 0 && !frame.flushing) {
        this.#replacer.unpin(pageId);
      }
      return this.#toHandle(frameIndex);
    });
  }

  /**
   * 修改已 pin 的页。mutator 在锁内同步执行并返回 WAL 负载；
   * WAL 先顺序提交拿到 lsn，再把 lsn 写到页上并标记脏。
   */
  async modifyPage(pageId, mutator, walPayload) {
    return this.#metaLock.runExclusive(async () => {
      const frameIndex = this.#pageTable.get(pageId);
      if (frameIndex === undefined) {
        throw new Error(`page ${pageId} is not resident in the buffer pool`);
      }
      const frame = this.#frames[frameIndex];
      if (frame.pinCount <= 0) {
        throw new Error(`cannot modify unpinned page ${pageId}; fetch/pin it first`);
      }
      if (frame.flushing) {
        throw new Error(`cannot modify page ${pageId} while it is being flushed`);
      }
      const payload = mutator ? mutator(frame.page.data) : walPayload;
      let lsn = frame.page.lsn;
      if (this.#wal) {
        lsn = await this.#wal.append({ pageId, payload });
      }
      frame.page.lsn = lsn;
      frame.dirty = true;
      return this.#toHandle(frameIndex);
    });
  }

  /** 显式刷脏页：回写期间该页保持不可淘汰，刷完后按 pin 状态恢复候选资格。 */
  async flushPage(pageId) {
    return this.#metaLock.runExclusive(async () => {
      const frameIndex = this.#pageTable.get(pageId);
      if (frameIndex === undefined) {
        throw new Error(`page ${pageId} is not resident in the buffer pool`);
      }
      const frame = this.#frames[frameIndex];
      if (!frame.dirty) return this.#toHandle(frameIndex);
      this.#replacer.pin(pageId);
      frame.flushing = true;
      try {
        await this.#writebackFrame(frameIndex);
        this.#stats.flushes += 1;
      } catch (error) {
        frame.flushing = false;
        if (frame.pinCount === 0) this.#replacer.unpin(pageId);
        throw error;
      }
      if (frame.pinCount === 0) this.#replacer.unpin(pageId);
      return this.#toHandle(frameIndex);
    });
  }

  /** 依次刷回所有脏页，用于检查点/关闭场景。 */
  async flushAllPages() {
    const pageIds = [...this.#pageTable.keys()];
    for (const pageId of pageIds) {
      await this.flushPage(pageId);
    }
  }

  #toHandle(frameIndex) {
    return new PageHandle(this.#frames[frameIndex]);
  }

  /** 命中率：hits / (hits + misses)，无访问时返回 0。 */
  get hitRate() {
    const total = this.#stats.hits + this.#stats.misses;
    return total === 0 ? 0 : this.#stats.hits / total;
  }

  getStats() {
    return {
      ...this.#stats,
      hitRate: this.hitRate,
      residentPages: this.#pageTable.size,
      replacerCandidates: this.#replacer.size,
      inflightReads: this.#inflight.size,
    };
  }

  /** 调试视图：当前每个 frame 承载的页及其 pin/脏/回写状态。 */
  inspect() {
    return this.#frames.map((frame, index) =>
      frame
        ? {
            frame: index,
            pageId: frame.page.id,
            pinCount: frame.pinCount,
            dirty: frame.dirty,
            flushing: frame.flushing,
            lsn: frame.page.lsn,
          }
        : { frame: index, pageId: null },
    );
  }
}
