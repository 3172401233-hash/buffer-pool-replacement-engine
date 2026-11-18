import { sleep } from './concurrency.js';

/**
 * 内存版磁盘管理器，负责页的持久化存储与按页读取/写回。
 * 可注入延迟模拟真实异步 I/O，并统计物理读写次数。
 */
export class DiskManager {
  #pages = new Map();
  #latencyMs;
  reads = 0;
  writes = 0;

  constructor({ latencyMs = 0 } = {}) {
    this.#latencyMs = latencyMs;
  }

  async readPage(pageId) {
    this.reads += 1;
    if (this.#latencyMs > 0) await sleep(this.#latencyMs);
    const stored = this.#pages.get(pageId);
    // 深拷贝，确保调用者拿到的副本与磁盘上的数据互不影响。
    return structuredClone(stored ?? { id: pageId, data: {}, lsn: 0 });
  }

  async writePage(page) {
    this.writes += 1;
    if (this.#latencyMs > 0) await sleep(this.#latencyMs);
    this.#pages.set(page.id, structuredClone(page));
  }

  /** 直接写入磁盘（绕过缓冲池），用于测试初始化场景。 */
  seedPage(page) {
    this.#pages.set(page.id, structuredClone(page));
  }
}

/**
 * 顺序 WAL：每条记录按全局 LSN 追加。
 * append 串行执行，保证日志提交顺序先于任何数据页写回。
 */
export class WriteAheadLog {
  #records = [];
  #nextLsn = 1;
  #latencyMs;
  commits = 0;

  constructor({ latencyMs = 0 } = {}) {
    this.#latencyMs = latencyMs;
  }

  async append({ pageId, payload }) {
    const lsn = this.#nextLsn++;
    if (this.#latencyMs > 0) await sleep(this.#latencyMs);
    this.#records.push({ lsn, pageId, payload });
    this.commits += 1;
    return lsn;
  }

  get durableLsn() {
    return this.#records.length ? this.#records[this.#records.length - 1].lsn : 0;
  }

  get records() {
    return this.#records.map((record) => ({ ...record }));
  }
}
