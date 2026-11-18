/**
 * 轻量互斥锁：把临界区串行化，保证 await I/O 期间帧状态不会被其他调用者修改。
 * 单进程内不使用原子量，等待队列即可保证互斥。
 */
export class Mutex {
  #locked = false;
  #waiters = [];

  async acquire() {
    if (!this.#locked) {
      this.#locked = true;
      return;
    }
    await new Promise((resolve) => this.#waiters.push(resolve));
    // 被前一个持有者唤醒时锁已经转移过来，无需再次抢占。
  }

  release() {
    const next = this.#waiters.shift();
    if (next) {
      next();
    } else {
      this.#locked = false;
    }
  }

  /** 在同一把锁保护下运行一个异步临界区，异常时也会正确释放。 */
  async runExclusive(fn) {
    await this.acquire();
    try {
      return await fn();
    } finally {
      this.release();
    }
  }
}

export const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
