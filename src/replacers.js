/**
 * 替换策略统一接口：
 * - victim()  选出一个可淘汰的页 id，没有候选时返回 undefined
 * - pin(id)   页被固定，从候选集合移除
 * - unpin(id) 页解除固定，重新成为淘汰候选
 * - remove(id) 物理移除（frame 被重新分配时）
 * - get size() 当前候选数量
 * 缓冲池保证：被 pin（pinCount > 0）或正在回写（flushing）的页不会进入 replacer。
 */

/**
 * LRU：按最近使用顺序维护候选，Map 的迭代顺序即插入顺序。
 * 重新 unpin 相当于刷新一次最近使用时间（移动到末尾）。
 */
export class LruReplacer {
  #candidates = new Map();

  victim() {
    const oldest = this.#candidates.keys().next().value;
    if (oldest !== undefined) this.#candidates.delete(oldest);
    return oldest;
  }

  pin(pageId) {
    this.#candidates.delete(pageId);
  }

  unpin(pageId) {
    if (this.#candidates.has(pageId)) this.#candidates.delete(pageId);
    this.#candidates.set(pageId, true);
  }

  remove(pageId) {
    this.#candidates.delete(pageId);
  }

  get size() {
    return this.#candidates.size;
  }
}

/**
 * Clock（第二次机会）：环形扫描候选页，ref 位为 1 则清零并跳过，
 * 为 0 则选为 victim。每次 unpin 把 ref 位置 1。
 */
export class ClockReplacer {
  #refBits = new Map();
  #hand = 0;

  victim() {
    if (this.#refBits.size === 0) return undefined;
    // 最多扫描两圈：第一圈把 ref=1 清零（第二次机会），第二圈淘汰首个 ref=0 的页。
    for (let attempt = 0; attempt < 2; attempt += 1) {
      const ids = [...this.#refBits.keys()];
      this.#hand %= ids.length;
      for (let steps = 0; steps < ids.length; steps += 1) {
        const id = ids[this.#hand];
        const referenced = this.#refBits.get(id);
        if (referenced === 0) {
          this.#refBits.delete(id);
          // 保持指针指向被删位置的下一个元素。
          if (this.#refBits.size > 0) {
            this.#hand %= this.#refBits.size;
          } else {
            this.#hand = 0;
          }
          return id;
        }
        this.#refBits.set(id, 0);
        this.#hand = (this.#hand + 1) % ids.length;
      }
    }
    return undefined;
  }

  pin(pageId) {
    this.#refBits.delete(pageId);
  }

  unpin(pageId) {
    this.#refBits.set(pageId, 1);
  }

  remove(pageId) {
    this.#refBits.delete(pageId);
  }

  get size() {
    return this.#refBits.size;
  }
}
