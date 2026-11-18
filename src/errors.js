/**
 * 所有 frame 都被 pin 或正在回写，replacer 找不到可淘汰的 victim 时抛出。
 */
export class EvictionError extends Error {
  constructor(pageId) {
    super(`no evictable frame for page ${pageId}: all frames pinned or flushing`);
    this.name = 'EvictionError';
    this.pageId = pageId;
  }
}
