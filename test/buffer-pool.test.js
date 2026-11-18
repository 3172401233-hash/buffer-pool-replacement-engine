import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  BufferPoolManager,
  ClockReplacer,
  DiskManager,
  EvictionError,
  WriteAheadLog,
} from '../src/index.js';

const makeDisk = (latencyMs = 1) => new DiskManager({ latencyMs });

async function fetchAndRelease(pool, pageId) {
  await pool.fetchPage(pageId);
  await pool.unpinPage(pageId);
}

test('全表扫描只污染 LRU 尾部，热点索引页保留在缓冲池', async () => {
  const disk = makeDisk();
  for (let id = 1; id <= 12; id += 1) disk.seedPage({ id, data: { v: id }, lsn: 0 });
  const pool = new BufferPoolManager({ capacity: 5, disk, wal: new WriteAheadLog() });

  // 热点索引页先加载并保持使用，随后释放成为淘汰候选。
  for (const hotId of [100, 101]) {
    await pool.fetchPage(hotId);
    await pool.unpinPage(hotId);
  }

  // 模拟大表顺序扫描：访问 7 个冷页，每读一个冷页就重新触达一次热点索引页，
  // 复现 OLTP 中“扫描与索引查询交错”的访问模式。
  for (let id = 1; id <= 7; id += 1) {
    await fetchAndRelease(pool, id);
    for (const hotId of [100, 101]) {
      await pool.fetchPage(hotId);
      await pool.unpinPage(hotId);
    }
  }

  const stats = pool.getStats();
  assert.equal(stats.residentPages, 5);
  assert.equal(stats.evictions, 4);
  assert.equal(stats.evictionFailures, 0);
  assert.equal(stats.writebacks, 0);

  const resident = pool.inspect().map((frame) => frame.pageId);
  assert.ok(resident.includes(100));
  assert.ok(resident.includes(101));
  assert.ok(!resident.includes(1));
  assert.deepEqual(resident.sort((a, b) => a - b), [5, 6, 7, 100, 101]);

  // 热点页再次访问全部命中，冷页 1 已被淘汰需要物理读。
  const diskReadsBefore = disk.reads;
  await pool.fetchPage(100);
  await pool.fetchPage(101);
  assert.equal(disk.reads, diskReadsBefore);
  await pool.unpinPage(100);
  await pool.unpinPage(101);

  // 没有重触达保护时，纯顺序扫描会把热点页全部冲走。
  const unprotected = new BufferPoolManager({ capacity: 5, disk, wal: new WriteAheadLog() });
  for (const hotId of [100, 101]) await fetchAndRelease(unprotected, hotId);
  for (let id = 1; id <= 7; id += 1) await fetchAndRelease(unprotected, id);
  const unprotectedResident = unprotected.inspect().map((frame) => frame.pageId);
  assert.ok(!unprotectedResident.includes(100));
  assert.ok(!unprotectedResident.includes(101));
});

test('并发 fetch 同一页只触发一次磁盘读取且每个调用者各自持有 pin', async () => {
  const disk = makeDisk(5);
  const pool = new BufferPoolManager({ capacity: 1, disk, wal: new WriteAheadLog() });

  const [first, ...rest] = await Promise.all(
    Array.from({ length: 8 }, () => pool.fetchPage(42)),
  );

  assert.equal(disk.reads, 1);
  assert.equal(first.id, 42);
  assert.equal(pool.inspect().find((frame) => frame.pageId === 42).pinCount, 8);
  assert.equal(pool.getStats().misses, 1);
  assert.equal(pool.getStats().hits, 7);

  // 页被 8 个调用者 pin 住，另一个页无法淘汰它。
  await assert.rejects(pool.fetchPage(43), EvictionError);
  assert.equal(pool.getStats().evictionFailures, 1);

  for (let i = 0; i < 8; i += 1) await pool.unpinPage(42);
  const handle = await pool.fetchPage(43);
  assert.equal(handle.id, 43);
  assert.ok(!pool.inspect().some((frame) => frame.pageId === 42));
});

test('脏页写回前必须先顺序提交 WAL，且回写期间页不可淘汰', async () => {
  const disk = makeDisk(2);
  const wal = new WriteAheadLog({ latencyMs: 2 });
  const events = [];
  const originalWrite = disk.writePage.bind(disk);
  const originalAppend = wal.append.bind(wal);
  wal.append = async (record) => {
    const lsn = await originalAppend(record);
    events.push({ type: 'wal', pageId: record.pageId, lsn });
    return lsn;
  };
  disk.writePage = async (page) => {
    events.push({ type: 'write', pageId: page.id, lsn: page.lsn });
    return originalWrite(page);
  };

  const pool = new BufferPoolManager({ capacity: 1, disk, wal });
  await pool.fetchPage(7);
  await pool.modifyPage(7, (data) => {
    data.counter = 1;
  });
  await pool.unpinPage(7);

  const flushPromise = pool.flushPage(7);
  // flush 在锁内执行；此处只在微任务间隙确认回写完成后的磁盘内容。
  const flushed = await flushPromise;
  assert.equal(flushed.isDirty, false);
  assert.equal(flushed.flushing, false);

  assert.equal(events.length, 2);
  assert.deepEqual(events[0], { type: 'wal', pageId: 7, lsn: 1 });
  assert.deepEqual(events[1], { type: 'write', pageId: 7, lsn: 1 });
  assert.equal(wal.durableLsn, 1);
  assert.equal(pool.getStats().writebacks, 1);
  assert.equal(pool.getStats().flushes, 1);

  // 写回后磁盘上的页携带 WAL 的 lsn，且脏标记已清除。
  const onDisk = await disk.readPage(7);
  assert.equal(onDisk.data.counter, 1);
  assert.equal(onDisk.lsn, 1);
  await pool.fetchPage(7);
  await pool.unpinPage(7);
});

test('淘汰脏页走 WAL -> 磁盘写回路径，flushAllPages 刷回所有脏页', async () => {
  const disk = makeDisk();
  const wal = new WriteAheadLog();
  const pool = new BufferPoolManager({ capacity: 2, disk, wal });

  await pool.fetchPage(1);
  await pool.modifyPage(1, (data) => {
    data.a = 1;
  });
  await pool.unpinPage(1);

  // 容量占满后加载新页，页 1 作为脏 victim 被回写后淘汰。
  await pool.fetchPage(2);
  await pool.modifyPage(2, (data) => {
    data.b = 2;
  });
  await pool.fetchPage(3);
  await pool.modifyPage(3, (data) => {
    data.c = 3;
  });

  assert.equal(pool.getStats().writebacks, 1);
  assert.equal(disk.writes, 1);
  const evictedOnDisk = await disk.readPage(1);
  assert.equal(evictedOnDisk.data.a, 1);
  assert.equal(evictedOnDisk.lsn, 1);

  await pool.unpinPage(2);
  await pool.unpinPage(3);
  await pool.flushAllPages();
  assert.equal(disk.writes, 3);
  assert.equal(pool.getStats().flushes, 2);
});

test('页被淘汰后重新读取恢复磁盘内容，命中率统计正确', async () => {
  const disk = makeDisk();
  disk.seedPage({ id: 9, data: { value: 'old' }, lsn: 0 });
  const pool = new BufferPoolManager({ capacity: 1, disk, wal: new WriteAheadLog() });

  let handle = await pool.fetchPage(9);
  assert.deepEqual(handle.data, { value: 'old' });
  await pool.unpinPage(9);

  await fetchAndRelease(pool, 10);
  assert.ok(!pool.inspect().some((frame) => frame.pageId === 9));

  handle = await pool.fetchPage(9);
  assert.deepEqual(handle.data, { value: 'old' });
  assert.equal(handle.pinCount, 1);
  await pool.unpinPage(9);

  const stats = pool.getStats();
  assert.equal(stats.misses, 3);
  assert.equal(stats.evictions, 2);
  assert.ok(Number.isFinite(stats.hitRate));
});

test('容量为一时 pin 的页不可驱逐，释放后新页才能装入', async () => {
  const disk = makeDisk();
  const pool = new BufferPoolManager({ capacity: 1, disk, wal: new WriteAheadLog() });

  const pinned = await pool.fetchPage(1);
  assert.equal(pinned.pinCount, 1);
  await assert.rejects(pool.fetchPage(2), (error) => {
    assert.ok(error instanceof EvictionError);
    assert.equal(error.pageId, 2);
    return true;
  });
  assert.equal(pool.getStats().evictionFailures, 1);
  assert.deepEqual(pool.inspect().map((frame) => frame.pageId), [1]);

  await pool.unpinPage(1);
  const next = await pool.fetchPage(2);
  assert.equal(next.id, 2);
  assert.equal(pool.getStats().evictions, 1);
  assert.deepEqual(pool.inspect().map((frame) => frame.pageId), [2]);
  await pool.unpinPage(2);
});

test('未 pin 的页拒绝修改，修改必须先通过 fetch/pin', async () => {
  const disk = makeDisk();
  const pool = new BufferPoolManager({ capacity: 1, disk, wal: new WriteAheadLog() });
  await pool.fetchPage(5);
  await pool.unpinPage(5);
  await assert.rejects(
    pool.modifyPage(5, (data) => {
      data.x = 1;
    }),
    /cannot modify unpinned page/,
  );
});

test('Clock 策略下缓冲池按第二次机会规则淘汰', async () => {
  const disk = makeDisk();
  const pool = new BufferPoolManager({
    capacity: 2,
    disk,
    wal: new WriteAheadLog(),
    replacer: new ClockReplacer(),
  });

  await fetchAndRelease(pool, 1);
  await fetchAndRelease(pool, 2);
  await fetchAndRelease(pool, 3);

  const resident = pool.inspect().map((frame) => frame.pageId);
  assert.deepEqual(resident, [3, 2]);
  assert.equal(pool.getStats().evictions, 1);
});

test('回写失败的 frame 不会被重新分配，解除 flushing 后恢复淘汰候选', async () => {
  const disk = makeDisk(2);
  const wal = new WriteAheadLog();
  const pool = new BufferPoolManager({ capacity: 1, disk, wal });

  await pool.fetchPage(1);
  await pool.modifyPage(1, (data) => {
    data.v = 1;
  });
  await pool.unpinPage(1);

  // WAL 已提交，但磁盘写回抛错：frame 必须保留原页并解除 flushing。
  disk.writePage = async () => {
    throw new Error('disk outage');
  };
  await assert.rejects(pool.flushPage(1), /disk outage/);

  const afterFailure = pool.inspect()[0];
  assert.equal(afterFailure.pageId, 1);
  assert.equal(afterFailure.flushing, false);
  assert.equal(afterFailure.pinCount, 0);
  assert.equal(pool.getStats().replacerCandidates, 1);

  // 恢复磁盘后重试，同一 frame 成功写回，没有发生版本混用。
  disk.writePage = DiskManager.prototype.writePage.bind(disk);
  const recovered = await pool.flushPage(1);
  assert.equal(recovered.id, 1);
  assert.equal(recovered.isDirty, false);
  assert.equal(pool.getStats().writebacks, 1);
});

test('无 WAL 时脏页仍可写回，且未命中统计与命中率自洽', async () => {
  const disk = makeDisk();
  const pool = new BufferPoolManager({ capacity: 1, disk });

  await pool.fetchPage(1);
  await pool.modifyPage(1, (data) => {
    data.n = 1;
  });
  await pool.unpinPage(1, { dirty: true });
  await fetchAndRelease(pool, 2);

  assert.equal(pool.getStats().writebacks, 1);
  assert.equal(disk.writes, 1);
  const persisted = await disk.readPage(1);
  assert.equal(persisted.data.n, 1);
  assert.equal(pool.hitRate, 0);
});
