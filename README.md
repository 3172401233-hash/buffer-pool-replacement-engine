# buffer-pool-replacement-engine

JavaScript 数据库缓冲池管理库，处理页淘汰、pin 计数、脏页回写和并发读取。
支持可替换的 LRU / Clock 替换策略，并保证被 pin 或正在回写的页不会被驱逐、
脏页在写回磁盘前必须先完成 WAL 顺序提交。

## 目录结构

```
src/
  buffer-pool.js  缓冲池核心：fetch/pin/unpin/modify/flush 与统计
  replacers.js    LruReplacer 与 ClockReplacer（统一 victim/pin/unpin 接口）
  disk.js         内存版 DiskManager 与顺序 WriteAheadLog
  concurrency.js  Mutex（临界区串行化）
  errors.js       EvictionError
  index.js        统一导出
test/
  buffer-pool.test.js  缓冲池行为测试（14 个场景）
  replacers.test.js    替换策略单元测试
```

## 快速开始

```js
import {
  BufferPoolManager,
  ClockReplacer,
  DiskManager,
  WriteAheadLog,
} from './src/index.js';

const disk = new DiskManager({ latencyMs: 1 });
const wal = new WriteAheadLog({ latencyMs: 1 });
const pool = new BufferPoolManager({
  capacity: 16,
  disk,
  wal,
  replacer: new ClockReplacer(), // 不传时默认使用 LruReplacer
});

const page = await pool.fetchPage(42);      // 未命中 -> 磁盘读入并 pin
await pool.modifyPage(42, (data) => {       // 先追加 WAL，再标记脏
  data.title = 'hello';
});
await pool.unpinPage(42);                   // pinCount 归零，重新成为淘汰候选
await pool.flushPage(42);                   // 显式回写（WAL 已持久化）
console.log(pool.getStats());
```

## Pin 生命周期

每个 frame 维护 `pinCount`，表示当前持有该页的调用者数量：

1. `fetchPage(pageId)`：页在池中则命中并 `pinCount + 1`；未命中则选出 victim、
   必要时回写脏页，再从磁盘读入，新页初始 `pinCount = 1`。
2. `pinPage(pageId)`：对已驻留的页手动加一次 pin，不影响命中/未命中统计。
3. `unpinPage(pageId, { dirty })`：`pinCount - 1`；可顺带把页标记为脏。
   `pinCount` 归零且不在回写中时，页才进入 replacer 成为淘汰候选。
4. 只要 `pinCount > 0`，页就不会出现在 replacer 中，因此绝不可能被选为 victim。
5. 页必须处于 pin 状态才能调用 `modifyPage`，避免对随时可能被淘汰的页做就地修改。

并发读取同一页时走 single-flight：只有第一次 `fetchPage` 触发磁盘 I/O，
其余并发调用等待同一个读取 Promise，完成后各自加一次 pin（统计为命中）。

## 页驱逐与回写保护

- 缓冲池满且没有空闲 frame 时，replacer 的 `victim()` 选出一个候选页。
- 被 pin（`pinCount > 0`）或正在回写（`flushing === true`）的页不在 replacer 中，
  因此不会被驱逐。
- 没有任何可淘汰 frame 时抛出 `EvictionError`，并累加 `evictionFailures`。
- victim 是脏页时，先在锁内把 `flushing` 置为 `true` 再执行异步写回；
  写回完成前该 frame 不会被重新分配，从根上杜绝“回写途中版本混用”。
  写回失败时 frame 保留原页、清除 `flushing` 并恢复候选资格，可稍后重试。
- `flushPage` 显式回写期间同样设置 `flushing` 保护，回写结束后按 pin 状态决定
  是否重新进入 replacer。

## WAL 先写规则

`modifyPage` 的顺序固定为：

1. 在元数据锁内执行同步 `mutator` 修改页数据；
2. 调用 `wal.append(...)` 顺序提交日志，拿到全局递增的 LSN；
3. 把 LSN 写到页上并标记脏页。

任何脏页写回（淘汰路径或 `flushPage`）前都会检查
`page.lsn <= wal.durableLsn`，不满足则抛错而不是把未落日志的数据写回磁盘。
未注入 WAL 时该守卫跳过，缓冲池仍可独立运行。

## 替换策略

两种策略实现同一接口（`victim / pin / unpin / remove / size`），构造时注入即可互换：

- `LruReplacer`：基于 `Map` 插入顺序维护最近使用次序，`unpin` 会把页刷新到
  “最近使用”一端，`victim` 取最久未使用的页。
- `ClockReplacer`：环形指针 + ref 位的第二次机会算法。第一圈扫描把 ref=1 的页
  清零并跳过，第二圈才淘汰首个 ref=0 的页；每次 `unpin` 把 ref 置 1。

## 统计指标

`pool.getStats()` 返回：

| 指标 | 含义 |
| --- | --- |
| `hits` / `misses` | 命中 / 未命中次数（single-flight 等待者计为命中） |
| `hitRate` | `hits / (hits + misses)`，无访问时为 0 |
| `evictions` | 成功淘汰的页数 |
| `evictionFailures` | 无可用 frame（全部 pin 或回写中）的次数 |
| `writebacks` | 脏页写回磁盘的次数 |
| `flushes` | 显式 `flushPage` 成功次数 |
| `residentPages` | 当前驻留页数 |
| `replacerCandidates` | 当前可淘汰候选数 |

`pool.inspect()` 输出每个 frame 的 `pageId / pinCount / dirty / flushing / lsn`，
便于测试与排查。

## 测试

```bash
node --test
# 或
npm test
```

覆盖场景：

- 全表扫描污染（含热点重触达保护与无保护对照组）
- 并发 `fetchPage` 的 single-flight 与多调用者 pin
- 被 pin 页驱逐失败（`EvictionError` 与 `evictionFailures`）
- 脏页 WAL 顺序提交、回写事件顺序与回写失败恢复
- 淘汰脏页、`flushAllPages` 与页淘汰后重读
- 容量为一的边界行为，以及 LRU / Clock 两种替换策略
