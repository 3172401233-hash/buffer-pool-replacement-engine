import { test } from 'node:test';
import assert from 'node:assert/strict';
import { ClockReplacer, LruReplacer } from '../src/replacers.js';

test('LruReplacer 按最久未使用顺序选择 victim', () => {
  const replacer = new LruReplacer();
  replacer.unpin(1);
  replacer.unpin(2);
  replacer.unpin(3);
  assert.equal(replacer.size, 3);

  assert.equal(replacer.victim(), 1);
  assert.equal(replacer.victim(), 2);

  replacer.pin(3);
  assert.equal(replacer.size, 0);
  assert.equal(replacer.victim(), undefined);

  replacer.unpin(4);
  replacer.remove(4);
  assert.equal(replacer.victim(), undefined);
});

test('LruReplacer 重新 unpin 会刷新最近使用位置', () => {
  const replacer = new LruReplacer();
  replacer.unpin(1);
  replacer.unpin(2);
  replacer.unpin(1);
  assert.deepEqual([replacer.victim(), replacer.victim()], [2, 1]);
});

test('ClockReplacer 给带 ref 位的页第二次机会', () => {
  const replacer = new ClockReplacer();
  replacer.unpin(1);
  replacer.unpin(2);
  replacer.unpin(3);

  // 第一圈扫描给所有页第二次机会（ref 清零），第二圈才淘汰首个 ref=0 的页。
  assert.equal(replacer.victim(), 1);
  assert.equal(replacer.victim(), 2);
  assert.equal(replacer.victim(), 3);
  assert.equal(replacer.victim(), undefined);
});

test('ClockReplacer 在 pin/remove 后仍保持正确的环形扫描', () => {
  const replacer = new ClockReplacer();
  replacer.unpin(1);
  replacer.unpin(2);
  replacer.pin(1);
  replacer.unpin(3);
  assert.equal(replacer.victim(), 2);
  assert.equal(replacer.victim(), 3);
  assert.equal(replacer.size, 0);
});
