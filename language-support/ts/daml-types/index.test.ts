// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
import { Optional, Text, memo, Map } from './index';

describe('@daml/types', () => {
  it('optional', () => {
    const dict = Optional(Text);
    expect(dict.decoder.run(null).ok).toBe(true);
    expect(dict.decoder.run('X').ok).toBe(true);
    expect(dict.decoder.run([]).ok).toBe(false);
    expect(dict.decoder.run(['X']).ok).toBe(false);
  });

  it('nested optionals', () => {
    const dict = Optional(Optional(Text));
    expect(dict.decoder.run(null).ok).toBe(true);
    expect(dict.decoder.run([]).ok).toBe(true);
    expect(dict.decoder.run(['X']).ok).toBe(true);
    expect(dict.decoder.run('X').ok).toBe(false);
    expect(dict.decoder.run([['X']]).ok).toBe(false);
    expect(dict.decoder.run([[]]).ok).toBe(false);
    expect(dict.decoder.run([null]).ok).toBe(false);
  });
  it('genmap', () => {
    const {encode, decoder} = Map(Text, Text);
    const decode = (u: unknown): Map<Text, Text> => decoder.runWithException(u);
    const kvs = [["1", "a"], ["2", "b"]];
    expect(encode(decode(kvs))).toEqual(kvs);
    const m = decode(kvs);
    expect(m.get("1")).toEqual("a");
    expect(m.get("3")).toEqual(null);
    expect(m.getOr("3", "hello")).toEqual("hello");
    expect(m.has("1")).toBe(true);
    expect(m.has("a")).toBe(false);
    expect(m.insert("3", "c").get("3")).toEqual("c");
    expect(m.getOr("3", "hello")).toEqual("hello");
    expect(m.remove("1").getOr("1", "not found")).toEqual("not found");
    expect(m.get("1")).toEqual("a");
    expect(m.keys()).toEqual(["1", "2"]);
    expect(m.values()).toEqual(["a", "b"]);
    expect(m.kvs()).toEqual(kvs);
  });
});

test('memo', () => {
  let x = 0;
  const f = memo(() => {
    x += 1;
    return x;
  });
  expect(f()).toBe(1);
  expect(f()).toBe(1);
  expect(x).toBe(1);
});
