// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
import { Optional, Text, memo, Map, emptyMap } from "./index";
import _ from "lodash";

function makeArr<T>(it: Iterator<T, undefined, undefined>): T[] {
  const r: T[] = [];
  let i = it.next();
  while (!i.done) {
    r.push(i.value);
    i = it.next();
  }
  return r;
}

describe("@daml/types", () => {
  it("optional", () => {
    const dict = Optional(Text);
    expect(dict.decoder.run(null).ok).toBe(true);
    expect(dict.decoder.run("X").ok).toBe(true);
    expect(dict.decoder.run([]).ok).toBe(false);
    expect(dict.decoder.run(["X"]).ok).toBe(false);
  });

  it("nested optionals", () => {
    const dict = Optional(Optional(Text));
    expect(dict.decoder.run(null).ok).toBe(true);
    expect(dict.decoder.run([]).ok).toBe(true);
    expect(dict.decoder.run(["X"]).ok).toBe(true);
    expect(dict.decoder.run("X").ok).toBe(false);
    expect(dict.decoder.run([["X"]]).ok).toBe(false);
    expect(dict.decoder.run([[]]).ok).toBe(false);
    expect(dict.decoder.run([null]).ok).toBe(false);
  });
  it("genmap", () => {
    const { encode, decoder } = Map(Text, Text);
    const decode = (u: unknown): Map<Text, Text> => decoder.runWithException(u);
    const kvs = [
      ["1", "a"],
      ["2", "b"],
    ];
    expect(encode(decode(kvs))).toEqual(kvs);
    const m = decode(kvs);
    expect(m.get("1")).toEqual("a");
    expect(m.get("3")).toEqual(undefined);
    expect(m.has("1")).toBe(true);
    expect(m.has("a")).toBe(false);
    expect(m.set("3", "c").get("3")).toEqual("c");
    expect(m.delete("1").get("1") || "not found").toEqual("not found");
    expect(m.get("1")).toEqual("a");
    expect(makeArr(m.keys())).toEqual(["1", "2"]);
    expect(makeArr(m.values())).toEqual(["a", "b"]);
    expect(makeArr(m.entries())).toEqual(kvs);
    expect(m.entriesArray()).toEqual(kvs);
    const counter = (function (): () => number {
      let i = 0;
      return function (): number {
        i = i + 1;
        return i;
      };
    })();
    const m2 = emptyMap<string, string>()
      .set("a", "b")
      .set("c", "d")
      .set("e", "f");
    const res1: [string, string, Map<string, string>, number][] = [];
    m2.forEach(function (this: () => number, v, k, m) {
      res1.push([v, k, m, this()]);
    }, counter);
    const res2: [string, string, Map<string, string>][] = [];
    m2.forEach((v, k, m) => {
      res2.push([v, k, m]);
    });
    expect(res1).toEqual([
      ["b", "a", m2, 1],
      ["d", "c", m2, 2],
      ["f", "e", m2, 3],
    ]);
    expect(res2).toEqual([
      ["b", "a", m2],
      ["d", "c", m2],
      ["f", "e", emptyMap().set("e", "f").set("a", "b").set("c", "d")],
    ]);
    for (const k of ["a", "c", "e", "not found"]) {
      expect(m2.get(k)).toEqual(
        makeArr(m2.values())[
          makeArr(m2.keys()).findIndex(l => _.isEqual(k, l))
        ],
      );
    }
  });
  it("nested genmap", () => {
    const { encode, decoder } = Map(Text, Map(Text, Text));
    const decoded: Map<Text, Map<Text, Text>> = emptyMap<
      Text,
      Map<Text, Text>
    >().set("a", emptyMap<Text, Text>().set("b", "c"));
    const decode = (u: unknown): Map<Text, Map<Text, Text>> =>
      decoder.runWithException(u);
    const encoded = [["a", [["b", "c"]]]];
    expect(encode(decoded)).toEqual(encoded);
    expect(decode(encoded)).toEqual(decoded);
  });
});

test("memo", () => {
  let x = 0;
  const f = memo(() => {
    x += 1;
    return x;
  });
  expect(f()).toBe(1);
  expect(f()).toBe(1);
  expect(x).toBe(1);
});
