// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
import { Optional, Text, memo } from './index';

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
