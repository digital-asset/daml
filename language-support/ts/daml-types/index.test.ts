// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
import { Optional, Text } from './index';

describe('@daml/types', () => {
  it('optional', () => {
    const dict = Optional(Text);
    expect(dict.decoder().run(null).ok).toBe(true);
    expect(dict.decoder().run('X').ok).toBe(true);
    expect(dict.decoder().run([]).ok).toBe(false);
    expect(dict.decoder().run(['X']).ok).toBe(false);
  });

  it('nested optionals', () => {
    const dict = Optional(Optional(Text));
    expect(dict.decoder().run(null).ok).toBe(true);
    expect(dict.decoder().run([]).ok).toBe(true);
    expect(dict.decoder().run(['X']).ok).toBe(true);
    expect(dict.decoder().run('X').ok).toBe(false);
    expect(dict.decoder().run([['X']]).ok).toBe(false);
    expect(dict.decoder().run([[]]).ok).toBe(false);
    expect(dict.decoder().run([null]).ok).toBe(false);
  });
});
