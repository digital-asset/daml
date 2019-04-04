// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as jsc from 'jsverify';
import * as ledger from '../../src';

// Can't use jsc.constant: we are going to mutate the object for test purposes and `constant` returns always the same instance
export const Empty: jsc.Arbitrary<ledger.Empty> = jsc.bless({ generator: jsc.generator.bless(() => ({})) });