// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { expect } from 'chai';
import * as ledger from '../src';

describe('Timestamp.lessThen', () => {

    it('should work as expected', () => {

        expect(ledger.Timestamp.lessThanOrEqual({ seconds: 1, nanoseconds: 1 }, { seconds: 1, nanoseconds: 1 }),
            'when left side has same seconds, same nanos result should be true').to.be.true;

        expect(ledger.Timestamp.lessThanOrEqual({ seconds: 1, nanoseconds: 0 }, { seconds: 1, nanoseconds: 1 }),
            'when left side has when left side has same seconds, less nanos result should be true').to.be.true;

        expect(ledger.Timestamp.lessThanOrEqual({ seconds: 1, nanoseconds: 2 }, { seconds: 1, nanoseconds: 1 }),
            'when left side has same seconds, more nanos result should be false').to.be.false;

        expect(ledger.Timestamp.lessThanOrEqual({ seconds: 0, nanoseconds: 1 }, { seconds: 1, nanoseconds: 1 }),
            'when left side has less seconds, same nanos result should be true').to.be.true;

        expect(ledger.Timestamp.lessThanOrEqual({ seconds: 0, nanoseconds: 0 }, { seconds: 1, nanoseconds: 1 }),
            'when left side has less seconds, less nanos result should be true').to.be.true;

        expect(ledger.Timestamp.lessThanOrEqual({ seconds: 0, nanoseconds: 2 }, { seconds: 1, nanoseconds: 1 }),
            'when left side has less seconds, more nanos result should be true').to.be.true;

        expect(ledger.Timestamp.lessThanOrEqual({ seconds: 2, nanoseconds: 1 }, { seconds: 1, nanoseconds: 1 }),
            'when left side has more seconds, same nanos result should be false').to.be.false;

        expect(ledger.Timestamp.lessThanOrEqual({ seconds: 2, nanoseconds: 0 }, { seconds: 1, nanoseconds: 1 }),
            'when left side has more seconds, less nanos result should be false').to.be.false;

        expect(ledger.Timestamp.lessThanOrEqual({ seconds: 2, nanoseconds: 2 }, { seconds: 1, nanoseconds: 1 }),
            'when left side has more seconds, more nanos result should be false').to.be.false;

    });

});