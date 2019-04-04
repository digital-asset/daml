// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { expect } from 'chai';
import * as ledger from '../../../src';
import * as validation from '../../../src/validation';

describe('Validation: Union', () => {

    it('should validate an absolute offset', () => {
        const offset: ledger.LedgerOffset = {
            absolute: '20'
        };
        const expected: validation.Tree = {
            errors: [],
            children: {
                absolute: {
                    errors: [],
                    children: {}
                }
            }
        };
        expect(validation.LedgerOffset.validate(offset)).to.deep.equal(expected);
    });

    it('should validate a valid boundary (begin)', () => {
        const offset: ledger.LedgerOffset = {
            boundary: ledger.LedgerOffset.Boundary.BEGIN
        };
        const expected: validation.Tree = {
            errors: [],
            children: {
                boundary: {
                    errors: [],
                    children: {}
                }
            }
        };
        expect(validation.LedgerOffset.validate(offset)).to.deep.equal(expected);
    });

    it('should validate a valid boundary (end)', () => {
        const offset: ledger.LedgerOffset = {
            boundary: ledger.LedgerOffset.Boundary.END
        };
        const expected: validation.Tree = {
            errors: [],
            children: {
                boundary: {
                    errors: [],
                    children: {}
                }
            }
        };
        expect(validation.LedgerOffset.validate(offset)).to.deep.equal(expected);
    });

    it('should not validate a ledger offset with both (valid) absolute and boundary values', () => {
        const offset: ledger.LedgerOffset = {
            absolute: '20',
            boundary: ledger.LedgerOffset.Boundary.END
        };
        const expected: validation.Tree = {
            errors: [{
                kind: 'non-unique-union',
                keys: ['absolute', 'boundary']
            }],
            children: {
                absolute: {
                    errors: [],
                    children: {}
                },
                boundary: {
                    errors: [],
                    children: {}
                }
            }
        };
        expect(validation.LedgerOffset.validate(offset)).to.deep.equal(expected);
    });

    it('should not validate a ledger offset without values', () => {
        const offset: ledger.LedgerOffset = {};
        const expected: validation.Tree = {
            errors: [{
                kind: 'non-unique-union',
                keys: []
            }],
            children: {}
        };
        expect(validation.LedgerOffset.validate(offset)).to.deep.equal(expected);
    });

    it('should not validate a ledger offset with an unexpected key', () => {
        const offset = {
            wrong: '20'
        };
        const expected: validation.Tree = {
            errors: [{
                kind: 'unexpected-key',
                key: 'wrong'
            }],
            children: {}
        };
        expect(validation.LedgerOffset.validate(offset)).to.deep.equal(expected);
    });

});