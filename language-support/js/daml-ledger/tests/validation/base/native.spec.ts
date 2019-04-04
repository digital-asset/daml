// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { expect } from 'chai';
import { native } from '../../../src/validation/base'
import { Tree } from '../../../src/validation';

describe('Validation: Native', () => {

    it('should validate a simple string correctly', () => {
        const expected: Tree = {
            errors: [],
            children: {}
        }
        expect(native('string').validate('hello, world')).to.deep.equal(expected);
    });

    it('should fail to validate a number when a string is expected', () => {
        const expected: Tree = {
            errors: [{
                kind: 'type-error',
                expectedType: 'string',
                actualType: 'number'
            }],
            children: {}
        }
        expect(native('string').validate(42)).to.deep.equal(expected);
    });

    it('should fail to validate a string when a boolean is expected', () => {
        const expected: Tree = {
            errors: [{
                kind: 'type-error',
                expectedType: 'boolean',
                actualType: 'string'
            }],
            children: {}
        }
        expect(native('boolean').validate('true')).to.deep.equal(expected);
    });

});