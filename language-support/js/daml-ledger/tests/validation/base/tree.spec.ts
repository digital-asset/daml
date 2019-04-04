// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { expect } from 'chai';
import { ok, Tree } from '../../../src/validation';

describe('Tree: ok', () => {

    it('should tell the empty tree is valid', () => {
        const tree: Tree = {
            errors: [],
            children: {}
        }
        expect(ok(tree)).to.be.true;
    });

    it('should tell a well-formed tree is such', () => {
        const tree: Tree = {
            errors: [],
            children: {
                inclusive: {
                    errors: [],
                    children: {
                        templateIds: {
                            errors: [],
                            children: {
                                '0': {
                                    errors: [],
                                    children: {
                                        name: {
                                            errors: [],
                                            children: {}
                                        },
                                        packageId: {
                                            errors: [],
                                            children: {}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        expect(ok(tree)).to.be.true;
    });

    it('should tell an ill-formed tree is such', () => {
        const tree: Tree = {
            errors: [],
            children: {
                filtersByParty: {
                    errors: [],
                    children: {
                        birthday: {
                            errors: [{
                                kind: 'type-error',
                                expectedType: 'Filters',
                                actualType: 'string'
                            }],
                            children: {}
                        }
                    }
                }
            }
        }
        expect(ok(tree)).to.be.false;
    });

});