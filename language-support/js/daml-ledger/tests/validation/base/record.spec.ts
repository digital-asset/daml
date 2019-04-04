// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { expect } from 'chai';
import * as ledger from '../../../src';
import * as validation from '../../../src/validation';

describe('Validation: Record', () => {

    it('should validate an empty object', () => {
        const transactionFilter: ledger.TransactionFilter = {
            filtersByParty: {}
        };
        const expected: validation.Tree = {
            errors: [],
            children: {
                filtersByParty: {
                    errors: [],
                    children: {}
                }
            }
        };
        expect(validation.TransactionFilter.validate(transactionFilter)).to.deep.equal(expected);
    });

    it('should validate a simple correct object', () => {
        const transactionFilter: ledger.TransactionFilter = {
            filtersByParty: {
                birthday: {
                    inclusive: {
                        templateIds: [
                            {
                                name: 'digital',
                                packageId: 'asset'
                            }
                        ]
                    }
                }
            }
        };
        const expected: validation.Tree = {
            errors: [],
            children: {
                filtersByParty: {
                    errors: [],
                    children: {
                        birthday: {
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
                    }
                }
            }
        };
        expect(validation.TransactionFilter.validate(transactionFilter)).to.deep.equal(expected);
    });

    it('should not validate an ill-formed object', () => {
        const invalidTransactionFilter = {
            filtersByParty: {
                birthday: 42
            }
        };
        const expected: validation.Tree = {
            errors: [],
            children: {
                filtersByParty: {
                    errors: [],
                    children: {
                        birthday: {
                            errors: [{
                                kind: 'type-error',
                                expectedType: 'Filters',
                                actualType: 'number'
                            }],
                            children: {}
                        }
                    }
                }
            }
        };
        expect(validation.TransactionFilter.validate(invalidTransactionFilter)).to.deep.equal(expected);
    });

});