// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { expect } from 'chai';
import * as ledger from '../../../src';
import * as validation from '../../../src/validation';

describe('Validation: Object', () => {

    it('should report a correct tree as such', () => {
        const identifier: ledger.Identifier = {
            packageId: 'bar',
            moduleName: 'foo',
            entityName: 'baz',
        };
        const expected: validation.Tree = {
            errors: [],
            children: {
                moduleName: {
                    errors: [],
                    children: {}
                },
                entityName: {
                    errors: [],
                    children: {}
                },
                packageId: {
                    errors: [],
                    children: {}
                }
            }
        };
        expect(validation.Identifier.validate(identifier)).to.deep.equal(expected);
    });

    it('should correctly report a missing key', () => {
        const invalidIdentifier = {
            packageId: 'bar'
        };
        const expected: validation.Tree = {
            errors: [{
                kind: 'missing-key',
                expectedKey: 'moduleName',
                expectedType: 'string'
            },{
                kind: 'missing-key',
                expectedKey: 'entityName',
                expectedType: 'string'
            }],
            children: {
                packageId: {
                    errors: [],
                    children: {}
                }
            }
        };
        expect(validation.Identifier.validate(invalidIdentifier)).to.deep.equal(expected);
    });

    it('should correctly report a type error in a child', () => {
        const invalidIdentifier = {
            packageId: 'bar',
            moduleName: 42,
            entityName: 'baz',
        };
        const expected: validation.Tree = {
            errors: [],
            children: {
                moduleName: {
                    errors: [
                        {
                            kind: 'type-error',
                            expectedType: 'string',
                            actualType: 'number'
                        }
                    ],
                    children: {}
                },
                packageId: {
                    errors: [],
                    children: {}
                },
                entityName: {
                    errors: [],
                    children: {}
                },
            }
        };
        expect(validation.Identifier.validate(invalidIdentifier)).to.deep.equal(expected);
    });

    it('should correctly report multiple type errors in the children', () => {
        const invalidIdentifier = {
            packageId: true,
            moduleName: 42,
            entityName: false,
        };
        const expected: validation.Tree = {
            errors: [],
            children: {
                moduleName: {
                    errors: [
                        {
                            kind: 'type-error',
                            expectedType: 'string',
                            actualType: 'number'
                        }
                    ],
                    children: {}
                },
                packageId: {
                    errors: [
                        {
                            kind: 'type-error',
                            expectedType: 'string',
                            actualType: 'boolean'
                        }
                    ],
                    children: {}
                },
                entityName: {
                    errors: [
                        {
                            kind: 'type-error',
                            expectedType: 'string',
                            actualType: 'boolean'
                        }
                    ],
                    children: {}
                },
            }
        };
        expect(validation.Identifier.validate(invalidIdentifier)).to.deep.equal(expected);
    });

    it('should correctly report a type error at the root (passing a native)', () => {
        const invalidIdentifier = 42;
        const expected: validation.Tree = {
            errors: [
                {
                    kind: 'type-error',
                    expectedType: 'Identifier',
                    actualType: 'number'
                }
            ],
            children: {}
        }
        expect(validation.Identifier.validate(invalidIdentifier)).to.deep.equal(expected);
    });

    it('should correctly report a type error at the root (passing an array)', () => {
        const invalidIdentifier = [ 42, 47 ];
        const expected: validation.Tree = {
            errors: [
                {
                    kind: 'type-error',
                    expectedType: 'Identifier',
                    actualType: 'array'
                }
            ],
            children: {}
        }
        expect(validation.Identifier.validate(invalidIdentifier)).to.deep.equal(expected);
    });

    it('should validate the filters without the optional fields', () => {
        const emptyFilters: ledger.Filters = {}
        const expected: validation.Tree = {
            errors: [],
            children: {}
        };
        expect(validation.Filters.validate(emptyFilters)).to.deep.equal(expected);
    });

    it('should validate a filter with an empty set of inclusive filters', () => {
        const filters: ledger.Filters = {
            inclusive: {
                templateIds: []
            }
        }
        const expected: validation.Tree = {
            errors: [],
            children: {
                inclusive: {
                    errors: [],
                    children: {
                        templateIds: {
                            errors: [],
                            children: {}
                        }
                    }
                }
            }
        };
        expect(validation.Filters.validate(filters)).to.deep.equal(expected);
    });

    it('should validate a filter with a set of inclusive filters with one identifier', () => {
        const filters: ledger.Filters = {
            inclusive: {
                templateIds: [{
                    packageId: 'bar',
                    moduleName: 'foo',
                    entityName: 'baz',
                }]
            }
        }
        const expected: validation.Tree = {
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
                                        moduleName: {
                                            errors: [],
                                            children: {}
                                        },
                                        entityName: {
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
        };
        expect(validation.Filters.validate(filters)).to.deep.equal(expected);
    });

    it('should not validate a string', () => {
        const invalidFilters = 'not a valid object :(';
        const expected: validation.Tree = {
            errors: [{
                kind: 'type-error',
                expectedType: 'Filters',
                actualType: 'string'
            }],
            children: {}
        };
        expect(validation.Filters.validate(invalidFilters)).to.deep.equal(expected);
    });

    it('should report in case of a crass mistake', () => {
        const actuallyInclusiveFilters: ledger.InclusiveFilters = {
            templateIds: [{
                packageId: 'bar1',
                moduleName: 'foo1',
                entityName: 'baz1',
            }, {
                packageId: 'bar2',
                moduleName: 'foo2',
                entityName: 'baz2',
            }]
        };
        const expected: validation.Tree = {
            errors: [{
                kind: 'unexpected-key',
                key: 'templateIds'
            }],
            children: {}
        };
        expect(validation.Filters.validate(actuallyInclusiveFilters)).to.deep.equal(expected);
    });

});