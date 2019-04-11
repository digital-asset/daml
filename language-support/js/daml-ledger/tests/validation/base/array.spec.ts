// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { expect } from 'chai';
import { array, native } from '../../../src/validation/base'
import { Tree, Identifier, InclusiveFilters } from '../../../src/validation';
import * as ledger from '../../../src';

describe('Validation: Array', () => {

    it('should validate an empty array correctly', () => {
        const expected: Tree = {
            errors: [],
            children: {}
        }
        expect(array(native('number')).validate([])).to.deep.equal(expected);
    });

    it('should not validate a number', () => {
        const expected: Tree = {
            errors: [{
                kind: 'type-error',
                expectedType: 'Array<number>',
                actualType: 'number'
            }],
            children: {}
        }
        expect(array(native('number')).validate(42)).to.deep.equal(expected);
    });

    it('should not validate a string', () => {
        const expected: Tree = {
            errors: [{
                kind: 'type-error',
                expectedType: 'Array<number>',
                actualType: 'string'
            }],
            children: {}
        }
        expect(array(native('number')).validate('42')).to.deep.equal(expected);
    });

    it('should validate an array with one number', () => {
        const expected: Tree = {
            errors: [],
            children: {
                '0': {
                    errors: [],
                    children: {}
                }
            }
        }
        expect(array(native('number')).validate([42])).to.deep.equal(expected);
    });

    it('should validate an array with two numbers', () => {
        const expected: Tree = {
            errors: [],
            children: {
                '0': {
                    errors: [],
                    children: {}
                },
                '1': {
                    errors: [],
                    children: {}
                }
            }
        }
        expect(array(native('number')).validate([42, 47])).to.deep.equal(expected);
    });

    it('should correctly report an error if an underlying item is of the wrong type', () => {
        const expected: Tree = {
            errors: [],
            children: {
                '0': {
                    errors: [],
                    children: {}
                },
                '1': {
                    errors: [{
                        kind: 'type-error',
                        expectedType: 'number',
                        actualType: 'string'
                    }],
                    children: {}
                }
            }
        }
        expect(array(native('number')).validate([42, '47'])).to.deep.equal(expected);
    });

    it('should validate an array with two objects', () => {
        const identifiers: ledger.Identifier[] = [
            {
                packageId: 'bar1',
                moduleName: 'foo1',
                entityName: 'baz1'
            },
            {
                packageId: 'bar2',
                moduleName: 'foo2',
                entityName: 'baz2'
            }
        ]
        const expected: Tree = {
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
                },
                '1': {
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
        expect(array(Identifier).validate(identifiers)).to.deep.equal(expected);
    });

    it('should correctly report errors in an array with invalid objects', () => {
        const invalidIdentifiers = [
            'not-an-identifier :(',
            {
                moduleName: 'foo',
                entityName: 'baz'
            }
        ]
        const expected: Tree = {
                errors: [],
                children: {
                    '0': {
                        errors: [{
                            kind: 'type-error',
                            expectedType: 'Identifier',
                            actualType: 'string'
                        }],
                        children: {}
                    },
                    '1': {
                        errors: [{
                            kind: 'missing-key',
                            expectedKey: 'packageId',
                            expectedType: 'string'
                        }],
                        children: {
                            moduleName: {
                                errors: [],
                                children: {}
                            },
                            entityName: {
                                errors: [],
                                children: {}
                            },
                        }
                    }
                }
        }
        expect(array(Identifier).validate(invalidIdentifiers)).to.deep.equal(expected);
    });

    it('should validate an empty set of filters', () => {
        const inclusiveFilters: ledger.InclusiveFilters = {
            templateIds: []
        };
        const expected: Tree = {
            errors: [],
            children: {
                templateIds: {
                    errors: [],
                    children: {}
                }
            }
        };
        expect(InclusiveFilters.validate(inclusiveFilters)).to.deep.equal(expected);
    });

    it('should validate an set of filters with one identifier', () => {
        const inclusiveFilters: ledger.InclusiveFilters = {
            templateIds: [{
                packageId: 'bar',
                moduleName: 'foo',
                entityName: 'baz',
            }]
        };
        const expected: Tree = {
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
        };
        expect(InclusiveFilters.validate(inclusiveFilters)).to.deep.equal(expected);
    });

    it('should validate an set of filters with two identifiers', () => {
        const inclusiveFilters: ledger.InclusiveFilters = {
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
        const expected: Tree = {
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
                        },
                        '1': {
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
        };
        expect(InclusiveFilters.validate(inclusiveFilters)).to.deep.equal(expected);
    });

    it('should not validate a string', () => {
        const invalidInclusiveFilters = 'not a valid object :(';
        const expected: Tree = {
            errors: [{
                kind: 'type-error',
                expectedType: 'InclusiveFilters',
                actualType: 'string'
            }],
            children: {}
        };
        expect(InclusiveFilters.validate(invalidInclusiveFilters)).to.deep.equal(expected);
    });

    it('should provide precise feedback about a single mistake', () => {
        const invalidInclusiveFilters = {
            templateIds: [{
                packageId: 'bar1',
                moduleName: 'foo1',
                entityName: 'baz1',
            }, {
                packageId: 42,
                moduleName: 'foo2',
                entityName: 'baz2',
            }]
        };
        const expected: Tree = {
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
                        },
                        '1': {
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
                                    errors: [{
                                        kind: 'type-error',
                                        expectedType: 'string',
                                        actualType: 'number'
                                    }],
                                    children: {}
                                }
                            }
                        }
                    }
                }
            }
        };
        expect(InclusiveFilters.validate(invalidInclusiveFilters)).to.deep.equal(expected);
    });

    it('should provide thorough feedback about extensive mistakes', () => {
        const invalidInclusiveFilters = {
            templateIds: [{
                moduleName: false
            },
                42
            ]
        };
        const expected: Tree = {
            errors: [],
            children: {
                templateIds: {
                    errors: [],
                    children: {
                        '0': {
                            errors: [{
                                kind: 'missing-key',
                                expectedKey: 'packageId',
                                expectedType: 'string'
                            },{
                                kind: 'missing-key',
                                expectedKey: 'entityName',
                                expectedType: 'string'
                            }],
                            children: {
                                moduleName: {
                                    errors: [{
                                        kind: 'type-error',
                                        expectedType: 'string',
                                        actualType: 'boolean'
                                    }],
                                    children: {}
                                }
                            }
                        },
                        '1': {
                            errors: [{
                                kind: 'type-error',
                                expectedType: 'Identifier',
                                actualType: 'number'
                            }],
                            children: {}
                        }
                    }
                }
            }
        };
        expect(InclusiveFilters.validate(invalidInclusiveFilters)).to.deep.equal(expected);
    });

});