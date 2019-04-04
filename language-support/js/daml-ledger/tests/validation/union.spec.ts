// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as jsc from 'jsverify';
import * as arbitrary from '../arbitrary';
import * as validation from '../../src/validation';
import { ok, UnionValidation } from "../../src/validation/base";

import { containsError } from '.';

function test<A extends { [_: string]: any }>([validation, arbitrary]: [UnionValidation<A>, jsc.Arbitrary<A>]): void {
    describe(`Validation: ${validation.type}`, () => {
        if (validation.type !== 'null') {
            jsc.property('not validate a null', () => {
                return !ok(validation.validate(null));
            });
            jsc.property('signal a type error on a null', () => {
                return containsError(validation.validate(null).errors, {
                    kind: 'type-error',
                    expectedType: validation.type,
                    actualType: 'null'
                });
            });
        }
        jsc.property('validate well-formed objects', arbitrary, value => {
            return ok(validation.validate(value));
        });
        jsc.property('not validate objects with an extra key', arbitrary, value => {
            const extraKey = 'supercalifragilisticexpialidocious'; // reasonably no one will ever use this as a key
            value[extraKey] = null;
            return !ok(validation.validate(value));
        });
        jsc.property('signal an unexpected key error on objects with an extra key', arbitrary, value => {
            const extraKey = 'supercalifragilisticexpialidocious'; // reasonably no one will ever use this as a key
            value[extraKey] = null;
            return containsError(validation.validate(value).errors, {
                kind: 'unexpected-key',
                key: extraKey
            });
        });
        jsc.property('not validate objects without at least one defined key', () => {
            return !ok(validation.validate({}));
        });
        jsc.property('signal a non-unique union error on objects without at least one defined key', () => {
            return containsError(validation.validate({}).errors, {
                kind: 'non-unique-union',
                keys: []
            });
        });
        jsc.property('not validate objects with more than one defined key', arbitrary, value => {
            const values = validation.values();
            const keys = Object.keys(values);
            let set = keys.filter(key => value[key] !== undefined);
            for (const key of keys) {
                if (set.some(set => set === key)) continue;
                value[key] = null;
                set.push(key);
                break;
            }
            return containsError(validation.validate(value).errors, {
                kind: 'non-unique-union',
                keys: set
            });
        });
    });
}

[
    [validation.Command, arbitrary.Command],
    [validation.Event, arbitrary.Event],
    [validation.TreeEvent, arbitrary.TreeEvent],
    [validation.LedgerOffset, arbitrary.LedgerOffset],
    [validation.Value, arbitrary.Value],

].forEach(test);