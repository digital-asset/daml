// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as jsc from 'jsverify';
import * as arbitrary from '../arbitrary';
import * as validation from '../../src/validation';
import { ok, ObjectValidation } from '../../src/validation/base';

import { containsError, pickFrom } from '.';

function test<A extends { [_: string]: any }>([validation, arbitrary]: [ObjectValidation<A>, jsc.Arbitrary<A>]): void {
    describe(`Validation: ${validation.type}`, () => {
        const required = validation.required();
        const optional = validation.optional();
        const requiredKeys = Object.keys(required);
        const optionalKeys = Object.keys(optional);
        const validations = Object.assign({}, required, optional);
        const keys = Object.keys(validations);
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
        if (requiredKeys.length > 0) {
            jsc.property('not validate objects without a required key', arbitrary, value => {
                delete value[pickFrom(requiredKeys)];
                return !ok(validation.validate(value));
            });
            jsc.property('signal a missing key error on objects without a required key', arbitrary, value => {
                const removedKey = pickFrom(requiredKeys);
                const removedKeyType = (<any>required)[removedKey].type;
                delete value[removedKey];
                return containsError(validation.validate(value).errors, {
                    kind: 'missing-key',
                    expectedKey: removedKey,
                    expectedType: removedKeyType
                });
            });
        }
        if (optionalKeys.length > 0) {
            jsc.property('still validate objects without an optional key', arbitrary, value => {
                delete value[pickFrom(optionalKeys)];
                return ok(validation.validate(value));
            });
        }
        if (keys.length > 0) {
            jsc.property('not validate object with a nulled out property', arbitrary, value => {
                const key = pickFrom(keys);
                value[key] = null;
                return !ok(validation.validate(value));
            });
            jsc.property('signal a type error on object with a nulled out property', arbitrary, value => {
                const key = pickFrom(keys);
                value[key] = null;
                const children = validation.validate(value).children;
                return Object.keys(children).some(key => containsError(children[key].errors, {
                    kind: 'type-error',
                    expectedType: (<any>validations)[key].type,
                    actualType: 'null'
                }));
            });
        }
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
    });
}

[
    [validation.Any, arbitrary.Any],
    [validation.ArchivedEvent, arbitrary.ArchivedEvent],
    [validation.Checkpoint, arbitrary.Checkpoint],
    [validation.Commands, arbitrary.Commands],
    [validation.Completion, arbitrary.Completion],
    [validation.CompletionEndResponse, arbitrary.CompletionEndResponse],
    [validation.CompletionStreamRequest, arbitrary.CompletionStreamRequest],
    [validation.CompletionStreamResponse, arbitrary.CompletionStreamResponse],
    [validation.CreateCommand, arbitrary.CreateCommand],
    [validation.CreatedEvent, arbitrary.CreatedEvent],
    [validation.Duration, arbitrary.Duration],
    [validation.Empty, arbitrary.Empty],
    [validation.ExercisedEvent, arbitrary.ExercisedEvent],
    [validation.ExerciseCommand, arbitrary.ExerciseCommand],
    [validation.Filters, arbitrary.Filters],
    [validation.GetActiveContractsRequest, arbitrary.GetActiveContractsRequest],
    [validation.GetActiveContractsResponse, arbitrary.GetActiveContractsResponse],
    [validation.GetLedgerConfigurationResponse, arbitrary.GetLedgerConfigurationResponse],
    [validation.GetLedgerEndResponse, arbitrary.GetLedgerEndResponse],
    [validation.GetLedgerIdentityResponse, arbitrary.GetLedgerIdentityResponse],
    [validation.GetPackageResponse, arbitrary.GetPackageResponse],
    [validation.GetPackageStatusResponse, arbitrary.GetPackageStatusResponse],
    [validation.GetTimeResponse, arbitrary.GetTimeResponse],
    [validation.GetTransactionByEventIdRequest, arbitrary.GetTransactionByEventIdRequest],
    [validation.GetTransactionByIdRequest, arbitrary.GetTransactionByIdRequest],
    [validation.GetTransactionResponse, arbitrary.GetTransactionResponse],
    [validation.GetTransactionsRequest, arbitrary.GetTransactionsRequest],
    [validation.GetTransactionsResponse, arbitrary.GetTransactionsResponse],
    [validation.GetTransactionTreesResponse, arbitrary.GetTransactionTreesResponse],
    [validation.Identifier, arbitrary.Identifier],
    [validation.InclusiveFilters, arbitrary.InclusiveFilters],
    [validation.LedgerConfiguration, arbitrary.LedgerConfiguration],
    [validation.ListPackagesResponse, arbitrary.ListPackagesResponse],
    [validation.Optional, arbitrary.Optional],
    [validation.Record, arbitrary.Record],
    [validation.SetTimeRequest, arbitrary.SetTimeRequest],
    [validation.Status, arbitrary.Status],
    [validation.SubmitRequest, arbitrary.SubmitRequest],
    [validation.SubmitAndWaitRequest, arbitrary.SubmitAndWaitRequest],
    [validation.Timestamp, arbitrary.Timestamp],
    [validation.TransactionFilter, arbitrary.TransactionFilter],
    [validation.Transaction, arbitrary.Transaction],
    [validation.TransactionTree, arbitrary.TransactionTree],
    [validation.Variant, arbitrary.Variant],

].forEach(test);