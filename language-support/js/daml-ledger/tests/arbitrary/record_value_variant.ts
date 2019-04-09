// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as jsc from 'jsverify';
import * as ledger from '../../src';
import { Identifier } from './identifier';
import { maybe } from './maybe';
import { Empty } from './empty';

declare module 'jsverify' {
    function letrec<A>(tie: (f: (key: string) => jsc.Arbitrary<A>) => { [key: string]: jsc.Arbitrary<A> }): { [key: string]: jsc.Arbitrary<A> }
}

const BoolValue: jsc.Arbitrary<ledger.Value> =
    jsc.bool.smap<{ bool: boolean }>(
        boolean => ({ bool: boolean }),
        value => value.bool
    );
const ContractIdValue: jsc.Arbitrary<ledger.Value> =
    jsc.string.smap<{ contractId: string }>(
        string => ({ contractId: string }),
        value => value.contractId
    );
const DateValue: jsc.Arbitrary<ledger.Value> =
    jsc.string.smap<{ date: string }>(
        number => ({ date: number }),
        value => value.date
    );
const DecimalValue: jsc.Arbitrary<ledger.Value> =
    jsc.string.smap<{ decimal: string }>(
        string => ({ decimal: string }),
        value => value.decimal
    );
const Int64Value: jsc.Arbitrary<ledger.Value> =
    jsc.string.smap<{ int64: string }>(
        number => ({ int64: number }),
        value => value.int64
    );
const PartyValue: jsc.Arbitrary<ledger.Value> =
    jsc.string.smap<{ party: string }>(
        string => ({ party: string }),
        value => value.party
    );
const TextValue: jsc.Arbitrary<ledger.Value> =
    jsc.string.smap<{ text: string }>(
        string => ({ text: string }),
        value => value.text
    );
const TimestampValue: jsc.Arbitrary<ledger.Value> =
    jsc.string.smap<{ timestamp: string }>(
        string => ({ timestamp: string }),
        value => value.timestamp
    );
const UnitValue: jsc.Arbitrary<ledger.Value> =
    Empty.smap<{ unit: ledger.Empty }>(
        empty => ({ unit: empty }),
        value => value.unit
    );

const { Record: record, Value: value, Variant: variant }: { [key: string]: jsc.Arbitrary<ledger.Record | ledger.Value | ledger.Variant> } =
    jsc.letrec<ledger.Record | ledger.Value | ledger.Variant>(tie => ({
        ListValue: jsc.array(tie('Value') as jsc.Arbitrary<ledger.Value>).smap<{ list: ledger.Value[] }>(
            list => ({ list: list }),
            value => value.list
        ),
        Record: jsc.pair(maybe(Identifier), jsc.dict(tie('Value') as jsc.Arbitrary<ledger.Value>)).smap<ledger.Record>(
            ([recordId, fields]) => {
                const record: ledger.Record = {
                    fields: fields
                };
                if (recordId) {
                    record.recordId = recordId;
                }
                return record;
            },
            (record) => {
                return [record.recordId, record.fields]
            }
        ),
        RecordValue: (tie('Record') as jsc.Arbitrary<ledger.Record>).smap<{ record: ledger.Record }>(
            record => ({ record: record }),
            value => value.record
        ),
        VariantValue: (tie('Variant') as jsc.Arbitrary<ledger.Variant>).smap<{ variant: ledger.Variant }>(
            variant => ({ variant: variant }),
            value => value.variant
        ),
        Value: jsc.oneof([
            BoolValue,
            ContractIdValue,
            DateValue,
            DecimalValue,
            Int64Value,
            tie('ListValue'),
            PartyValue,
            tie('RecordValue'),
            TextValue,
            TimestampValue,
            UnitValue,
            tie('VariantValue')
        ]),
        Variant: jsc.tuple([jsc.string, tie('Value') as jsc.Arbitrary<ledger.Value>, maybe(Identifier)]).smap<ledger.Variant>(
            ([constructor, value, variantId]) => {
                const variant: ledger.Variant = {
                    constructor: constructor,
                    value: value
                };
                if (variantId) {
                    variant.variantId = variantId;
                }
                return variant;
            },
            (variant) => {
                return [variant.constructor, variant.value, variant.variantId]
            }
        )
    }));

    export const Record = record as jsc.Arbitrary<ledger.Record>;
    export const Value = value as jsc.Arbitrary<ledger.Value>;
    export const Variant = variant as jsc.Arbitrary<ledger.Variant>;
