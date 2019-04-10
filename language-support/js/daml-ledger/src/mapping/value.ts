// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';
import * as protobuf from 'google-protobuf/google/protobuf/empty_pb';

import { inspect } from 'util';

export const Value: mapping.Mapping<grpc.Value, ledger.Value> = {
    toObject(value: grpc.Value): ledger.Value {
        if (value.hasBool()) {
            return { bool: value.getBool() }
        } else if (value.hasContractId()) {
            return { contractId: value.getContractId() }
        } else if (value.hasDate()) {
            return { date: '' + value.getDate() }
        } else if (value.hasDecimal()) {
            return { decimal: value.getDecimal() }
        } else if (value.hasInt64()) {
            return { int64: value.getInt64() }
        } else if (value.hasList()) {
            const values: ledger.Value[] = [];
            if (value.hasList()) {
                value.getList()!.getElementsList().forEach(v => {
                    values.push(Value.toObject(v));
                });
            }
            return { list: values }
        } else if (value.hasParty()) {
            return { party: value.getParty() }
        } else if (value.hasRecord()) {
            return { record: mapping.Record.toObject(value.getRecord()!) }
        } else if (value.hasText()) {
            return { text: value.getText() }
        } else if (value.hasTimestamp()) {
            return { timestamp: value.getTimestamp() }
        } else if (value.hasUnit()) {
            return { unit: {} }
        } else if (value.hasVariant()) {
            const variant = value.getVariant()!;
            const result: ledger.Variant = {
                constructor: variant.getConstructor(),
                value: Value.toObject(variant.getValue()!),
            }
            if (variant.hasVariantId()) {
                result.variantId = mapping.Identifier.toObject(variant.getVariantId()!);
            }
            return { variant: result }
        } else if (value.hasOptional()) {
            return { optional: mapping.Optional.toObject(value.getOptional()!) }
        } else {
            throw new Error(`Message Value of unknown type '${inspect(value)}'`);
        }
    },
    toMessage(value: ledger.Value): grpc.Value {
        const result = new grpc.Value();
        if (value.bool !== undefined) {
            result.setBool(value.bool);
        } else if (value.contractId !== undefined) {
            result.setContractId(value.contractId);
        } else if (value.date !== undefined) {
            result.setDate(parseInt(value.date));
        } else if (value.decimal !== undefined) {
            result.setDecimal(value.decimal);
        } else if (value.int64 !== undefined) {
            result.setInt64(value.int64);
        } else if (value.list !== undefined) {
            const values: grpc.Value[] = []
            value.list.forEach(v => values.push(Value.toMessage(v)));
            const list = new grpc.List();
            list.setElementsList(values);
            result.setList(list);
        } else if (value.party !== undefined) {
            result.setParty(value.party);
        } else if (value.record !== undefined) {
            result.setRecord(mapping.Record.toMessage(value.record));
        } else if (value.text !== undefined) {
            result.setText(value.text);
        } else if (value.timestamp !== undefined) {
            result.setTimestamp(value.timestamp);
        } else if (value.unit !== undefined) {
            result.setUnit(new protobuf.Empty());
        } else if (value.variant !== undefined) {
            const variant = new grpc.Variant();
            variant.setConstructor(value.variant.constructor);
            variant.setValue(Value.toMessage(value.variant.value));
            if (value.variant.variantId) {
                variant.setVariantId(mapping.Identifier.toMessage(value.variant.variantId));
            }
            result.setVariant(variant);
        } else if (value.optional) {
            result.setOptional(mapping.Optional.toMessage(value.optional));
        } else {
            throw new Error(`Object Value of unknown type '${inspect(value)}'`);
        }
        return result;
    }
}