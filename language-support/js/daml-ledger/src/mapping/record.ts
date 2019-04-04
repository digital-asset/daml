// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

export const Record: mapping.Mapping<grpc.Record, ledger.Record> = {
    toObject(record: grpc.Record): ledger.Record {
        let fieldIndex = 0; // used for records returned in non-verbose mode
        const fields: {[k: string]: ledger.Value} = {};
        record.getFieldsList().forEach((field: grpc.RecordField) => {
            fields[field.getLabel() || fieldIndex++] = mapping.Value.toObject(field.getValue()!);
        });
        const result: ledger.Record = { fields: fields }
        if (record.hasRecordId()) {
            result.recordId = mapping.Identifier.toObject(record.getRecordId()!);
        }
        return result;
    },
    toMessage(record: ledger.Record): grpc.Record {
        const result = new grpc.Record();
        if (record.recordId) {
            result.setRecordId(mapping.Identifier.toMessage(record.recordId));
        }
        const list: grpc.RecordField[] = []
        Object.keys(record.fields).forEach((label: string) => {
            const value = mapping.Value.toMessage(record.fields[label]);
            const field = new grpc.RecordField();
            field.setLabel(label);
            field.setValue(value);
            list.push(field);
        });
        result.setFieldsList(list);
        return result;
    }
}