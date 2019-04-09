// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { expect } from 'chai';
import * as mapping from '../../src/mapping';
import { Timestamp } from 'google-protobuf/google/protobuf/timestamp_pb';

import * as ledger from '../../src';
import * as grpc from 'daml-grpc';

describe('Reference Mapping (InclusiveFilters)', () => {

    it('should not throw an error with a valid input', () => {
        const object = {
            templateIds: [
                { name: 'foo', packageId: 'bar' }
            ]
        };
        expect(() => mapping.InclusiveFilters.toMessage(object)).to.not.throw(Error);
    });

    it('should push the right number of items into the populated array', () => {
        const object = {
            templateIds: [
                { name: 'foo', packageId: 'bar' }
            ]
        };
        const result = mapping.InclusiveFilters.toMessage(object);
        expect(result.getTemplateIdsList()).to.have.lengthOf(1);
    });

    it('should push the correctly built items to the array', () => {
        const object = {
            templateIds: [
                { name: 'foo', packageId: 'bar' }
            ]
        };
        const result = mapping.InclusiveFilters.toMessage(object);
        const identifier = result.getTemplateIdsList()[0];
        expect(identifier.getName()).to.equal('foo');
        expect(identifier.getPackageId()).to.equal('bar');
    });

    it('should work for more than one item as well', () => {
        const object = {
            templateIds: [
                { name: 'foo', packageId: 'bar' },
                { name: 'baz', packageId: 'quux' },
            ]
        };
        const result = mapping.InclusiveFilters.toMessage(object);
        const identifier1 = result.getTemplateIdsList()[0];
        const identifier2 = result.getTemplateIdsList()[1];
        expect(identifier1.getName()).to.equal('foo');
        expect(identifier1.getPackageId()).to.equal('bar');
        expect(identifier2.getName()).to.equal('baz');
        expect(identifier2.getPackageId()).to.equal('quux');
    });

    it('should not throw exception with a valid "bean"', () => {
        const message = new grpc.InclusiveFilters();
        const identifier = new grpc.Identifier();
        identifier.setName('foo');
        identifier.setPackageId('bar');
        message.setTemplateIdsList([identifier]);
        expect(() => mapping.InclusiveFilters.toObject(message)).to.not.throw(Error);
    });

    it('should map array items with the correct values from beans to objects', () => {
        const message = new grpc.InclusiveFilters();
        const identifier = new grpc.Identifier();
        identifier.setName('foo');
        identifier.setPackageId('bar');
        message.setTemplateIdsList([identifier]);
        expect(mapping.InclusiveFilters.toObject(message).templateIds[0]).to.deep.equal({ name: 'foo', packageId: 'bar' });
    });

    it('should map array items with the correct values from beans to objects with multiple items', () => {
        const message = new grpc.InclusiveFilters();
        const identifier1 = new grpc.Identifier();
        identifier1.setName('foo');
        identifier1.setPackageId('bar');
        const identifier2 = new grpc.Identifier();
        identifier2.setName('baz');
        identifier2.setPackageId('quux');
        message.setTemplateIdsList([identifier1, identifier2]);
        expect(mapping.InclusiveFilters.toObject(message)).to.deep.equal({
            templateIds: [
                { name: 'foo', packageId: 'bar' },
                { name: 'baz', packageId: 'quux' },
            ]
        });
    });

});

describe('Reference Mapping (Filters)', () => {

    it('should not set any property unless explicitly told to', () => {
        const message = mapping.Filters.toMessage({});
        expect(message.getInclusive()).to.be.undefined;
    });

    it('should not throw an error when converting an empty array', () => {
        const conversion = () => mapping.Filters.toMessage({ inclusive: { templateIds: [] } });
        expect(conversion).to.not.throw(Error);
    });

    it('should yield the expected value for nested properties of an empty object', () => {
        const result = mapping.Filters.toMessage({ inclusive: { templateIds: [] } });
        expect(result.getInclusive()!.getTemplateIdsList()).to.have.length(0);
    });

    it('should not throw an error when converting an array with one item', () => {
        const conversion = () => mapping.Filters.toMessage({
            inclusive: {
                templateIds: [
                    { name: 'foo', packageId: 'bar' }
                ]
            }
        });
        expect(conversion).to.not.throw(Error);
    });

    it('should convert a valid object with a one-item array to its properly sized value', () => {
        const result = mapping.Filters.toMessage({
            inclusive: {
                templateIds: [
                    { name: 'foo', packageId: 'bar' }
                ]
            }
        });
        expect(result.getInclusive()!.getTemplateIdsList()).to.have.length(1);
    });

    it('should convert a valid object with a one-item array to its proper value for nested properties', () => {
        const result = mapping.Filters.toMessage({
            inclusive: {
                templateIds: [
                    { name: 'foo', packageId: 'bar' }
                ]
            }
        });
        expect(result.getInclusive()!.getTemplateIdsList()[0].getName()).to.equal('foo');
        expect(result.getInclusive()!.getTemplateIdsList()[0].getPackageId()).to.equal('bar');
    });

    it('should not throw an error when converting an array with two items', () => {
        const conversion = () => mapping.Filters.toMessage({
            inclusive: {
                templateIds: [
                    { name: 'foo', packageId: 'bar' },
                    { name: 'baz', packageId: 'quux' }
                ]
            }
        });
        expect(conversion).to.not.throw(Error);
    });

    it('should convert a valid object with a two-item array to its properly sized value', () => {
        const result = mapping.Filters.toMessage({
            inclusive: {
                templateIds: [
                    { name: 'foo', packageId: 'bar' },
                    { name: 'baz', packageId: 'quux' }
                ]
            }
        });
        expect(result.getInclusive()!.getTemplateIdsList()).to.have.length(2);
    });

});

describe('Reference Mapping (TransactionFilter)', () => {

    it('should not throw when converting a valid, empty input', () => {
        const filter = { filtersByParty: {} };
        const conversion = () => mapping.TransactionFilter.toMessage(filter);
        expect(conversion).to.not.throw(Error);
    });

    it('should result in the correct content value for an empty object', () => {
        const filter = { filtersByParty: {} };
        const result = mapping.TransactionFilter.toMessage(filter);
        expect(result.getFiltersByPartyMap().toArray()).to.have.lengthOf(0);
    });

    it('should add a key for a valid empty object as input', () => {
        const filter = {
            filtersByParty: {
                someKey: {}
            }
        };
        const result = mapping.TransactionFilter.toMessage(filter);
        const map = result.getFiltersByPartyMap();
        expect(map.has('someKey')).to.be.true;
    });

    it('should add a correctly empty value for a valid empty object as input', () => {
        const filter = {
            filtersByParty: {
                someKey: {}
            }
        };
        const result = mapping.TransactionFilter.toMessage(filter);
        const map = result.getFiltersByPartyMap();
        expect(map.get('someKey')!.getInclusive()).to.be.undefined;
    });

    it('should add a fully built value for a valid object as input', () => {
        const filter = {
            filtersByParty: {
                someKey: { inclusive: { templateIds: [] } }
            }
        };
        const result = mapping.TransactionFilter.toMessage(filter);
        const map = result.getFiltersByPartyMap();
        expect(map.get('someKey')!.getInclusive()).to.not.be.undefined;
    });

    it('should not throw when converting a valid, empty message', () => {
        const message = new grpc.TransactionFilter();
        const conversion = () => mapping.TransactionFilter.toObject(message);
        expect(conversion).to.not.throw(Error);
    });

    it('should fill in the properties correctly for an empty input message', () => {
        const message = new grpc.TransactionFilter();
        const result = mapping.TransactionFilter.toObject(message);
        expect(result.filtersByParty).to.be.empty;
    });

    it('should fill in the properties for a non-empty input message with empty nested properties', () => {
        const message = new grpc.TransactionFilter();
        const map = message.getFiltersByPartyMap();
        map.set('someKey', new grpc.Filters());
        const result = mapping.TransactionFilter.toObject(message);
        expect(result.filtersByParty).to.not.be.empty;
    });

    it('should fill in the properties correctly for a non-empty input message with non-empty nested properties', () => {
        const transactionFilter = new grpc.TransactionFilter();
        const map = transactionFilter.getFiltersByPartyMap();
        const filters = new grpc.Filters();
        const inclusive = new grpc.InclusiveFilters();
        const identifier1 = new grpc.Identifier();
        identifier1.setName('foo');
        identifier1.setPackageId('bar');
        const identifier2 = new grpc.Identifier();
        identifier2.setName('baz');
        identifier2.setPackageId('quux');
        inclusive.setTemplateIdsList([identifier1, identifier2]);
        filters.setInclusive(inclusive);
        map.set('someKey', filters);
        const result = mapping.TransactionFilter.toObject(transactionFilter);
        expect(result).to.deep.equal({
            filtersByParty: {
                someKey: {
                    inclusive: {
                        templateIds: [
                            { name: 'foo', packageId: 'bar' },
                            { name: 'baz', packageId: 'quux' }
                        ]
                    }
                }
            }
        });
    });

    it('should fill in the properties correctly for a non-empty input message with empty and non-empty nested properties', () => {
        const transactionFilter = new grpc.TransactionFilter();
        const map = transactionFilter.getFiltersByPartyMap();
        const filters = new grpc.Filters();
        const inclusive = new grpc.InclusiveFilters();
        const identifier1 = new grpc.Identifier();
        identifier1.setName('foo');
        identifier1.setPackageId('bar');
        const identifier2 = new grpc.Identifier();
        identifier2.setName('baz');
        identifier2.setPackageId('quux');
        inclusive.setTemplateIdsList([identifier1, identifier2]);
        filters.setInclusive(inclusive);
        map.set('someKey', filters);
        map.set('someOtherKey', new grpc.Filters());
        const result = mapping.TransactionFilter.toObject(transactionFilter);
        expect(result).to.deep.equal({
            filtersByParty: {
                someOtherKey: {},
                someKey: {
                    inclusive: {
                        templateIds: [
                            { name: 'foo', packageId: 'bar' },
                            { name: 'baz', packageId: 'quux' }
                        ]
                    }
                }
            }
        });
    });

});

describe('Reference Mapping (SubmitRequest)', () => {

    const command = new grpc.Command();

    const templateId = new grpc.Identifier();
    templateId.setName('templateId-name');
    templateId.setPackageId('templateId-packageId');

    const recordId = new grpc.Identifier();
    recordId.setName('recordId-name');
    recordId.setPackageId('recordId-packageId');

    const create = new grpc.CreateCommand();
    create.setTemplateId(templateId);

    const record = new grpc.Record();
    record.setRecordId(recordId);

    const senderField = new grpc.RecordField();
    senderField.setLabel('sender');
    const senderValue = new grpc.Value();
    senderValue.setParty('sender-party');
    senderField.setValue(senderValue);
    record.addFields(senderField);

    const receiverField = new grpc.RecordField();
    receiverField.setLabel('receiver');
    const receiverValue = new grpc.Value();
    receiverValue.setParty('receiver-party');
    receiverField.setValue(receiverValue);
    record.addFields(receiverField);

    const countField = new grpc.RecordField();
    countField.setLabel('count');
    const countValue = new grpc.Value();
    countValue.setInt64('42');
    countField.setValue(countValue);
    record.addFields(countField);
    create.setCreateArguments(record);

    command.setCreate(create);

    const message = new grpc.SubmitRequest();

    const commands = new grpc.Commands();
    commands.setLedgerId('ledgerId');

    const ledgerEffectiveTime = new Timestamp();
    ledgerEffectiveTime.setSeconds(47);
    ledgerEffectiveTime.setNanos(68);
    commands.setLedgerEffectiveTime(ledgerEffectiveTime);

    const maximumRecordTime = new Timestamp();
    maximumRecordTime.setSeconds(94);
    maximumRecordTime.setNanos(140);
    commands.setMaximumRecordTime(maximumRecordTime);

    commands.setCommandId('command-commandId');
    commands.setWorkflowId('command-workflowId');
    commands.setParty('command-party');
    commands.setApplicationId('command-applicationId');
    commands.setCommandsList([command]);

    message.setCommands(commands);

    const object: ledger.SubmitRequest = {
        commands: {
            ledgerEffectiveTime: { seconds: 47, nanoseconds: 68 },
            maximumRecordTime: { seconds: 94, nanoseconds: 140 },
            commandId: 'command-commandId',
            workflowId: 'command-workflowId',
            party: 'command-party',
            applicationId: 'command-applicationId',
            list: [
                {
                    create: {
                        templateId: { packageId: 'templateId-packageId', name: 'templateId-name' },
                        arguments: {
                            recordId: { packageId: 'recordId-packageId', name: 'recordId-name' },
                            fields: {
                                sender: { party: 'sender-party' },
                                receiver: { party: 'receiver-party' },
                                count: { int64: '42' }
                            }
                        }
                    }
                }
            ]
        }
    }

    it('should correctly translate a message to an object', () => {

        expect(mapping.SubmitRequest.toObject(message)).to.deep.equal(object);

    });

    it('should translate an object to a message both ways while preserving meaning', () => {

        expect(mapping.SubmitRequest.toObject(mapping.SubmitRequest.toMessage(object))).to.deep.equal(object);

    });

});

describe('Reference Mapping (SubmitRequest/Pvp)', () => {

    const command = new grpc.Command();

    const pvpId = new grpc.Identifier();
    pvpId.setName('Pvp');
    pvpId.setPackageId('934023fa9c89e8f89b8a');

    const create = new grpc.CreateCommand();
    create.setTemplateId(pvpId);

    const record = new grpc.Record();
    record.setRecordId(pvpId);

    const baseAmountField = new grpc.RecordField();
    baseAmountField.setLabel('baseAmount');
    const baseAmountValue = new grpc.Value();
    baseAmountValue.setDecimal('1000000.00');
    baseAmountField.setValue(baseAmountValue);
    record.addFields(baseAmountField);

    const baseCurrencyField = new grpc.RecordField();
    baseCurrencyField.setLabel('baseCurrency');
    const baseCurrencyValue = new grpc.Value();
    baseCurrencyValue.setText('CHF');
    baseCurrencyField.setValue(baseCurrencyValue);
    record.addFields(baseCurrencyField);

    const baseIouCidField = new grpc.RecordField();
    baseIouCidField.setLabel('baseIouCid');
    const baseIouCidValue = new grpc.Value();
    const baseIouCidVariant = new grpc.Variant();
    baseIouCidVariant.setConstructor('Maybe');
    const baseIouCidVariantId = new grpc.Identifier();
    baseIouCidVariantId.setName('Maybe');
    baseIouCidVariantId.setPackageId('ba777d8d7c88e87f7');
    baseIouCidVariant.setVariantId(baseIouCidVariantId);
    const baseIouCidVariantValue = new grpc.Value();
    baseIouCidVariantValue.setContractId('76238b8998a98d98e978f');
    baseIouCidVariant.setValue(baseIouCidVariantValue);
    baseIouCidValue.setVariant(baseIouCidVariant);
    baseIouCidField.setValue(baseIouCidValue);
    record.addFields(baseIouCidField);

    const baseIssuerField = new grpc.RecordField();
    baseIssuerField.setLabel('baseIssuer');
    const baseIssuerValue = new grpc.Value();
    baseIssuerValue.setParty('some-base-issuer');
    baseIssuerField.setValue(baseIssuerValue);
    record.addFields(baseIssuerField);

    const buyerField = new grpc.RecordField();
    buyerField.setLabel('buyer');
    const buyerValue = new grpc.Value();
    buyerValue.setParty('some-buyer');
    buyerField.setValue(buyerValue);
    record.addFields(buyerField);

    const quoteAmountField = new grpc.RecordField();
    quoteAmountField.setLabel('quoteAmount');
    const quoteAmountValue = new grpc.Value();
    quoteAmountValue.setDecimal('1000001.00');
    quoteAmountField.setValue(quoteAmountValue);
    record.addFields(quoteAmountField);

    const quoteCurrencyField = new grpc.RecordField();
    quoteCurrencyField.setLabel('quoteCurrency');
    const quoteCurrencyValue = new grpc.Value();
    quoteCurrencyValue.setText('USD');
    quoteCurrencyField.setValue(quoteCurrencyValue);
    record.addFields(quoteCurrencyField);

    const quoteIouCidField = new grpc.RecordField();
    quoteIouCidField.setLabel('quoteIouCid');
    const quoteIouCidValue = new grpc.Value();
    const quoteIouCidVariant = new grpc.Variant();
    quoteIouCidVariant.setConstructor('Maybe');
    const quoteIouCidVariantId = new grpc.Identifier();
    quoteIouCidVariantId.setName('Maybe');
    quoteIouCidVariantId.setPackageId('ba777d8d7c88e87f7');
    quoteIouCidVariant.setVariantId(quoteIouCidVariantId);
    const quoteIouCidVariantValue = new grpc.Value();
    quoteIouCidVariantValue.setContractId('76238b8998a98d98e978f');
    quoteIouCidVariant.setValue(quoteIouCidVariantValue);
    quoteIouCidValue.setVariant(quoteIouCidVariant);
    quoteIouCidField.setValue(quoteIouCidValue);
    record.addFields(quoteIouCidField);

    const quoteIssuerField = new grpc.RecordField();
    quoteIssuerField.setLabel('quoteIssuer');
    const quoteIssuerValue = new grpc.Value();
    quoteIssuerValue.setParty('some-quote-issuer');
    quoteIssuerField.setValue(quoteIssuerValue);
    record.addFields(quoteIssuerField);

    const sellerField = new grpc.RecordField();
    sellerField.setLabel('seller');
    const sellerValue = new grpc.Value();
    sellerValue.setParty('some-seller');
    sellerField.setValue(sellerValue);
    record.addFields(sellerField);

    const settleTimeField = new grpc.RecordField();
    settleTimeField.setLabel('settleTime');
    const settleTimeValue = new grpc.Value();
    settleTimeValue.setTimestamp(93641099000000000);
    settleTimeField.setValue(settleTimeValue);
    record.addFields(settleTimeField);

    create.setCreateArguments(record);

    command.setCreate(create);

    const message = new grpc.SubmitRequest();

    const commands = new grpc.Commands();
    commands.setLedgerId('ledgerId');

    const ledgerEffectiveTime = new Timestamp();
    ledgerEffectiveTime.setSeconds(0);
    ledgerEffectiveTime.setNanos(0);
    commands.setLedgerEffectiveTime(ledgerEffectiveTime);

    const maximumRecordTime = new Timestamp();
    maximumRecordTime.setSeconds(5);
    maximumRecordTime.setNanos(0);
    commands.setMaximumRecordTime(maximumRecordTime);

    commands.setCommandId('78676d87b86d86');
    commands.setWorkflowId('some-workflow-id');
    commands.setParty('some-sender');
    commands.setApplicationId('some-app-id');
    commands.setCommandsList([command]);

    message.setCommands(commands);

    const reference = {
        commands: {
            ledgerEffectiveTime: { seconds: 0, nanoseconds: 0 },
            maximumRecordTime: { seconds: 5, nanoseconds: 0 },
            commandId: '78676d87b86d86',
            workflowId: 'some-workflow-id',
            party: 'some-sender',
            applicationId: 'some-app-id',
            list: [
                {
                    create: {
                        templateId: { packageId: '934023fa9c89e8f89b8a', name: 'Pvp' },
                        arguments: {
                            recordId: { packageId: '934023fa9c89e8f89b8a', name: 'Pvp' },
                            fields: {
                                buyer        : { party: 'some-buyer' },
                                seller       : { party: 'some-seller' },
                                baseIssuer   : { party: 'some-base-issuer' },
                                baseCurrency : { text: 'CHF' },
                                baseAmount   : { decimal: '1000000.00' },
                                baseIouCid   : { variant: { variantId: { packageId: 'ba777d8d7c88e87f7', name: 'Maybe' }, constructor: 'Maybe', value: { contractId: '76238b8998a98d98e978f' } } },
                                quoteIssuer  : { party: 'some-quote-issuer' },
                                quoteCurrency: { text: 'USD' },
                                quoteAmount  : { decimal: '1000001.00' },
                                quoteIouCid  : { variant: { variantId: { packageId: 'ba777d8d7c88e87f7', name: 'Maybe' }, constructor: 'Maybe', value: { contractId: '76238b8998a98d98e978f' } } },
                                settleTime   : { timestamp: 93641099000000000 }
                            }
                        }
                    }
                }
            ]
        }
    };

    it('should match exactly the expected result', () => {

        expect(mapping.SubmitRequest.toObject(message)).to.deep.equal(reference);

    });

});

describe('Non-verbose records', () => {

    it('should be mapped to numeric indexes', () => {
        const expected: ledger.Record = {
            fields: {
                '0': { int64: '42' },
                '1': { contractId: '0123456789abcdef' },
                '2': { bool: true }
            }
        }
        const record: grpc.Record = new grpc.Record();
        const value0: grpc.Value = new grpc.Value();
        const field0: grpc.RecordField = new grpc.RecordField();
        const value1: grpc.Value = new grpc.Value();
        const field1: grpc.RecordField = new grpc.RecordField();
        const value2: grpc.Value = new grpc.Value();
        const field2: grpc.RecordField = new grpc.RecordField();
        field0.setValue(value0);
        value0.setInt64('42');
        field1.setValue(value1);
        value1.setContractId('0123456789abcdef');
        field2.setValue(value2);
        value2.setBool(true);
        record.setFieldsList([field0, field1, field2]);
        expect(mapping.Record.toObject(record)).to.deep.equal(expected);
    });

});
