// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { expect } from 'chai';

import * as mapping from '../../src/mapping';
import * as ledger from '../../src';
import * as grpc from 'daml-grpc';

import { Timestamp } from 'google-protobuf/google/protobuf/timestamp_pb';
import { Duration } from 'google-protobuf/google/protobuf/duration_pb';
import { Empty } from 'google-protobuf/google/protobuf/empty_pb';
import { Any } from 'google-protobuf/google/protobuf/any_pb';

describe("Mapping", () => {

    const packageId = 'packageId';
    const templateName = 'templateName';

    const identifierObject: ledger.Identifier = {
        packageId: packageId,
        name: templateName
    }

    const inclusiveFiltersObject: ledger.InclusiveFilters = {
        templateIds: [identifierObject]
    }

    const filtersObject: ledger.Filters = {
        inclusive: inclusiveFiltersObject
    }

    const transactionFilterObject: ledger.TransactionFilter = {
        filtersByParty: {
            'Alice': filtersObject,
            'Bob': {}
        }
    }

    const identifierMessage = new grpc.Identifier();
    identifierMessage.setPackageId(packageId);
    identifierMessage.setName(templateName);

    const inclusiveFiltersMessage = new grpc.InclusiveFilters();
    inclusiveFiltersMessage.setTemplateIdsList([identifierMessage]);

    const filtersMessage = new grpc.Filters();
    filtersMessage.setInclusive(inclusiveFiltersMessage);

    const transactionFilterMessage = new grpc.TransactionFilter();
    transactionFilterMessage.getFiltersByPartyMap().set('Alice', filtersMessage);
    transactionFilterMessage.getFiltersByPartyMap().set('Bob', new grpc.Filters());

    const textValue = new grpc.Value();
    textValue.setText('text');
    const textField = new grpc.RecordField();
    textField.setLabel('textLabel');
    textField.setValue(textValue);
    const dateValue = new grpc.Value();
    dateValue.setDate(40);
    const dateField = new grpc.RecordField();
    dateField.setLabel('dateLabel');
    dateField.setValue(dateValue);
    const partyValue = new grpc.Value();
    partyValue.setParty('Bob');
    const partyField = new grpc.RecordField();
    partyField.setLabel('partyLabel');
    partyField.setValue(partyValue);
    const nestedRecord = new grpc.Record();
    nestedRecord.setRecordId(identifierMessage);
    nestedRecord.setFieldsList([partyField]);
    const nestedValue = new grpc.Value();
    nestedValue.setRecord(nestedRecord);
    const nestedField = new grpc.RecordField();
    nestedField.setLabel('nestedLabel');
    nestedField.setValue(nestedValue);
    const recordMessage = new grpc.Record();
    recordMessage.setRecordId(identifierMessage);
    recordMessage.setFieldsList([textField, dateField, nestedField]);

    const recordObject: ledger.Record = {
        recordId: identifierObject,
        fields: {
            textLabel: { text: 'text' },
            dateLabel: { date: 40 },
            nestedLabel: {
                record: {
                    recordId: identifierObject,
                    fields: {
                        partyLabel: { party: 'Bob' }
                    }
                }
            }
        }
    }

    const createCommandMessage = new grpc.CreateCommand();
    createCommandMessage.setTemplateId(identifierMessage);
    createCommandMessage.setCreateArguments(recordMessage);
    const createCommandObject: ledger.CreateCommand = {
        templateId: identifierObject,
        arguments: recordObject
    }

    const choiceArgument = new grpc.Value();
    choiceArgument.setBool(true);
    const exerciseCommandMessage = new grpc.ExerciseCommand();
    exerciseCommandMessage.setTemplateId(identifierMessage);
    exerciseCommandMessage.setContractId('contractId2');
    exerciseCommandMessage.setChoice('choice');
    exerciseCommandMessage.setChoiceArgument(choiceArgument);
    const exerciseCommandObject: ledger.ExerciseCommand = {
        templateId: identifierObject,
        contractId: 'contractId2',
        choice: 'choice',
        argument: { bool: true }
    };


    const command1 = new grpc.Command();
    command1.setCreate(createCommandMessage);
    const command2 = new grpc.Command();
    command2.setExercise(exerciseCommandMessage);
    const ledgerEffectiveTime = new Timestamp();
    ledgerEffectiveTime.setSeconds(60);
    ledgerEffectiveTime.setNanos(61);
    const maximumRecordTime = new Timestamp();
    maximumRecordTime.setSeconds(62);
    maximumRecordTime.setNanos(63);
    const commandsMessage = new grpc.Commands();
    commandsMessage.setApplicationId('applicationId');
    commandsMessage.setCommandId('commandId');
    commandsMessage.setLedgerEffectiveTime(ledgerEffectiveTime);
    commandsMessage.setMaximumRecordTime(maximumRecordTime);
    commandsMessage.setParty('Alice');
    commandsMessage.setWorkflowId('workflowId');
    commandsMessage.setCommandsList([command1, command2]);
    const commandsObject: ledger.Commands = {
        applicationId: 'applicationId',
        commandId: 'commandId',
        ledgerEffectiveTime: { seconds: 60, nanoseconds: 61 },
        maximumRecordTime: { seconds: 62, nanoseconds: 63 },
        party: 'Alice',
        workflowId: 'workflowId',
        list: [{ create: createCommandObject }, { exercise: exerciseCommandObject }]
    }

    const createdEventMessage = new grpc.CreatedEvent();
    createdEventMessage.setEventId('eventId');
    createdEventMessage.setContractId('contractId');
    createdEventMessage.setTemplateId(identifierMessage);
    createdEventMessage.setCreateArguments(recordMessage);
    createdEventMessage.setWitnessPartiesList(['Alice', 'Bob']);
    const createdEventObject: ledger.CreatedEvent = {
        eventId: 'eventId',
        contractId: 'contractId',
        templateId: identifierObject,
        arguments: recordObject,
        witnessParties: ['Alice', 'Bob']
    };

    itShouldConvert("Identifier", () => {
        mappingCheck(mapping.Identifier, identifierMessage, identifierObject);
    });

    itShouldConvert('InclusiveFilters', () => {
        mappingCheck(mapping.InclusiveFilters, inclusiveFiltersMessage, inclusiveFiltersObject);
    });

    itShouldConvert('Filters', () => {
        mappingCheck(mapping.Filters, filtersMessage, filtersObject);
    });

    itShouldConvert('TransactionsFilter', () => {
        mappingCheck(mapping.TransactionFilter, transactionFilterMessage, transactionFilterObject);
    });

    itShouldConvert('LedgerOffset.Absolute', () => {
        const absoluteObject = { absolute: '20' };
        const beginObject = { boundary: ledger.LedgerOffset.Boundary.BEGIN };
        const endObject = { boundary: ledger.LedgerOffset.Boundary.END };
        const absoluteMessage = new grpc.LedgerOffset();
        absoluteMessage.setAbsolute('20');
        const beginMessage = new grpc.LedgerOffset();
        beginMessage.setBoundary(grpc.LedgerOffset.LedgerBoundary.LEDGER_BEGIN);
        const endMessage = new grpc.LedgerOffset();
        endMessage.setBoundary(grpc.LedgerOffset.LedgerBoundary.LEDGER_END);
        expect(mapping.LedgerOffset.toMessage(absoluteObject)).to.deep.equal(absoluteMessage);
        expect(mapping.LedgerOffset.toMessage(beginObject)).to.deep.equal(beginMessage);
        expect(mapping.LedgerOffset.toMessage(endObject)).to.deep.equal(endMessage);
        expect(mapping.LedgerOffset.toObject(absoluteMessage)).to.deep.equal(absoluteObject);
        expect(mapping.LedgerOffset.toObject(beginMessage)).to.deep.equal(beginObject);
        expect(mapping.LedgerOffset.toObject(endMessage)).to.deep.equal(endObject);
    });

    itShouldConvert('Timestamp', () => {
        const timestampMessage = new Timestamp();
        timestampMessage.setSeconds(20);
        timestampMessage.setNanos(21);

        const timestampObject: ledger.Timestamp = {
            seconds: 20,
            nanoseconds: 21
        };

        mappingCheck(mapping.Timestamp, timestampMessage, timestampObject);
    });

    itShouldConvert('Value(Bool)', () => {
        const boolMessage = new grpc.Value();
        boolMessage.setBool(true);
        const boolObject = { bool: true };
        mappingCheck(mapping.Value, boolMessage, boolObject);
    });

    itShouldConvert('Value(ContractId)', () => {
        const contractIdMessage = new grpc.Value();
        contractIdMessage.setContractId('contractId');
        const contractIdObject: ledger.Value = { contractId: 'contractId' };
        mappingCheck(mapping.Value, contractIdMessage, contractIdObject);
    });

    itShouldConvert('Value(Date)', () => {
        const dateMessage = new grpc.Value();
        dateMessage.setDate(1);
        const dateObject: ledger.Value = { date: 1 }
        mappingCheck(mapping.Value, dateMessage, dateObject);
    });

    itShouldConvert('Value(Decimal)', () => {
        const decimalMessage = new grpc.Value();
        decimalMessage.setDecimal('30');
        const decimalObject: ledger.Value = { decimal: '30' };
        mappingCheck(mapping.Value, decimalMessage, decimalObject);
    });

    itShouldConvert('Value(Int64)', () => {
        const int64Message = new grpc.Value();
        int64Message.setInt64('40');
        const int64Object: ledger.Value = { int64: '40' };
        mappingCheck(mapping.Value, int64Message, int64Object);
    });

    itShouldConvert('Value(List)', () => {
        const emptyList = new grpc.List();
        emptyList.setElementsList([]);
        const emptyListMessage = new grpc.Value();
        emptyListMessage.setList(emptyList);
        const emptyListObject = { list: [] };
        mappingCheck(mapping.Value, emptyListMessage, emptyListObject);

        const dateMessage = new grpc.Value();
        dateMessage.setDate(2);
        const singletonList = new grpc.List();
        singletonList.setElementsList([dateMessage]);
        const singletonListMessage = new grpc.Value();
        singletonListMessage.setList(singletonList);
        const singletonListObject: ledger.Value = { list: [{ date: 2 }] };
        mappingCheck(mapping.Value, singletonListMessage, singletonListObject);
    });

    itShouldConvert('Value(Party)', () => {
        const partyMessage = new grpc.Value();
        partyMessage.setParty('Alice');
        const partyObject = { party: 'Alice' };
        mappingCheck(mapping.Value, partyMessage, partyObject);
    });

    itShouldConvert('Value(Record)', () => {
        const recordValueMessage = new grpc.Value();
        recordValueMessage.setRecord(recordMessage);
        const recordValueObject: ledger.Value = { record: recordObject };
        mappingCheck(mapping.Value, recordValueMessage, recordValueObject);
    });

    itShouldConvert('Value(Text)', () => {
        const textMessage = new grpc.Value();
        textMessage.setText('text2');
        const textObject: ledger.Value = { text: 'text2' };
        mappingCheck(mapping.Value, textMessage, textObject);
    });

    itShouldConvert('Value(Timestamp)', () => {
        const timestampMessage = new grpc.Value();
        timestampMessage.setTimestamp(50);
        const timestampObject: ledger.Value = { timestamp: 50 };
        mappingCheck(mapping.Value, timestampMessage, timestampObject);
    });

    itShouldConvert('Value(Unit)', () => {
        const unitMessage = new grpc.Value();
        unitMessage.setUnit(new Empty());
        expect(unitMessage.hasUnit()).to.be.true;
        const unitObject: ledger.Value = { unit: {} };
        mappingCheck(mapping.Value, unitMessage, unitObject);
    });

    itShouldConvert('Value(Variant)', () => {
        const recordValueMessage = new grpc.Value();
        recordValueMessage.setRecord(recordMessage);
        const variant = new grpc.Variant();
        variant.setConstructor('constructor');
        variant.setVariantId(identifierMessage);
        variant.setValue(recordValueMessage);
        const variantMessage = new grpc.Value();
        variantMessage.setVariant(variant);
        const variantObject: ledger.Value = {
            variant: {
                constructor: 'constructor',
                variantId: identifierObject,
                value: { record: recordObject }
            }
        }
        mappingCheck(mapping.Value, variantMessage, variantObject);
    });

    itShouldConvert('Record', () => {
        mappingCheck(mapping.Record, recordMessage, recordObject);
    });

    itShouldConvert('Command.Create', () => {
        mappingCheck(mapping.CreateCommand, createCommandMessage, createCommandObject);
    });

    itShouldConvert('Command.Exercise', () => {
        mappingCheck(mapping.ExerciseCommand, exerciseCommandMessage, exerciseCommandObject);
    });

    itShouldConvert('Command', () => {
        const commandMessage = new grpc.Command();
        commandMessage.setCreate(createCommandMessage);
        mappingCheck(mapping.Command, commandMessage, { create: createCommandObject });

        const commandMessage2 = new grpc.Command();
        commandMessage2.setExercise(exerciseCommandMessage);
        mappingCheck(mapping.Command, commandMessage2, { exercise: exerciseCommandObject });
    });

    itShouldConvert('Commands', () => {
        mappingCheck(mapping.Commands, commandsMessage, commandsObject);
    });

    itShouldConvert('SubmitRequest', () => {
        const submitRequestMessage = new grpc.SubmitRequest();
        submitRequestMessage.setCommands(commandsMessage);
        const submitRequestObject = { commands: commandsObject };
        mappingCheck(mapping.SubmitRequest, submitRequestMessage, submitRequestObject);
    });

    itShouldConvert('GetTransactionsRequest', () => {
        const offset = new grpc.LedgerOffset();
        offset.setAbsolute('70');
        const requestMessage = new grpc.GetTransactionsRequest();
        requestMessage.setBegin(offset);
        requestMessage.setFilter(transactionFilterMessage);
        requestMessage.setVerbose(true);
        const requestObject: ledger.GetTransactionsRequest = {
            begin: { absolute: '70' },
            filter: transactionFilterObject,
            verbose: true
        }
        mappingCheck(mapping.GetTransactionsRequest, requestMessage, requestObject);
        requestMessage.setEnd(offset);
        requestObject.end = requestObject.begin;
        mappingCheck(mapping.GetTransactionsRequest, requestMessage, requestObject);
    });

    itShouldConvert('GetActiveContractsRequest', () => {
        const requestMessage = new grpc.GetActiveContractsRequest();
        requestMessage.setVerbose(true);
        requestMessage.setFilter(transactionFilterMessage);
        const requestObject: ledger.GetActiveContractsRequest = {
            verbose: true,
            filter: transactionFilterObject
        };
        mappingCheck(mapping.GetActiveContractsRequest, requestMessage, requestObject);
    });

    itShouldConvert('CreatedEvent', () => {
        mappingCheck(mapping.CreatedEvent, createdEventMessage, createdEventObject);
    });

    itShouldConvert('GetActiveContractsResponse', () => {
        const responseMessage = new grpc.GetActiveContractsResponse();
        responseMessage.setOffset('7');
        responseMessage.setWorkflowId('wid');
        responseMessage.setActiveContractsList([createdEventMessage]);
        const responseObject: ledger.GetActiveContractsResponse = {
            offset: { absolute: '7' },
            workflowId: 'wid',
            activeContracts: [createdEventObject]
        };
        mappingCheck(mapping.GetActiveContractsResponse, responseMessage, responseObject);
    });

    itShouldConvert('GetLedgerIdentityResponse', () => {
        const responseMessage = new grpc.GetLedgerIdentityResponse();
        responseMessage.setLedgerId('ledgerId2');
        const responseObject: ledger.GetLedgerIdentityResponse = {
            ledgerId: 'ledgerId2'
        }
        mappingCheck(mapping.GetLedgerIdentityResponse, responseMessage, responseObject);
    });

    itShouldConvert('GetPackageResponse', () => {
        const responseMessage = new grpc.GetPackageResponse();
        responseMessage.setArchivePayload('cafebabe');
        responseMessage.setHash('deadbeef');
        responseMessage.setHashFunction(grpc.HashFunction.SHA256);
        const responseObject: ledger.GetPackageResponse = {
            archivePayload: 'cafebabe',
            hash: 'deadbeef',
            hashFunction: ledger.HashFunction.SHA256
        };
        mappingCheck(mapping.GetPackageResponse, responseMessage, responseObject);
    });

    itShouldConvert('ListPackagesResponse', () => {
        const responseMessage = new grpc.ListPackagesResponse();
        responseMessage.setPackageIdsList(['package1', 'package2']);
        const responseObject: ledger.ListPackagesResponse = {
            packageIds: ['package1', 'package2']
        };
        mappingCheck(mapping.ListPackagesResponse, responseMessage, responseObject);
    });

    itShouldConvert('SubmitAndWaitRequest', () => {
        const submitRequestMessage = new grpc.SubmitAndWaitRequest()
        submitRequestMessage.setCommands(commandsMessage);
        const submitRequestObject = { commands: commandsObject };
        mappingCheck(mapping.SubmitAndWaitRequest, submitRequestMessage, submitRequestObject);
    });

    itShouldConvert('CompletionEndResponse', () => {
        const message = new grpc.CompletionEndResponse();
        const offset = new grpc.LedgerOffset();
        offset.setAbsolute('20');
        message.setOffset(offset)
        const object: ledger.CompletionEndResponse = { offset: { absolute: '20' } };
        mappingCheck(mapping.CompletionEndResponse, message, object);
    });

    itShouldConvert('CompletionStreamRequest', () => {
        const message = new grpc.CompletionStreamRequest();
        const offset = new grpc.LedgerOffset();
        offset.setAbsolute('20');
        message.setApplicationId('space-invaders-on-the-blockchain');
        message.setOffset(offset);
        message.setPartiesList(['pool', 'birthday']);
        const object: ledger.CompletionStreamRequest = {
            applicationId: 'space-invaders-on-the-blockchain',
            offset: { absolute: '20' },
            parties: ['pool', 'birthday']
        };
        mappingCheck(mapping.CompletionStreamRequest, message, object)
    })

    itShouldConvert('Checkpoint', () => {
        const message = new grpc.Checkpoint();
        const offset = new grpc.LedgerOffset();
        offset.setAbsolute('20');
        message.setOffset(offset);
        const recordTime = new Timestamp();
        recordTime.setSeconds(42);
        recordTime.setNanos(999);
        message.setRecordTime(recordTime);
        const object: ledger.Checkpoint = {
            offset: { absolute: '20' },
            recordTime: { seconds: 42, nanoseconds: 999 }
        };
        mappingCheck(mapping.Checkpoint, message, object);
    });

    itShouldConvert('Any', () => {
        const message = new Any();
        message.setValue('deadbeef');
        message.setTypeUrl('some-url');
        const object: ledger.Any = {
            value: 'deadbeef',
            typeUrl: 'some-url'
        }
        mappingCheck(mapping.Any, message, object);
    });

    itShouldConvert('Status', () => {
        const message = new grpc.Status();
        message.setCode(42);
        message.setMessage('we come in peace');
        const any1 = new Any();
        any1.setValue('deadbeef');
        any1.setTypeUrl('some-url');
        const any2 = new Any();
        any2.setValue('cafebabe');
        any2.setTypeUrl('some-other-url');
        message.setDetailsList([any1, any2]);
        const object: ledger.Status = {
            code: 42,
            message: 'we come in peace',
            details: [
                {
                    value: 'deadbeef',
                    typeUrl: 'some-url'
                },
                {
                    value: 'cafebabe',
                    typeUrl: 'some-other-url'
                }
            ]
        }
        mappingCheck(mapping.Status, message, object);
    });

    itShouldConvert('Completion', () => {
        const message = new grpc.Completion();
        message.setCommandId('befehl');

        const status = new grpc.Status();
        status.setCode(42);
        status.setMessage('we come in peace');
        const any1 = new Any();
        any1.setValue('deadbeef');
        any1.setTypeUrl('some-url');
        const any2 = new Any();
        any2.setValue('cafebabe');
        any2.setTypeUrl('some-other-url');
        status.setDetailsList([any1, any2]);

        message.setStatus(status);

        const object: ledger.Completion = {
            commandId: 'befehl',
            status: {
                code: 42,
                message: 'we come in peace',
                details: [
                    {
                        value: 'deadbeef',
                        typeUrl: 'some-url'
                    },
                    {
                        value: 'cafebabe',
                        typeUrl: 'some-other-url'
                    }
                ]
            }
        }

        mappingCheck(mapping.Completion, message, object)
    });

    itShouldConvert('CompletionStreamResponse', () => {
        const message = new grpc.CompletionStreamResponse();

        const checkpoint = new grpc.Checkpoint();
        const offset = new grpc.LedgerOffset();
        offset.setAbsolute('20');
        checkpoint.setOffset(offset);
        const recordTime = new Timestamp();
        recordTime.setSeconds(42);
        recordTime.setNanos(999);
        checkpoint.setRecordTime(recordTime);

        message.setCheckpoint(checkpoint);

        const completion1 = new grpc.Completion();
        completion1.setCommandId('befehl1');

        const status1 = new grpc.Status();
        status1.setCode(42);
        status1.setMessage('we come in peace1');
        const any11 = new Any();
        any11.setValue('deadbeef');
        any11.setTypeUrl('some-url');
        const any12 = new Any();
        any12.setValue('cafebabe');
        any12.setTypeUrl('some-other-url');
        status1.setDetailsList([any11, any12]);

        completion1.setStatus(status1);

        const completion2 = new grpc.Completion();
        completion2.setCommandId('befehl2');

        const status2 = new grpc.Status();
        status2.setCode(47);
        status2.setMessage('we come in peace2');
        const any21 = new Any();
        any21.setValue('deadcafe');
        any21.setTypeUrl('some-url');
        const any22 = new Any();
        any22.setValue('cafebeef');
        any22.setTypeUrl('some-other-url');
        status2.setDetailsList([any21, any22]);

        completion2.setStatus(status2);

        message.setCompletionsList([completion1, completion2]);

        const object: ledger.CompletionStreamResponse = {
            checkpoint: {
                offset: { absolute: '20' },
                recordTime: { seconds: 42, nanoseconds: 999 }
            },
            completions: [
                {
                    commandId: 'befehl1',
                    status: {
                        code: 42,
                        message: 'we come in peace1',
                        details: [
                            {
                                value: 'deadbeef',
                                typeUrl: 'some-url'
                            },
                            {
                                value: 'cafebabe',
                                typeUrl: 'some-other-url'
                            }
                        ]
                    }
                },
                {
                    commandId: 'befehl2',
                    status: {
                        code: 47,
                        message: 'we come in peace2',
                        details: [
                            {
                                value: 'deadcafe',
                                typeUrl: 'some-url'
                            },
                            {
                                value: 'cafebeef',
                                typeUrl: 'some-other-url'
                            }
                        ]
                    }
                }
            ]
        }

        mappingCheck(mapping.CompletionStreamResponse, message, object);
    });

    itShouldConvert('Duration', () => {
        const message = new Duration();
        message.setSeconds(20);
        message.setNanos(21);

        const object: ledger.Duration = {
            seconds: 20,
            nanoseconds: 21
        };

        mappingCheck(mapping.Duration, message, object);
    });

    itShouldConvert('LedgerConfiguration', () => {
        const maxTtl = new Duration();
        maxTtl.setSeconds(20);
        maxTtl.setNanos(21);

        const minTtl = new Duration();
        minTtl.setSeconds(22);
        minTtl.setNanos(23);

        const message = new grpc.LedgerConfiguration();
        message.setMaxTtl(maxTtl);
        message.setMinTtl(minTtl);

        const object: ledger.LedgerConfiguration = {
            maxTtl: {
                seconds: 20,
                nanoseconds: 21
            },
            minTtl: {
                seconds: 22,
                nanoseconds: 23
            }
        }

        mappingCheck(mapping.LedgerConfiguration, message, object);

    }),

        itShouldConvert('GetLedgerConfigurationResponse', () => {

            const maxTtl = new Duration();
            maxTtl.setSeconds(20);
            maxTtl.setNanos(21);

            const minTtl = new Duration();
            minTtl.setSeconds(22);
            minTtl.setNanos(23);

            const ledgerConfiguration = new grpc.LedgerConfiguration();
            ledgerConfiguration.setMaxTtl(maxTtl);
            ledgerConfiguration.setMinTtl(minTtl);

            const message = new grpc.GetLedgerConfigurationResponse();
            message.setLedgerConfiguration(ledgerConfiguration);

            const object: ledger.GetLedgerConfigurationResponse = {
                config: {
                    maxTtl: {
                        seconds: 20,
                        nanoseconds: 21
                    },
                    minTtl: {
                        seconds: 22,
                        nanoseconds: 23
                    }
                }
            }
            mappingCheck(mapping.GetLedgerConfigurationResponse, message, object);

        });

    itShouldConvert('GetLedgerEndResponse', () => {
        const message = new grpc.GetLedgerEndResponse();
        const offset = new grpc.LedgerOffset();
        offset.setAbsolute('47');
        message.setOffset(offset);
        const object: ledger.GetLedgerEndResponse = {
            offset: {
                absolute: '47'
            }
        }
        mappingCheck(mapping.GetLedgerEndResponse, message, object);
    });

    itShouldConvert('GetTransactionByEventIdRequest', () => {
        const message = new grpc.GetTransactionByEventIdRequest();
        message.setEventId('some-id');
        message.setRequestingPartiesList(['birthday', 'pool', 'house-warming']);
        const object: ledger.GetTransactionByEventIdRequest = {
            eventId: 'some-id',
            requestingParties: ['birthday', 'pool', 'house-warming']
        }
        mappingCheck(mapping.GetTransactionByEventIdRequest, message, object);
    });

    itShouldConvert('GetTransactionByIdRequest', () => {
        const message = new grpc.GetTransactionByIdRequest();
        message.setTransactionId('some-id');
        message.setRequestingPartiesList(['birthday', 'pool', 'house-warming']);
        const object: ledger.GetTransactionByIdRequest = {
            transactionId: 'some-id',
            requestingParties: ['birthday', 'pool', 'house-warming']
        }
        mappingCheck(mapping.GetTransactionByIdRequest, message, object);
    });

    itShouldConvert('ArchivedEvent', () => {
        const message = new grpc.ArchivedEvent();
        const templateId = new grpc.Identifier();
        templateId.setPackageId('pkg');
        templateId.setName('alejandro');
        message.setTemplateId(templateId);
        message.setContractId('some-contract-id');
        message.setEventId('some-event-id');
        message.setWitnessPartiesList(['birthday', 'pool', 'house-warming']);
        const object: ledger.ArchivedEvent = {
            contractId: 'some-contract-id',
            eventId: 'some-event-id',
            templateId: { packageId: 'pkg', name: 'alejandro' },
            witnessParties: ['birthday', 'pool', 'house-warming']
        }
        mappingCheck(mapping.ArchivedEvent, message, object);
    });

    itShouldConvert('ExercisedEvent', () => {
        const message = new grpc.ExercisedEvent();
        const templateId = new grpc.Identifier();
        templateId.setPackageId('pkg');
        templateId.setName('alejandro');
        message.setTemplateId(templateId);
        message.setContractId('some-contract-id');
        message.setEventId('some-event-id');
        message.setActingPartiesList(['birthday']);
        message.setWitnessPartiesList(['house-warming']);
        const argument = new grpc.Value();
        const list = new grpc.List();
        const party = new grpc.Value();
        party.setParty('patricians');
        list.setElementsList([party]);
        argument.setList(list);
        message.setChoiceArgument(argument);
        message.setChoice('freedom');
        message.setConsuming(true);
        message.setContractCreatingEventId('father');
        message.setChildEventIdsList(['event']);

        const object: ledger.ExercisedEvent = {
            contractId: 'some-contract-id',
            eventId: 'some-event-id',
            templateId: { packageId: 'pkg', name: 'alejandro' },
            actingParties: ['birthday'],
            argument: { list: [{ party: 'patricians' }] },
            childEventIds: ['event'],
            choice: 'freedom',
            consuming: true,
            contractCreatingEventId: 'father',
            witnessParties: ['house-warming']
        }

        mappingCheck(mapping.ExercisedEvent, message, object);
    });

    itShouldConvert('Event', () => {

        const templateId = new grpc.Identifier();
        templateId.setPackageId('pkg');
        templateId.setName('alejandro');
        const event = new grpc.Event();
        const archived = new grpc.ArchivedEvent();
        const archivedTemplateId = new grpc.Identifier();
        archivedTemplateId.setPackageId('pkg');
        archivedTemplateId.setName('alejandro');
        archived.setTemplateId(templateId);
        archived.setContractId('some-contract-id');
        archived.setEventId('some-event-id');
        archived.setWitnessPartiesList(['pool']);
        event.setArchived(archived);

        const object = {
            archived: {
                contractId: 'some-contract-id',
                eventId: 'some-event-id',
                templateId: { packageId: 'pkg', name: 'alejandro' },
                witnessParties: ['pool']
            }
        };

        mappingCheck(mapping.Event, event, object);

    });

    itShouldConvert('TransactionTree', () => {

        const templateId = new grpc.Identifier();
        templateId.setPackageId('pkg');
        templateId.setName('alejandro');
        const event = new grpc.TreeEvent();
        const created = new grpc.CreatedEvent()
        const createdTemplateId = new grpc.Identifier();
        const createdArgs = new grpc.Record();
        const createdArgsField = new grpc.RecordField();
        const createdArgsFieldValue = new grpc.Value();
        createdArgsFieldValue.setContractId('boo');
        createdArgsField.setLabel('foo');
        createdArgsField.setValue(createdArgsFieldValue);
        createdArgs.setFieldsList([createdArgsField]);
        createdTemplateId.setPackageId('pkg');
        createdTemplateId.setName('alejandro');
        created.setTemplateId(templateId);
        created.setContractId('some-contract-id');
        created.setEventId('some-event-id');
        created.setWitnessPartiesList(['pool']);
        created.setCreateArguments(createdArgs);
        event.setCreated(created);

        const effectiveAt = new Timestamp();
        effectiveAt.setSeconds(10);
        effectiveAt.setNanos(20);
        const message = new grpc.TransactionTree();
        message.getEventsByIdMap().set('someId', event);
        message.addRootEventIds('root');
        message.setCommandId('befehl');
        message.setEffectiveAt(effectiveAt);
        message.setOffset('zero');
        message.setTransactionId('tx');
        message.setWorkflowId('workflow');
        message.getEventsByIdMap().set('someId', event);

        const object: ledger.TransactionTree = {
            commandId: 'befehl',
            effectiveAt: { seconds: 10, nanoseconds: 20 },
            eventsById: {
                someId: {
                    created: {
                        templateId: {
                            name: 'alejandro',
                            packageId: 'pkg'
                        },
                        contractId: 'some-contract-id',
                        eventId: 'some-event-id',
                        witnessParties: ['pool'],
                        arguments: {
                            fields: {
                                foo: { contractId: 'boo' }
                            }
                        }
                    }
                }
            },
            rootEventIds: ['root'],
            offset: 'zero',
            transactionId: 'tx',
            workflowId: 'workflow'
        }

        mappingCheck(mapping.TransactionTree, message, object);

    });

    itShouldConvert('GetTransactionResponse', () => {

        const templateId = new grpc.Identifier();
        templateId.setPackageId('pkg');
        templateId.setName('alejandro');
        const event = new grpc.TreeEvent();
        const created = new grpc.CreatedEvent()
        const createdTemplateId = new grpc.Identifier();
        const createdArgs = new grpc.Record();
        const createdArgsField = new grpc.RecordField();
        const createdArgsFieldValue = new grpc.Value();
        createdArgsFieldValue.setContractId('boo');
        createdArgsField.setLabel('foo');
        createdArgsField.setValue(createdArgsFieldValue);
        createdArgs.setFieldsList([createdArgsField]);
        createdTemplateId.setPackageId('pkg');
        createdTemplateId.setName('alejandro');
        created.setTemplateId(templateId);
        created.setContractId('some-contract-id');
        created.setEventId('some-event-id');
        created.setWitnessPartiesList(['pool']);
        created.setCreateArguments(createdArgs);
        event.setCreated(created);

        const effectiveAt = new Timestamp();
        effectiveAt.setSeconds(10);
        effectiveAt.setNanos(20);
        const transaction = new grpc.TransactionTree();
        transaction.getEventsByIdMap().set('someId', event);
        transaction.addRootEventIds('root');
        transaction.setCommandId('befehl');
        transaction.setEffectiveAt(effectiveAt);
        transaction.setOffset('zero');
        transaction.setTransactionId('tx');
        transaction.setWorkflowId('workflow');
        transaction.getEventsByIdMap().set('someId', event);

        const message = new grpc.GetTransactionResponse()
        message.setTransaction(transaction);

        const object: ledger.GetTransactionResponse = {
            transaction: {
                commandId: 'befehl',
                effectiveAt: { seconds: 10, nanoseconds: 20 },
                eventsById: {
                    someId: {
                        created: {
                            templateId: {
                                name: 'alejandro',
                                packageId: 'pkg'
                            },
                            contractId: 'some-contract-id',
                            eventId: 'some-event-id',
                            witnessParties: ['pool'],
                            arguments: {
                                fields: {
                                    foo: { contractId: 'boo' }
                                }
                            }
                        }
                    }
                },
                rootEventIds: ['root'],
                offset: 'zero',
                transactionId: 'tx',
                workflowId: 'workflow'
            }
        };

        mappingCheck(mapping.GetTransactionResponse, message, object);

    });

    itShouldConvert('Transaction', () => {

        const templateId = new grpc.Identifier();
        templateId.setPackageId('pkg');
        templateId.setName('alejandro');
        const event = new grpc.Event();
        const archived = new grpc.ArchivedEvent();
        const archivedTemplateId = new grpc.Identifier();
        archivedTemplateId.setPackageId('pkg');
        archivedTemplateId.setName('alejandro');
        archived.setTemplateId(templateId);
        archived.setContractId('some-contract-id');
        archived.setEventId('some-event-id');
        archived.setWitnessPartiesList(['pool']);
        event.setArchived(archived);

        const effectiveAt = new Timestamp();
        effectiveAt.setSeconds(10);
        effectiveAt.setNanos(20);
        const message = new grpc.Transaction();
        message.setEventsList([event]);
        message.setCommandId('befehl');
        message.setEffectiveAt(effectiveAt);
        message.setOffset('zero');
        message.setTransactionId('tx');
        message.setWorkflowId('workflow');

        const object: ledger.Transaction = {
            commandId: 'befehl',
            effectiveAt: { seconds: 10, nanoseconds: 20 },
            events: [{
                archived: {
                    contractId: 'some-contract-id',
                    eventId: 'some-event-id',
                    templateId: { packageId: 'pkg', name: 'alejandro' },
                    witnessParties: ['pool']
                }
            }],
            offset: 'zero',
            transactionId: 'tx',
            workflowId: 'workflow'
        }

        mappingCheck(mapping.Transaction, message, object);

    });

    itShouldConvert('GetTransactionsResponse', () => {

        const templateId = new grpc.Identifier();
        templateId.setPackageId('pkg');
        templateId.setName('alejandro');
        const event = new grpc.Event();
        const archived = new grpc.ArchivedEvent();
        const archivedTemplateId = new grpc.Identifier();
        archivedTemplateId.setPackageId('pkg');
        archivedTemplateId.setName('alejandro');
        archived.setTemplateId(templateId);
        archived.setContractId('some-contract-id');
        archived.setEventId('some-event-id');
        archived.setWitnessPartiesList(['pool']);
        event.setArchived(archived);

        const effectiveAt = new Timestamp();
        effectiveAt.setSeconds(10);
        effectiveAt.setNanos(20);
        const transaction = new grpc.Transaction();
        transaction.setEventsList([event]);
        transaction.setCommandId('befehl');
        transaction.setEffectiveAt(effectiveAt);
        transaction.setOffset('zero');
        transaction.setTransactionId('tx');
        transaction.setWorkflowId('workflow');

        const message = new grpc.GetTransactionsResponse()
        message.setTransactionsList([transaction]);

        const object: ledger.GetTransactionsResponse = {
            transactions: [{
                commandId: 'befehl',
                effectiveAt: { seconds: 10, nanoseconds: 20 },
                events: [{
                    archived: {
                        contractId: 'some-contract-id',
                        eventId: 'some-event-id',
                        templateId: { packageId: 'pkg', name: 'alejandro' },
                        witnessParties: ['pool']
                    }
                }],
                offset: 'zero',
                transactionId: 'tx',
                workflowId: 'workflow'
            }]
        };

        mappingCheck(mapping.GetTransactionsResponse, message, object);

    });

    itShouldConvert('GetTransactionTreesResponse', () => {

        const templateId = new grpc.Identifier();
        templateId.setPackageId('pkg');
        templateId.setName('alejandro');
        const event = new grpc.TreeEvent();
        const created = new grpc.CreatedEvent()
        const createdTemplateId = new grpc.Identifier();
        const createdArgs = new grpc.Record();
        const createdArgsField = new grpc.RecordField();
        const createdArgsFieldValue = new grpc.Value();
        createdArgsFieldValue.setContractId('boo');
        createdArgsField.setLabel('foo');
        createdArgsField.setValue(createdArgsFieldValue);
        createdArgs.setFieldsList([createdArgsField]);
        createdTemplateId.setPackageId('pkg');
        createdTemplateId.setName('alejandro');
        created.setTemplateId(templateId);
        created.setContractId('some-contract-id');
        created.setEventId('some-event-id');
        created.setWitnessPartiesList(['pool']);
        created.setCreateArguments(createdArgs);
        event.setCreated(created);

        const effectiveAt = new Timestamp();
        effectiveAt.setSeconds(10);
        effectiveAt.setNanos(20);
        const transaction = new grpc.TransactionTree();
        transaction.getEventsByIdMap().set('someId', event);
        transaction.addRootEventIds('root');
        transaction.setCommandId('befehl');
        transaction.setEffectiveAt(effectiveAt);
        transaction.setOffset('zero');
        transaction.setTransactionId('tx');
        transaction.setWorkflowId('workflow');
        transaction.getEventsByIdMap().set('someId', event);

        const message = new grpc.GetTransactionTreesResponse()
        message.setTransactionsList([transaction]);

        const object: ledger.GetTransactionTreesResponse = {
            transactions: [{
                commandId: 'befehl',
                effectiveAt: { seconds: 10, nanoseconds: 20 },
                eventsById: {
                    someId: {
                        created: {
                            templateId: {
                                name: 'alejandro',
                                packageId: 'pkg'
                            },
                            contractId: 'some-contract-id',
                            eventId: 'some-event-id',
                            witnessParties: ['pool'],
                            arguments: {
                                fields: {
                                    foo: { contractId: 'boo' }
                                }
                            }
                        }
                    }
                },
                rootEventIds: ['root'],
                offset: 'zero',
                transactionId: 'tx',
                workflowId: 'workflow'
            }]
        };

        mappingCheck(mapping.GetTransactionTreesResponse, message, object);

    });

    itShouldConvert('GetTimeResponse', () => {

        const timestamp = new Timestamp();
        timestamp.setSeconds(20);
        timestamp.setNanos(21);

        const message = new grpc.testing.GetTimeResponse();
        message.setCurrentTime(timestamp);

        const object: ledger.GetTimeResponse = {
            currentTime: {
                seconds: 20,
                nanoseconds: 21
            }
        };

        mappingCheck(mapping.GetTimeResponse, message, object);

    });

    itShouldConvert('SetTimeRequest', () => {

        const currentTime = new Timestamp();
        currentTime.setSeconds(20);
        currentTime.setNanos(21);

        const newTime = new Timestamp();
        newTime.setSeconds(42);
        newTime.setNanos(47);

        const message = new grpc.testing.SetTimeRequest();
        message.setCurrentTime(currentTime);
        message.setNewTime(newTime);

        const object: ledger.SetTimeRequest = {
            currentTime: {
                seconds: 20,
                nanoseconds: 21
            },
            newTime: {
                seconds: 42,
                nanoseconds: 47
            }
        };

        mappingCheck(mapping.SetTimeRequest, message, object);

    });

    itShouldConvert('Optional', () => {

        const message = new grpc.Optional();
        const value = new grpc.Value();
        const none = new grpc.Optional();
        value.setOptional(none);
        message.setValue(value);

        const object: ledger.Optional = {
            value: { optional: {} }
        };

        mappingCheck(mapping.Optional, message, object);

    });

});

function itShouldConvert(typeName: String, fn?: Mocha.Func): Mocha.Test {
    return it(`should convert model.${typeName} to/from grpc.${typeName}`, fn);
}

function mappingCheck<M, O>(mapping: mapping.Mapping<M, O>, m: M, o: O): void {
    expect(mapping.toObject(m)).to.deep.equal(o);
    expect(mapping.toMessage(o)).to.deep.equal(m);
}