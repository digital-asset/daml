// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.digitalasset.ledger.api.v1.EventOuterClass;

import java.util.List;
import java.util.Objects;

public final class CreatedEvent extends Event {

    private final List<String> witnessParties;

    private final String eventId;

    private final Identifier templateId;

    private final String contractId;

    private final Record arguments;

    public CreatedEvent(List<String> witnessParties, String eventId, Identifier templateId, String contractId, Record arguments) {
        this.witnessParties = witnessParties;
        this.eventId = eventId;
        this.templateId = templateId;
        this.contractId = contractId;
        this.arguments = arguments;
    }


    @Override
    public List<String> getWitnessParties() {
        return witnessParties;
    }


    @Override
    public String getEventId() {
        return eventId;
    }


    @Override
    public Identifier getTemplateId() {
        return templateId;
    }


    @Override
    public String getContractId() {
        return contractId;
    }


    public Record getArguments() {
        return arguments;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreatedEvent that = (CreatedEvent) o;
        return Objects.equals(witnessParties, that.witnessParties) &&
                Objects.equals(eventId, that.eventId) &&
                Objects.equals(templateId, that.templateId) &&
                Objects.equals(contractId, that.contractId) &&
                Objects.equals(arguments, that.arguments);
    }

    @Override
    public int hashCode() {

        return Objects.hash(witnessParties, eventId, templateId, contractId, arguments);
    }

    @Override
    public String toString() {
        return "CreatedEvent{" +
                "witnessParties=" + witnessParties +
                ", eventId='" + eventId + '\'' +
                ", templateId=" + templateId +
                ", contractId='" + contractId + '\'' +
                ", arguments=" + arguments +
                '}';
    }

    public EventOuterClass.CreatedEvent toProto() {
        return EventOuterClass.CreatedEvent.newBuilder()
                .setContractId(getContractId())
                .setCreateArguments(getArguments().toProtoRecord())
                .setEventId(getEventId())
                .setTemplateId(getTemplateId().toProto())
                .addAllWitnessParties(this.getWitnessParties())
                .build();
    }

    public static CreatedEvent fromProto(EventOuterClass.CreatedEvent createdEvent) {
        return new CreatedEvent(
                createdEvent.getWitnessPartiesList(),
                createdEvent.getEventId(),
                Identifier.fromProto(createdEvent.getTemplateId()),
                createdEvent.getContractId(),
                Record.fromProto(createdEvent.getCreateArguments()));

    }
}
