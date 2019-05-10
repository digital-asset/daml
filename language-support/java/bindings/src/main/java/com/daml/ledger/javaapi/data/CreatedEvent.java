// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.digitalasset.ledger.api.v1.EventOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Objects;

public final class CreatedEvent implements Event, TreeEvent {

    private final @NonNull List<@NonNull String> witnessParties;

    private final String eventId;

    private final Identifier templateId;

    private final String contractId;

    private final Record arguments;

    public CreatedEvent(@NonNull List<@NonNull String> witnessParties, @NonNull String eventId, @NonNull Identifier templateId, @NonNull String contractId, @NonNull Record arguments) {
        this.witnessParties = witnessParties;
        this.eventId = eventId;
        this.templateId = templateId;
        this.contractId = contractId;
        this.arguments = arguments;
    }

    @NonNull
    @Override
    public List<@NonNull String> getWitnessParties() {
        return witnessParties;
    }

    @NonNull
    @Override
    public String getEventId() {
        return eventId;
    }

    @NonNull
    @Override
    public Identifier getTemplateId() {
        return templateId;
    }

    @NonNull
    @Override
    public String getContractId() {
        return contractId;
    }

    @NonNull
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

    public EventOuterClass.@NonNull CreatedEvent toProto() {
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
