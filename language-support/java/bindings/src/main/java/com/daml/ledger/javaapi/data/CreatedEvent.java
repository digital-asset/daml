// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.digitalasset.ledger.api.v1.EventOuterClass;
import com.google.protobuf.StringValue;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class CreatedEvent implements Event, TreeEvent {

    private final @NonNull List<@NonNull String> witnessParties;

    private final String eventId;

    private final Identifier templateId;

    private final String contractId;

    private final Record arguments;

    private final Optional<String> agreementText;

    private final Optional<Value> contractKey;

    public CreatedEvent(@NonNull List<@NonNull String> witnessParties, @NonNull String eventId, @NonNull Identifier templateId, @NonNull String contractId, @NonNull Record arguments, @NonNull Optional<String> agreementText, @NonNull Optional<Value> contractKey) {
        this.witnessParties = witnessParties;
        this.eventId = eventId;
        this.templateId = templateId;
        this.contractId = contractId;
        this.arguments = arguments;
        this.agreementText = agreementText;
        this.contractKey = contractKey;
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

    @NonNull
    public Optional<String> getAgreementText() {
        return agreementText;
    }

    @NonNull
    public Optional<Value> getContractKey() { return contractKey; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreatedEvent that = (CreatedEvent) o;
        return Objects.equals(witnessParties, that.witnessParties) &&
                Objects.equals(eventId, that.eventId) &&
                Objects.equals(templateId, that.templateId) &&
                Objects.equals(contractId, that.contractId) &&
                Objects.equals(arguments, that.arguments) &&
                Objects.equals(agreementText, that.agreementText) &&
                Objects.equals(contractKey, that.contractKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(witnessParties, eventId, templateId, contractId, arguments, agreementText, contractKey);
    }

    @Override
    public String toString() {
        return "CreatedEvent{" +
                "witnessParties=" + witnessParties +
                ", eventId='" + eventId + '\'' +
                ", templateId=" + templateId +
                ", contractId='" + contractId + '\'' +
                ", arguments=" + arguments +
                ", agreementText='" + agreementText + '\'' +
                ", contractKey=" + contractKey +
                '}';
    }

    public EventOuterClass.@NonNull CreatedEvent toProto() {
        EventOuterClass.CreatedEvent.Builder builder = EventOuterClass.CreatedEvent.newBuilder()
                .setContractId(getContractId())
                .setCreateArguments(getArguments().toProtoRecord())
                .setEventId(getEventId())
                .setTemplateId(getTemplateId().toProto())
                .addAllWitnessParties(this.getWitnessParties());
        agreementText.ifPresent(a -> builder.setAgreementText(StringValue.of(a)));
        contractKey.ifPresent(a -> builder.setContractKey(a.toProto()));
        return builder.build();
    }

    public static CreatedEvent fromProto(EventOuterClass.CreatedEvent createdEvent) {
        return new CreatedEvent(
                createdEvent.getWitnessPartiesList(),
                createdEvent.getEventId(),
                Identifier.fromProto(createdEvent.getTemplateId()),
                createdEvent.getContractId(),
                Record.fromProto(createdEvent.getCreateArguments()),
                createdEvent.hasAgreementText() ? Optional.of(createdEvent.getAgreementText().getValue()) : Optional.empty(),
                createdEvent.hasContractKey() ? Optional.of(Value.fromProto(createdEvent.getContractKey())) : Optional.empty());

    }
}
