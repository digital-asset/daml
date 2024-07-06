// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.EventOuterClass;
import com.google.protobuf.ByteString;
import com.google.protobuf.StringValue;
import com.google.rpc.Status;
import java.time.Instant;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class CreatedEvent implements Event, TreeEvent {

  private final @NonNull List<@NonNull String> witnessParties;

  private final String eventId;

  private final Identifier templateId;

  private final Optional<String> packageName;

  private final String contractId;

  private final DamlRecord arguments;

  private final @NonNull Map<@NonNull Identifier, @NonNull DamlRecord> interfaceViews;

  private final @NonNull Map<@NonNull Identifier, @NonNull Status> failedInterfaceViews;

  private final Optional<String> agreementText;

  private final Optional<Value> contractKey;

  private final @NonNull Set<@NonNull String> signatories;

  private final @NonNull Set<@NonNull String> observers;

  private final @NonNull ByteString createdEventBlob;

  // Note that we can't use a `com.daml.ledger.javaapi.data.Timestamp` here because
  // it only supports microseconds-precision and we require lossless conversions through
  // from/toProto.
  public final @NonNull Instant createdAt;

  public CreatedEvent(
      @NonNull List<@NonNull String> witnessParties,
      @NonNull String eventId,
      @NonNull Identifier templateId,
      @NonNull Optional<String> packageName,
      @NonNull String contractId,
      @NonNull DamlRecord arguments,
      @NonNull ByteString createdEventBlob,
      @NonNull Map<@NonNull Identifier, @NonNull DamlRecord> interfaceViews,
      @NonNull Map<@NonNull Identifier, com.google.rpc.@NonNull Status> failedInterfaceViews,
      @NonNull Optional<String> agreementText,
      @NonNull Optional<Value> contractKey,
      @NonNull Collection<@NonNull String> signatories,
      @NonNull Collection<@NonNull String> observers,
      @NonNull Instant createdAt) {
    this.witnessParties = List.copyOf(witnessParties);
    this.eventId = eventId;
    this.templateId = templateId;
    this.packageName = packageName;
    this.contractId = contractId;
    this.arguments = arguments;
    this.createdEventBlob = createdEventBlob;
    this.interfaceViews = Map.copyOf(interfaceViews);
    this.failedInterfaceViews = Map.copyOf(failedInterfaceViews);
    this.agreementText = agreementText;
    this.contractKey = contractKey;
    this.signatories = Set.copyOf(signatories);
    this.observers = Set.copyOf(observers);
    this.createdAt = createdAt;
  }

  /**
   * @deprecated You should pass {@code createArgumentsBlob} and {@code contractMetadata} arguments
   *     as well. Since Daml 2.6.0
   */
  @Deprecated
  public CreatedEvent(
      @NonNull List<@NonNull String> witnessParties,
      @NonNull String eventId,
      @NonNull Identifier templateId,
      @NonNull String contractId,
      @NonNull DamlRecord arguments,
      @NonNull Map<@NonNull Identifier, @NonNull DamlRecord> interfaceViews,
      @NonNull Map<@NonNull Identifier, com.google.rpc.@NonNull Status> failedInterfaceViews,
      @NonNull Optional<String> agreementText,
      @NonNull Optional<Value> contractKey,
      @NonNull Collection<@NonNull String> signatories,
      @NonNull Collection<@NonNull String> observers) {
    this(
        witnessParties,
        eventId,
        templateId,
        Optional.empty(),
        contractId,
        arguments,
        ByteString.EMPTY,
        interfaceViews,
        failedInterfaceViews,
        agreementText,
        contractKey,
        signatories,
        observers,
        Instant.EPOCH);
  }

  /**
   * @deprecated Pass {@code interfaceViews} and {@code failedInterfaceViews} arguments; empty maps
   *     are reasonable defaults. Since Daml 2.4.0
   */
  @Deprecated
  public CreatedEvent(
      @NonNull List<@NonNull String> witnessParties,
      @NonNull String eventId,
      @NonNull Identifier templateId,
      @NonNull String contractId,
      @NonNull DamlRecord arguments,
      @NonNull Optional<String> agreementText,
      @NonNull Optional<Value> contractKey,
      @NonNull Collection<@NonNull String> signatories,
      @NonNull Collection<@NonNull String> observers) {
    this(
        witnessParties,
        eventId,
        templateId,
        contractId,
        arguments,
        Collections.emptyMap(),
        Collections.emptyMap(),
        agreementText,
        contractKey,
        signatories,
        observers);
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
  public Optional<String> getPackageName() {
    return packageName;
}

  @NonNull
  @Override
  public String getContractId() {
    return contractId;
  }

  @NonNull
  public DamlRecord getArguments() {
    return arguments;
  }

  public ByteString getCreatedEventBlob() {
    return createdEventBlob;
  }

  @NonNull
  public Map<@NonNull Identifier, @NonNull DamlRecord> getInterfaceViews() {
    return interfaceViews;
  }

  @NonNull
  public Map<@NonNull Identifier, @NonNull Status> getFailedInterfaceViews() {
    return failedInterfaceViews;
  }

  @NonNull
  public Optional<String> getAgreementText() {
    return agreementText;
  }

  @NonNull
  public Optional<Value> getContractKey() {
    return contractKey;
  }

  @NonNull
  public Set<@NonNull String> getSignatories() {
    return signatories;
  }

  @NonNull
  public Set<@NonNull String> getObservers() {
    return observers;
  }

  /**
   * {@code createdAt} has been introduced in the Ledger API {@link
   * com.daml.ledger.api.v1.EventOuterClass.CreatedEvent} starting with Canton version 2.8.0. Events
   * sourced from the Ledger API prior to this version will return the default {@link Instant#EPOCH}
   * value.
   */
  @NonNull
  public Instant getCreatedAt() {
    return createdAt;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CreatedEvent that = (CreatedEvent) o;
    return Objects.equals(witnessParties, that.witnessParties)
        && Objects.equals(eventId, that.eventId)
        && Objects.equals(templateId, that.templateId)
        && Objects.equals(contractId, that.contractId)
        && Objects.equals(arguments, that.arguments)
        && Objects.equals(createdEventBlob, that.createdEventBlob)
        && Objects.equals(interfaceViews, that.interfaceViews)
        && Objects.equals(failedInterfaceViews, that.failedInterfaceViews)
        && Objects.equals(agreementText, that.agreementText)
        && Objects.equals(contractKey, that.contractKey)
        && Objects.equals(signatories, that.signatories)
        && Objects.equals(observers, that.observers)
        && Objects.equals(createdAt, that.createdAt)
        && Objects.equals(packageName, that.packageName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        witnessParties,
        eventId,
        templateId,
        contractId,
        arguments,
        createdEventBlob,
        interfaceViews,
        failedInterfaceViews,
        agreementText,
        contractKey,
        signatories,
        observers,
        createdAt,
        packageName);
  }

  @Override
  public String toString() {
    return "CreatedEvent{"
        + "witnessParties="
        + witnessParties
        + ", eventId='"
        + eventId
        + '\''
        + ", templateId="
        + templateId
        + ", contractId='"
        + contractId
        + '\''
        + ", arguments="
        + arguments
        + ", createdEventBlob="
        + createdEventBlob
        + ", interfaceViews="
        + interfaceViews
        + ", failedInterfaceViews="
        + failedInterfaceViews
        + ", agreementText='"
        + agreementText
        + "', contractKey="
        + contractKey
        + ", signatories="
        + signatories
        + ", observers="
        + observers
        + ", createdAt="
        + createdAt
        + ", packageName="
        + packageName
        + '}';
  }

  @SuppressWarnings("deprecation")
  public EventOuterClass.@NonNull CreatedEvent toProto() {
    EventOuterClass.CreatedEvent.Builder builder =
        EventOuterClass.CreatedEvent.newBuilder()
            .setContractId(this.getContractId())
            .setCreateArguments(this.getArguments().toProtoRecord())
            .setCreatedEventBlob(createdEventBlob)
            .addAllInterfaceViews(
                Stream.concat(
                        toProtoInterfaceViews(
                            interfaceViews, (b, dr) -> b.setViewValue(dr.toProtoRecord())),
                        toProtoInterfaceViews(
                            failedInterfaceViews, (b, status) -> b.setViewStatus(status)))
                    .collect(Collectors.toUnmodifiableList()))
            .setEventId(this.getEventId())
            .setTemplateId(this.getTemplateId().toProto())
            .addAllWitnessParties(this.getWitnessParties())
            .addAllSignatories(this.getSignatories())
            .addAllObservers(this.getObservers())
            .setCreatedAt(
                com.google.protobuf.Timestamp.newBuilder()
                    .setSeconds(this.createdAt.getEpochSecond())
                    .setNanos(this.createdAt.getNano())
                    .build());
    agreementText.ifPresent(a -> builder.setAgreementText(StringValue.of(a)));
    contractKey.ifPresent(a -> builder.setContractKey(a.toProto()));
    packageName.ifPresent(a -> builder.setPackageName(StringValue.of(a)));
    return builder.build();
  }

  private static <V> Stream<EventOuterClass.InterfaceView> toProtoInterfaceViews(
      Map<Identifier, V> views,
      BiFunction<EventOuterClass.InterfaceView.Builder, V, EventOuterClass.InterfaceView.Builder>
          addV) {
    return views.entrySet().stream()
        .map(
            e ->
                addV.apply(
                        EventOuterClass.InterfaceView.newBuilder()
                            .setInterfaceId(e.getKey().toProto()),
                        e.getValue())
                    .build());
  }

  @SuppressWarnings("deprecation")
  public static CreatedEvent fromProto(EventOuterClass.CreatedEvent createdEvent) {
    var splitInterfaceViews =
        createdEvent.getInterfaceViewsList().stream()
            .collect(Collectors.partitioningBy(EventOuterClass.InterfaceView::hasViewValue));
    return new CreatedEvent(
        createdEvent.getWitnessPartiesList(),
        createdEvent.getEventId(),
        Identifier.fromProto(createdEvent.getTemplateId()),
        createdEvent.hasPackageName()
                ? Optional.of(createdEvent.getPackageName().getValue())
                : Optional.empty(),
        createdEvent.getContractId(),
        DamlRecord.fromProto(createdEvent.getCreateArguments()),
        createdEvent.getCreatedEventBlob(),
        splitInterfaceViews.get(true).stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    iv -> Identifier.fromProto(iv.getInterfaceId()),
                    iv -> DamlRecord.fromProto(iv.getViewValue()))),
        splitInterfaceViews.get(false).stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    iv -> Identifier.fromProto(iv.getInterfaceId()),
                    EventOuterClass.InterfaceView::getViewStatus)),
        createdEvent.hasAgreementText()
            ? Optional.of(createdEvent.getAgreementText().getValue())
            : Optional.empty(),
        createdEvent.hasContractKey()
            ? Optional.of(Value.fromProto(createdEvent.getContractKey()))
            : Optional.empty(),
        createdEvent.getSignatoriesList(),
        createdEvent.getObserversList(),
        Instant.ofEpochSecond(
            createdEvent.getCreatedAt().getSeconds(), createdEvent.getCreatedAt().getNanos()));
  }
}
