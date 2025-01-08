// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.CommandsOuterClass;
import com.daml.ledger.javaapi.data.codegen.HasCommands;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.Optional.empty;

/**
 * This class can be used to build a valid submission. It provides {@link #create(String, String, String, List)}
 * for initial creation and methods to set optional parameters
 * e.g {@link #withActAs(List)}, {@link #withWorkflowId(String)} etc.
 *
 * Usage:
 * <pre>
 *   var submission = CommandsSubmission.create(applicationId, commandId, synchronizerId, commands)
 *                                   .withAccessToken(token)
 *                                   .withWorkflowId(workflowId)
 *                                   .with...
 * <pre/>
 */
public final class CommandsSubmission {

  @NonNull private final Optional<String> workflowId;

  @NonNull private final String applicationId;
  @NonNull private final String commandId;
  @NonNull private final List<@NonNull ? extends HasCommands> commands;
  @NonNull private final Optional<Duration> deduplicationDuration;
  @NonNull private final Optional<Long> deduplicationOffset;
  @NonNull private final Optional<Instant> minLedgerTimeAbs;
  @NonNull private final Optional<Duration> minLedgerTimeRel;
  @NonNull private final List<@NonNull String> actAs;
  @NonNull private final List<@NonNull String> readAs;
  @NonNull private final Optional<String> submissionId;
  @NonNull private final List<DisclosedContract> disclosedContracts;
  @NonNull private final String synchronizerId;
  @NonNull private final Optional<String> accessToken;
  @NonNull private List<String> packageIdSelectionPreference;
  @NonNull private List<@NonNull PrefetchContractKey> prefetchContractKeys;

  protected CommandsSubmission(
      @NonNull Optional<String> workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull Optional<Duration> deduplicationDuration,
      @NonNull Optional<Long> deduplicationOffset,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Optional<String> submissionId,
      @NonNull List<@NonNull DisclosedContract> disclosedContracts,
      @NonNull String synchronizerId,
      @NonNull Optional<String> accessToken,
      @NonNull List<String> packageIdSelectionPreference,
      @NonNull List<@NonNull PrefetchContractKey> prefetchContractKeys) {
    this.workflowId = workflowId;
    this.applicationId = applicationId;
    this.commandId = commandId;
    this.commands = commands;
    this.deduplicationDuration = deduplicationDuration;
    this.deduplicationOffset = deduplicationOffset;
    this.minLedgerTimeAbs = minLedgerTimeAbs;
    this.minLedgerTimeRel = minLedgerTimeRel;
    this.actAs = actAs;
    this.readAs = readAs;
    this.submissionId = submissionId;
    this.disclosedContracts = disclosedContracts;
    this.synchronizerId = synchronizerId;
    this.accessToken = accessToken;
    this.packageIdSelectionPreference = packageIdSelectionPreference;
    this.prefetchContractKeys = prefetchContractKeys;
  }

  public static CommandsSubmission create(
      String applicationId,
      String commandId,
      String synchronizerId,
      @NonNull List<@NonNull ? extends HasCommands> commands) {
    return new CommandsSubmission(
        empty(),
        applicationId,
        commandId,
        commands,
        empty(),
        empty(),
        empty(),
        empty(),
        emptyList(),
        emptyList(),
        empty(),
        emptyList(),
        synchronizerId,
        empty(),
        emptyList(),
        emptyList());
  }

  @NonNull
  public Optional<String> getWorkflowId() {
    return workflowId;
  }

  @NonNull
  public List<String> getPackageIdSelectionPreference() {
    return Collections.unmodifiableList(packageIdSelectionPreference);
  }

  @NonNull
  public String getApplicationId() {
    return applicationId;
  }

  @NonNull
  public String getCommandId() {
    return commandId;
  }

  @NonNull
  public List<? extends HasCommands> getCommands() {
    return unmodifiableList(commands);
  }

  @NonNull
  public Optional<Duration> getDeduplicationDuration() {
    return deduplicationDuration;
  }

  @NonNull
  public Optional<Long> getDeduplicationOffset() {
    return deduplicationOffset;
  }

  @NonNull
  public Optional<Instant> getMinLedgerTimeAbs() {
    return minLedgerTimeAbs;
  }

  @NonNull
  public Optional<Duration> getMinLedgerTimeRel() {
    return minLedgerTimeRel;
  }

  @NonNull
  public List<String> getActAs() {
    return unmodifiableList(actAs);
  }

  @NonNull
  public List<String> getReadAs() {
    return unmodifiableList(readAs);
  }

  @NonNull
  public Optional<String> getSubmissionId() {
    return submissionId;
  }

  @NonNull
  public List<DisclosedContract> getDisclosedContracts() {
    return unmodifiableList(disclosedContracts);
  }

  @NonNull
  public String getSynchronizerId() {
    return synchronizerId;
  }

  @NonNull
  public Optional<String> getAccessToken() {
    return accessToken;
  }

  public List<PrefetchContractKey> getPrefetchContractKeys() {
    return unmodifiableList(prefetchContractKeys);
  }

  public CommandsSubmission withWorkflowId(String workflowId) {
    return new CommandsSubmission(
        Optional.of(workflowId),
        applicationId,
        commandId,
        commands,
        deduplicationDuration,
        deduplicationOffset,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        actAs,
        readAs,
        submissionId,
        disclosedContracts,
        synchronizerId,
        accessToken,
        packageIdSelectionPreference,
        prefetchContractKeys);
  }

  public CommandsSubmission withActAs(String actAs) {
    return new CommandsSubmission(
        workflowId,
        applicationId,
        commandId,
        commands,
        deduplicationDuration,
        deduplicationOffset,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        List.of(actAs),
        readAs,
        submissionId,
        disclosedContracts,
        synchronizerId,
        accessToken,
        packageIdSelectionPreference,
        prefetchContractKeys);
  }

  public CommandsSubmission withActAs(List<@NonNull String> actAs) {
    return new CommandsSubmission(
        workflowId,
        applicationId,
        commandId,
        commands,
        deduplicationDuration,
        deduplicationOffset,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        actAs,
        readAs,
        submissionId,
        disclosedContracts,
        synchronizerId,
        accessToken,
        packageIdSelectionPreference,
        prefetchContractKeys);
  }

  public CommandsSubmission withReadAs(List<@NonNull String> readAs) {
    return new CommandsSubmission(
        workflowId,
        applicationId,
        commandId,
        commands,
        deduplicationDuration,
        deduplicationOffset,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        actAs,
        readAs,
        submissionId,
        disclosedContracts,
        synchronizerId,
        accessToken,
        packageIdSelectionPreference,
        prefetchContractKeys);
  }

  public CommandsSubmission withMinLedgerTimeAbs(@NonNull Instant minLedgerTimeAbs) {
    return new CommandsSubmission(
        workflowId,
        applicationId,
        commandId,
        commands,
        deduplicationDuration,
        deduplicationOffset,
        Optional.of(minLedgerTimeAbs),
        minLedgerTimeRel,
        actAs,
        readAs,
        submissionId,
        disclosedContracts,
        synchronizerId,
        accessToken,
        packageIdSelectionPreference,
        prefetchContractKeys);
  }

  public CommandsSubmission withMinLedgerTimeRel(@NonNull Duration minLedgerTimeRel) {
    return new CommandsSubmission(
        workflowId,
        applicationId,
        commandId,
        commands,
        deduplicationDuration,
        deduplicationOffset,
        minLedgerTimeAbs,
        Optional.of(minLedgerTimeRel),
        actAs,
        readAs,
        submissionId,
        disclosedContracts,
        synchronizerId,
        accessToken,
        packageIdSelectionPreference,
        prefetchContractKeys);
  }

  public CommandsSubmission withDeduplicationDuration(@NonNull Duration deduplicationDuration)
      throws RedundantDeduplicationSpecification {
    deduplicationOffset.ifPresent(
        offset -> {
          throw new RedundantDeduplicationSpecification(deduplicationDuration, offset);
        });
    return new CommandsSubmission(
        workflowId,
        applicationId,
        commandId,
        commands,
        Optional.of(deduplicationDuration),
        deduplicationOffset,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        actAs,
        readAs,
        submissionId,
        disclosedContracts,
        synchronizerId,
        accessToken,
        packageIdSelectionPreference,
        prefetchContractKeys);
  }

  public CommandsSubmission withDeduplicationOffset(@NonNull Long deduplicationOffset)
      throws RedundantDeduplicationSpecification {
    deduplicationDuration.ifPresent(
        duration -> {
          throw new RedundantDeduplicationSpecification(duration, deduplicationOffset);
        });
    return new CommandsSubmission(
        workflowId,
        applicationId,
        commandId,
        commands,
        deduplicationDuration,
        Optional.of(deduplicationOffset),
        minLedgerTimeAbs,
        minLedgerTimeRel,
        actAs,
        readAs,
        submissionId,
        disclosedContracts,
        synchronizerId,
        accessToken,
        packageIdSelectionPreference,
        prefetchContractKeys);
  }

  public CommandsSubmission withCommands(List<@NonNull ? extends HasCommands> commands) {
    return new CommandsSubmission(
        workflowId,
        applicationId,
        commandId,
        commands,
        deduplicationDuration,
        deduplicationOffset,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        actAs,
        readAs,
        submissionId,
        disclosedContracts,
        synchronizerId,
        accessToken,
        packageIdSelectionPreference,
        prefetchContractKeys);
  }

  public CommandsSubmission withAccessToken(@NonNull String accessToken) {
    return new CommandsSubmission(
        workflowId,
        applicationId,
        commandId,
        commands,
        deduplicationDuration,
        deduplicationOffset,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        actAs,
        readAs,
        submissionId,
        disclosedContracts,
        synchronizerId,
        Optional.of(accessToken),
        packageIdSelectionPreference,
        prefetchContractKeys);
  }

  public CommandsSubmission withDisclosedContracts(
      List<@NonNull DisclosedContract> disclosedContracts) {
    return new CommandsSubmission(
        workflowId,
        applicationId,
        commandId,
        commands,
        deduplicationDuration,
        deduplicationOffset,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        actAs,
        readAs,
        submissionId,
        disclosedContracts,
        synchronizerId,
        accessToken,
        packageIdSelectionPreference,
        prefetchContractKeys);
  }

  public CommandsSubmission withPackageIdSelectionPreference(
      List<@NonNull String> packageIdSelectionPreference) {
    return new CommandsSubmission(
        workflowId,
        applicationId,
        commandId,
        commands,
        deduplicationDuration,
        deduplicationOffset,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        actAs,
        readAs,
        submissionId,
        disclosedContracts,
        synchronizerId,
        accessToken,
        packageIdSelectionPreference,
        prefetchContractKeys);
  }

  public CommandsSubmission withPrefetchContractKeys(
      @NonNull List<@NonNull PrefetchContractKey> prefetchContractKeys) {
    return new CommandsSubmission(
        workflowId,
        applicationId,
        commandId,
        commands,
        deduplicationDuration,
        deduplicationOffset,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        actAs,
        readAs,
        submissionId,
        disclosedContracts,
        synchronizerId,
        accessToken,
        packageIdSelectionPreference,
        prefetchContractKeys);
  }

  public CommandsOuterClass.Commands toProto() {
    if (actAs.isEmpty()) {
      throw new IllegalArgumentException("actAs must have at least one element");
    }

    List<com.daml.ledger.api.v2.CommandsOuterClass.Command> commandsConverted =
        HasCommands.toCommands(commands).stream()
            .map(Command::toProtoCommand)
            .collect(Collectors.toList());

    List<com.daml.ledger.api.v2.CommandsOuterClass.DisclosedContract> disclosedContractsConverted =
        disclosedContracts.stream().map(DisclosedContract::toProto).collect(Collectors.toList());

    CommandsOuterClass.Commands.Builder builder =
        CommandsOuterClass.Commands.newBuilder()
            .setApplicationId(applicationId)
            .setCommandId(commandId)
            .addAllCommands(commandsConverted)
            .addAllActAs(actAs)
            .addAllReadAs(readAs)
            .addAllDisclosedContracts(disclosedContractsConverted)
            .setSynchronizerId(synchronizerId)
            .addAllPackageIdSelectionPreference(packageIdSelectionPreference);

    workflowId.ifPresent(builder::setWorkflowId);

    deduplicationDuration.ifPresent(
        dedup -> builder.setDeduplicationDuration(Utils.durationToProto(dedup)));

    deduplicationOffset.ifPresent(builder::setDeduplicationOffset);

    minLedgerTimeAbs.ifPresent(abs -> builder.setMinLedgerTimeAbs(Utils.instantToProto(abs)));

    minLedgerTimeRel.ifPresent(rel -> builder.setMinLedgerTimeRel(Utils.durationToProto(rel)));

    submissionId.ifPresent(builder::setSubmissionId);

    return builder.build();
  }

  public static CommandsSubmission fromProto(CommandsOuterClass.Commands commands) {
    Optional<String> workflowId =
        commands.getWorkflowId().isEmpty()
            ? Optional.empty()
            : Optional.of(commands.getWorkflowId());
    String applicationId = commands.getApplicationId();
    String commandId = commands.getCommandId();

    List<? extends HasCommands> listOfCommands =
        commands.getCommandsList().stream()
            .map(Command::fromProtoCommand)
            .collect(Collectors.toList());

    Optional<Duration> deduplicationDuration =
        commands.hasDeduplicationDuration()
            ? Optional.of(Utils.durationFromProto(commands.getDeduplicationDuration()))
            : Optional.empty();
    Optional<Long> deduplicationOffset =
        commands.hasDeduplicationOffset()
            ? Optional.of(commands.getDeduplicationOffset())
            : Optional.empty();

    Optional<Instant> minLedgerTimeAbs =
        commands.hasMinLedgerTimeAbs()
            ? Optional.of(Utils.instantFromProto(commands.getMinLedgerTimeAbs()))
            : Optional.empty();
    Optional<Duration> minLedgerTimeRel =
        commands.hasMinLedgerTimeRel()
            ? Optional.of(Utils.durationFromProto(commands.getMinLedgerTimeRel()))
            : Optional.empty();

    List<String> actAs = commands.getActAsList();
    List<String> readAs = commands.getReadAsList();

    Optional<String> submissionId =
        commands.getSubmissionId().isEmpty()
            ? Optional.empty()
            : Optional.of(commands.getSubmissionId());

    List<DisclosedContract> disclosedContracts =
        commands.getDisclosedContractsList().stream()
            .map(DisclosedContract::fromProto)
            .collect(Collectors.toList());

    List<String> packageIdSelectionPreference = commands.getPackageIdSelectionPreferenceList();

    List<PrefetchContractKey> prefetchContractKeys =
        commands.getPrefetchContractKeysList().stream()
            .map(PrefetchContractKey::fromProto)
            .collect(Collectors.toList());

    return new CommandsSubmission(
        workflowId,
        applicationId,
        commandId,
        listOfCommands,
        deduplicationDuration,
        deduplicationOffset,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        actAs,
        readAs,
        submissionId,
        disclosedContracts,
        commands.getSynchronizerId(),
        empty(),
        packageIdSelectionPreference,
        prefetchContractKeys);
  }

  @Override
  public String toString() {
    return "CommandsSubmission{"
        + "workflowId='"
        + workflowId
        + '\''
        + ", applicationId='"
        + applicationId
        + '\''
        + ", commandId='"
        + commandId
        + '\''
        + ", commands="
        + commands
        + ", deduplicationDuration="
        + deduplicationDuration
        + ", deduplicationOffset="
        + deduplicationOffset
        + ", minLedgerTimeAbs="
        + minLedgerTimeAbs
        + ", minLedgerTimeRel="
        + minLedgerTimeRel
        + ", actAs="
        + actAs
        + ", readAs="
        + readAs
        + ", submissionId="
        + submissionId
        + ", disclosedContracts="
        + disclosedContracts
        + ", synchronizerId='"
        + synchronizerId
        + '\''
        + ", accessToken="
        + accessToken
        + '\''
        + ", packageIdSelectionPreference="
        + packageIdSelectionPreference
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CommandsSubmission commandsSubmission = (CommandsSubmission) o;
    return Objects.equals(workflowId, commandsSubmission.workflowId)
        && Objects.equals(applicationId, commandsSubmission.applicationId)
        && Objects.equals(commandId, commandsSubmission.commandId)
        && Objects.equals(commands, commandsSubmission.commands)
        && Objects.equals(deduplicationDuration, commandsSubmission.deduplicationDuration)
        && Objects.equals(deduplicationOffset, commandsSubmission.deduplicationOffset)
        && Objects.equals(minLedgerTimeAbs, commandsSubmission.minLedgerTimeAbs)
        && Objects.equals(minLedgerTimeRel, commandsSubmission.minLedgerTimeRel)
        && Objects.equals(actAs, commandsSubmission.actAs)
        && Objects.equals(readAs, commandsSubmission.readAs)
        && Objects.equals(submissionId, commandsSubmission.submissionId)
        && Objects.equals(disclosedContracts, commandsSubmission.disclosedContracts)
        && Objects.equals(synchronizerId, commandsSubmission.synchronizerId)
        && Objects.equals(accessToken, commandsSubmission.accessToken)
        && Objects.equals(
            packageIdSelectionPreference, commandsSubmission.packageIdSelectionPreference);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        workflowId,
        applicationId,
        commandId,
        commands,
        deduplicationDuration,
        deduplicationOffset,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        actAs,
        readAs,
        submissionId,
        disclosedContracts,
        synchronizerId,
        accessToken,
        packageIdSelectionPreference);
  }

  public static class RedundantDeduplicationSpecification extends RuntimeException {
    public RedundantDeduplicationSpecification(
        Duration deduplicationDuration, Long deduplicationOffset) {
      super(
          "Both a deduplicationDuration: "
              + deduplicationDuration.toString()
              + " and a deduplicationOffset: "
              + deduplicationOffset.toString()
              + " given");
    }
  }
}
