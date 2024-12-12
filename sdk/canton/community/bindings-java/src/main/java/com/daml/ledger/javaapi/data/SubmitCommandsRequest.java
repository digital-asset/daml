// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import static com.daml.ledger.javaapi.data.codegen.HasCommands.toCommands;
import static java.util.Arrays.asList;

import com.daml.ledger.api.v1.CommandsOuterClass;
import com.google.protobuf.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class SubmitCommandsRequest {

  private final String workflowId;

  private final String applicationId;

  private final String commandId;

  private final String party;
  private final List<String> actAs;
  private final List<String> readAs;

  private final Optional<Instant> minLedgerTimeAbsolute;
  private final Optional<Duration> minLedgerTimeRelative;
  private final Optional<Duration> deduplicationTime;
  private final Optional<String> submissionId;
  private final List<Command> commands;
  private final List<DisclosedContract> disclosedContracts;
  private final List<String> packageIdSelectionPreference;
  private final List<PrefetchContractKey> prefetchContractKeys;

  private SubmitCommandsRequest(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Optional<Instant> minLedgerTimeAbsolute,
      @NonNull Optional<Duration> minLedgerTimeRelative,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull Optional<String> submissionId,
      @NonNull List<@NonNull Command> commands,
      @NonNull List<@NonNull DisclosedContract> disclosedContracts,
      @NonNull List<@NonNull String> packageIdSelectionPreference,
      @NonNull List<@NonNull PrefetchContractKey> prefetchContractKeys) {
    if (actAs.isEmpty()) {
      throw new IllegalArgumentException("actAs must have at least one element");
    }
    this.workflowId = workflowId;
    this.applicationId = applicationId;
    this.commandId = commandId;
    this.party = actAs.get(0);
    this.actAs = List.copyOf(actAs);
    this.readAs = List.copyOf(readAs);
    this.minLedgerTimeAbsolute = minLedgerTimeAbsolute;
    this.minLedgerTimeRelative = minLedgerTimeRelative;
    this.deduplicationTime = deduplicationTime;
    this.submissionId = submissionId;
    this.commands = commands;
    this.disclosedContracts = disclosedContracts;
    this.packageIdSelectionPreference = packageIdSelectionPreference;
    this.prefetchContractKeys = prefetchContractKeys;
  }

  public static SubmitCommandsRequest fromProto(CommandsOuterClass.Commands commands) {
    String workflowId = commands.getWorkflowId();
    String applicationId = commands.getApplicationId();
    String commandId = commands.getCommandId();
    String party = commands.getParty();
    List<String> actAs = commands.getActAsList();
    List<String> readAs = commands.getReadAsList();
    List<DisclosedContract> disclosedContracts =
        commands.getDisclosedContractsList().stream()
            .map(DisclosedContract::fromProto)
            .collect(Collectors.toList());
    List<String> packageIdSelectionPreference = commands.getPackageIdSelectionPreferenceList();
    Optional<Instant> minLedgerTimeAbs =
        commands.hasMinLedgerTimeAbs()
            ? Optional.of(
                Instant.ofEpochSecond(
                    commands.getMinLedgerTimeAbs().getSeconds(),
                    commands.getMinLedgerTimeAbs().getNanos()))
            : Optional.empty();
    Optional<Duration> minLedgerTimeRel =
        commands.hasMinLedgerTimeRel()
            ? Optional.of(
                Duration.ofSeconds(
                    commands.getMinLedgerTimeRel().getSeconds(),
                    commands.getMinLedgerTimeRel().getNanos()))
            : Optional.empty();
    Optional<Duration> deduplicationPeriod = Optional.empty();
    switch (commands.getDeduplicationPeriodCase()) {
      case DEDUPLICATION_DURATION:
        com.google.protobuf.Duration d = commands.getDeduplicationDuration();
        deduplicationPeriod = Optional.of(Duration.ofSeconds(d.getSeconds(), d.getNanos()));
        break;
      case DEDUPLICATION_TIME:
        @SuppressWarnings("deprecation")
        com.google.protobuf.Duration t = commands.getDeduplicationTime();
        deduplicationPeriod = Optional.of(Duration.ofSeconds(t.getSeconds(), t.getNanos()));
        break;
      case DEDUPLICATIONPERIOD_NOT_SET:
      default:
        // Backwards compatibility: do not throw, this field could be empty from a previous version
    }
    String submissionId = commands.getSubmissionId();
    ArrayList<Command> listOfCommands = new ArrayList<>(commands.getCommandsCount());
    for (CommandsOuterClass.Command command : commands.getCommandsList()) {
      listOfCommands.add(Command.fromProtoCommand(command));
    }
    if (!actAs.contains(party)) {
      actAs.add(0, party);
    }
    ArrayList<PrefetchContractKey> prefetchContractKeys = new ArrayList<>(commands.getPrefetchContractKeysCount());
    for (CommandsOuterClass.PrefetchContractKey key: commands.getPrefetchContractKeysList()) {
        prefetchContractKeys.add(PrefetchContractKey.fromProto(key));
    }
    return new SubmitCommandsRequest(
        workflowId,
        applicationId,
        commandId,
        actAs,
        readAs,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationPeriod,
        submissionId.isEmpty() ? Optional.empty() : Optional.of(submissionId),
        listOfCommands,
        disclosedContracts,
        packageIdSelectionPreference,
        prefetchContractKeys);
  }

  private static CommandsOuterClass.Commands toProto(
      @NonNull String ledgerId,
      @NonNull Optional<String> submissionId,
      @NonNull CommandsSubmission submission) {

    if (submission.getActAs().isEmpty()) {
      throw new IllegalArgumentException("actAs must have at least one element");
    }

    List<Command> commands = toCommands(submission.getCommands());
    List<CommandsOuterClass.Command> commandsConverted =
        commands.stream().map(Command::toProtoCommand).collect(Collectors.toList());
    List<CommandsOuterClass.DisclosedContract> disclosedContracts =
        submission.getDisclosedContracts().stream()
            .map(DisclosedContract::toProto)
            .collect(Collectors.toList());
    List<CommandsOuterClass.PrefetchContractKey> prefetchContractKeys =
        submission.getPrefetchContractKeys().stream()
            .map(PrefetchContractKey::toProto)
            .collect(Collectors.toList());

    CommandsOuterClass.Commands.Builder builder =
        CommandsOuterClass.Commands.newBuilder()
            .setLedgerId(ledgerId)
            .setApplicationId(submission.getApplicationId())
            .setCommandId(submission.getCommandId())
            .setParty(submission.getActAs().get(0))
            .addAllActAs(submission.getActAs())
            .addAllReadAs(submission.getReadAs())
            .addAllCommands(commandsConverted)
            .addAllDisclosedContracts(disclosedContracts)
            .addAllPackageIdSelectionPreference(submission.getPackageIdSelectionPreference())
            .addAllPrefetchContractKeys(prefetchContractKeys);

    submission
        .getMinLedgerTimeAbs()
        .ifPresent(
            abs ->
                builder.setMinLedgerTimeAbs(
                    Timestamp.newBuilder()
                        .setSeconds(abs.getEpochSecond())
                        .setNanos(abs.getNano())));

    submission
        .getMinLedgerTimeRel()
        .ifPresent(
            rel ->
                builder.setMinLedgerTimeRel(
                    com.google.protobuf.Duration.newBuilder()
                        .setSeconds(rel.getSeconds())
                        .setNanos(rel.getNano())));

    submission
        .getDeduplicationTime()
        .ifPresent(
            dedup -> {
              @SuppressWarnings("deprecation")
              var unused =
                  builder.setDeduplicationTime(
                      com.google.protobuf.Duration.newBuilder()
                          .setSeconds(dedup.getSeconds())
                          .setNanos(dedup.getNano()));
            });

    submission.getWorkflowId().ifPresent(builder::setWorkflowId);
    submissionId.ifPresent(builder::setSubmissionId);

    return builder.build();
  }

  public static CommandsOuterClass.Commands toProto(
      @NonNull String ledgerId, @NonNull CommandsSubmission submission) {
    return toProto(ledgerId, Optional.empty(), submission);
  }

  public static CommandsOuterClass.Commands toProto(
      @NonNull String ledgerId,
      @NonNull String submissionId,
      @NonNull CommandsSubmission submission) {
    return toProto(ledgerId, Optional.of(submissionId), submission);
  }

  @NonNull
  public String getWorkflowId() {
    return workflowId;
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
  public String getParty() {
    return party;
  }

  @NonNull
  public List<String> getActAs() {
    return actAs;
  }

  @NonNull
  public List<String> getReadAs() {
    return readAs;
  }

  @NonNull
  public Optional<Instant> getMinLedgerTimeAbsolute() {
    return minLedgerTimeAbsolute;
  }

  @NonNull
  public Optional<Duration> getMinLedgerTimeRelative() {
    return minLedgerTimeRelative;
  }

  @NonNull
  public Optional<Duration> getDeduplicationTime() {
    return deduplicationTime;
  }

  @NonNull
  public Optional<String> getSubmissionId() {
    return submissionId;
  }

  @NonNull
  public List<@NonNull Command> getCommands() {
    return commands;
  }

  @NonNull
  public List<@NonNull DisclosedContract> getDisclosedContracts() {
    return disclosedContracts;
  }

  @NonNull
  public List<@NonNull String> getPackageIdSelectionPreference() {
    return Collections.unmodifiableList(packageIdSelectionPreference);
  }

  @NonNull
  public List<@NonNull PrefetchContractKey> getPrefetchContractKeys() {
    return Collections.unmodifiableList(prefetchContractKeys);
  }

  @Override
  public String toString() {
    return "SubmitCommandsRequest{"
        + "workflowId='"
        + workflowId
        + '\''
        + ", applicationId='"
        + applicationId
        + '\''
        + ", commandId='"
        + commandId
        + '\''
        + ", party='"
        + party
        + '\''
        + ", minLedgerTimeAbs="
        + minLedgerTimeAbsolute
        + ", minLedgerTimeRel="
        + minLedgerTimeRelative
        + ", deduplicationTime="
        + deduplicationTime
        + ", submissionId="
        + submissionId
        + ", commands="
        + commands
        + ", disclosedContracts="
        + disclosedContracts
        + ", packageIdSelectionPreference="
        + packageIdSelectionPreference
        + ", prefetchContractKeys="
        + prefetchContractKeys
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SubmitCommandsRequest submitCommandsRequest1 = (SubmitCommandsRequest) o;
    return Objects.equals(workflowId, submitCommandsRequest1.workflowId)
        && Objects.equals(applicationId, submitCommandsRequest1.applicationId)
        && Objects.equals(commandId, submitCommandsRequest1.commandId)
        && Objects.equals(party, submitCommandsRequest1.party)
        && Objects.equals(actAs, submitCommandsRequest1.actAs)
        && Objects.equals(readAs, submitCommandsRequest1.readAs)
        && Objects.equals(minLedgerTimeAbsolute, submitCommandsRequest1.minLedgerTimeAbsolute)
        && Objects.equals(minLedgerTimeRelative, submitCommandsRequest1.minLedgerTimeRelative)
        && Objects.equals(deduplicationTime, submitCommandsRequest1.deduplicationTime)
        && Objects.equals(submissionId, submitCommandsRequest1.submissionId)
        && Objects.equals(commands, submitCommandsRequest1.commands)
        && Objects.equals(disclosedContracts, submitCommandsRequest1.disclosedContracts)
        && Objects.equals(packageIdSelectionPreference, submitCommandsRequest1.packageIdSelectionPreference);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        workflowId,
        applicationId,
        commandId,
        party,
        actAs,
        readAs,
        minLedgerTimeAbsolute,
        minLedgerTimeRelative,
        deduplicationTime,
        submissionId,
        commands,
        disclosedContracts,
        packageIdSelectionPreference);
  }
}
