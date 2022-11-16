// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

import com.daml.ledger.api.v1.CommandsOuterClass;
import com.google.protobuf.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
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

  public SubmitCommandsRequest(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull Optional<Instant> minLedgerTimeAbsolute,
      @NonNull Optional<Duration> minLedgerTimeRelative,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull Command> commands) {
    this(
        workflowId,
        applicationId,
        commandId,
        asList(party),
        asList(),
        minLedgerTimeAbsolute,
        minLedgerTimeRelative,
        deduplicationTime,
        commands);
  }

  public SubmitCommandsRequest(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String submissionId,
      @NonNull String party,
      @NonNull Optional<Instant> minLedgerTimeAbsolute,
      @NonNull Optional<Duration> minLedgerTimeRelative,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull Command> commands) {
    this(
        workflowId,
        applicationId,
        commandId,
        asList(party),
        asList(),
        minLedgerTimeAbsolute,
        minLedgerTimeRelative,
        deduplicationTime,
        Optional.of(submissionId),
        commands);
  }

  public SubmitCommandsRequest(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Optional<Instant> minLedgerTimeAbsolute,
      @NonNull Optional<Duration> minLedgerTimeRelative,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull Command> commands) {
    this(
        workflowId,
        applicationId,
        commandId,
        actAs,
        readAs,
        minLedgerTimeAbsolute,
        minLedgerTimeRelative,
        deduplicationTime,
        Optional.empty(),
        commands);
  }

  public SubmitCommandsRequest(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String submissionId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Optional<Instant> minLedgerTimeAbsolute,
      @NonNull Optional<Duration> minLedgerTimeRelative,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull Command> commands) {
    this(
        workflowId,
        applicationId,
        commandId,
        actAs,
        readAs,
        minLedgerTimeAbsolute,
        minLedgerTimeRelative,
        deduplicationTime,
        Optional.of(submissionId),
        commands);
  }

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
      @NonNull List<@NonNull Command> commands) {
    if (actAs.size() == 0) {
      throw new IllegalArgumentException("actAs must have at least one element");
    }
    this.workflowId = workflowId;
    this.applicationId = applicationId;
    this.commandId = commandId;
    this.party = actAs.get(0);
    this.actAs = unmodifiableList(new ArrayList<>(actAs));
    this.readAs = unmodifiableList(new ArrayList<>(readAs));
    this.minLedgerTimeAbsolute = minLedgerTimeAbsolute;
    this.minLedgerTimeRelative = minLedgerTimeRelative;
    this.deduplicationTime = deduplicationTime;
    this.submissionId = submissionId;
    this.commands = commands;
  }

  public static SubmitCommandsRequest fromProto(CommandsOuterClass.Commands commands) {
    String workflowId = commands.getWorkflowId();
    String applicationId = commands.getApplicationId();
    String commandId = commands.getCommandId();
    String party = commands.getParty();
    List<String> actAs = commands.getActAsList();
    List<String> readAs = commands.getReadAsList();
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
        listOfCommands);
  }

  // TODO: Refactor this to take Optional for workflowId when deprecated methods using it below are
  // removed.
  @Deprecated
  private static CommandsOuterClass.Commands toProto(
      @NonNull String ledgerId,
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Optional<Instant> minLedgerTimeAbsolute,
      @NonNull Optional<Duration> minLedgerTimeRelative,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull Optional<String> submissionId,
      @NonNull List<@NonNull Command> commands) {
    if (actAs.size() == 0) {
      throw new IllegalArgumentException("actAs must have at least one element");
    }
    ArrayList<CommandsOuterClass.Command> commandsConverted = new ArrayList<>(commands.size());
    for (Command command : commands) {
      commandsConverted.add(command.toProtoCommand());
    }
    CommandsOuterClass.Commands.Builder builder =
        CommandsOuterClass.Commands.newBuilder()
            .setLedgerId(ledgerId)
            .setWorkflowId(workflowId)
            .setApplicationId(applicationId)
            .setCommandId(commandId)
            .setParty(actAs.get(0))
            .addAllActAs(actAs)
            .addAllReadAs(readAs)
            .addAllCommands(commandsConverted);
    minLedgerTimeAbsolute.ifPresent(
        abs ->
            builder.setMinLedgerTimeAbs(
                Timestamp.newBuilder().setSeconds(abs.getEpochSecond()).setNanos(abs.getNano())));
    minLedgerTimeRelative.ifPresent(
        rel ->
            builder.setMinLedgerTimeRel(
                com.google.protobuf.Duration.newBuilder()
                    .setSeconds(rel.getSeconds())
                    .setNanos(rel.getNano())));
    deduplicationTime.ifPresent(
        dedup -> {
          @SuppressWarnings("deprecation")
          var unused =
              builder.setDeduplicationTime(
                  com.google.protobuf.Duration.newBuilder()
                      .setSeconds(dedup.getSeconds())
                      .setNanos(dedup.getNano()));
        });
    submissionId.ifPresent(builder::setSubmissionId);
    return builder.build();
  }

  private static CommandsOuterClass.Commands toProto(
      @NonNull String ledgerId,
      @NonNull Optional<String> submissionId,
      @NonNull CommandsSubmission commandsSubmission) {

    if (commandsSubmission.getActAs().size() == 0) {
      throw new IllegalArgumentException("actAs must have at least one element");
    }
    ArrayList<CommandsOuterClass.Command> commandsConverted =
        new ArrayList<>(commandsSubmission.getCommands().size());

    // TODO: there has to be a better way!
    List<Command> commands =
        commandsSubmission.getCommands().stream()
            .map(c -> (Command) c)
            .collect(Collectors.toList());

    for (Command command : commands) {
      commandsConverted.add(command.toProtoCommand());
    }

    CommandsOuterClass.Commands.Builder builder =
        CommandsOuterClass.Commands.newBuilder()
            .setLedgerId(ledgerId)
            .setApplicationId(commandsSubmission.getApplicationId())
            .setCommandId(commandsSubmission.getCommandId())
            .setParty(commandsSubmission.getActAs().get(0))
            .addAllActAs(commandsSubmission.getActAs())
            .addAllReadAs(commandsSubmission.getReadAs())
            .addAllCommands(commandsConverted);

    commandsSubmission
        .getMinLedgerTimeAbs()
        .ifPresent(
            abs ->
                builder.setMinLedgerTimeAbs(
                    Timestamp.newBuilder()
                        .setSeconds(abs.getEpochSecond())
                        .setNanos(abs.getNano())));

    commandsSubmission
        .getMinLedgerTimeRel()
        .ifPresent(
            rel ->
                builder.setMinLedgerTimeRel(
                    com.google.protobuf.Duration.newBuilder()
                        .setSeconds(rel.getSeconds())
                        .setNanos(rel.getNano())));

    commandsSubmission
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

    commandsSubmission.getWorkflowId().ifPresent(builder::setWorkflowId);
    submissionId.ifPresent(builder::setSubmissionId);

    return builder.build();
  }

  public static CommandsOuterClass.Commands toProto(
      @NonNull String ledgerId, @NonNull CommandsSubmission commandsSubmission) {
    return toProto(
        ledgerId,
        commandsSubmission.getWorkflowId().orElse(""),
        commandsSubmission.getApplicationId(),
        commandsSubmission.getCommandId(),
        commandsSubmission.getActAs(),
        commandsSubmission.getReadAs(),
        commandsSubmission.getMinLedgerTimeAbs(),
        commandsSubmission.getMinLedgerTimeRel(),
        commandsSubmission.getDeduplicationTime(),
        Optional.empty(),
        (List<Command>) commandsSubmission.getCommands()); // TODO: remove this horrible cast
  }

  /**
   * Please use {@link #toProto(String, CommandsSubmission)}
   *
   * @deprecated
   * @since 2.5
   */
  @Deprecated
  public static CommandsOuterClass.Commands toProto(
      @NonNull String ledgerId,
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Optional<Instant> minLedgerTimeAbsolute,
      @NonNull Optional<Duration> minLedgerTimeRelative,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull Command> commands) {
    return toProto(
        ledgerId,
        workflowId,
        applicationId,
        commandId,
        actAs,
        readAs,
        minLedgerTimeAbsolute,
        minLedgerTimeRelative,
        deduplicationTime,
        Optional.empty(),
        commands);
  }

  public static CommandsOuterClass.Commands toProto(
      @NonNull String ledgerId,
      @NonNull String submissionId,
      @NonNull CommandsSubmission commandsSubmission) {
    return toProto(
        ledgerId,
        commandsSubmission.getWorkflowId().orElse(""),
        commandsSubmission.getApplicationId(),
        commandsSubmission.getCommandId(),
        commandsSubmission.getActAs(),
        commandsSubmission.getReadAs(),
        commandsSubmission.getMinLedgerTimeAbs(),
        commandsSubmission.getMinLedgerTimeRel(),
        commandsSubmission.getDeduplicationTime(),
        Optional.of(submissionId),
        (List<Command>) commandsSubmission.getCommands()); // todo remove this horrible cast
  }

  /**
   * Please use {@link #toProto(String, String, CommandsSubmission)}
   *
   * @deprecated
   * @since 2.5
   */
  @Deprecated
  public static CommandsOuterClass.Commands toProto(
      @NonNull String ledgerId,
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String submissionId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Optional<Instant> minLedgerTimeAbsolute,
      @NonNull Optional<Duration> minLedgerTimeRelative,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull Command> commands) {
    return toProto(
        ledgerId,
        workflowId,
        applicationId,
        commandId,
        actAs,
        readAs,
        minLedgerTimeAbsolute,
        minLedgerTimeRelative,
        deduplicationTime,
        Optional.of(submissionId),
        commands);
  }

  /**
   * Please use {@link #toProto(String, String, CommandsSubmission)}
   *
   * @deprecated
   * @since 2.5
   */
  @Deprecated
  public static CommandsOuterClass.Commands toProto(
      @NonNull String ledgerId,
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String submissionId,
      @NonNull String party,
      @NonNull Optional<Instant> minLedgerTimeAbsolute,
      @NonNull Optional<Duration> minLedgerTimeRelative,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull Command> commands) {
    List<String> empty_read_as = new ArrayList<>();
    List<String> act_as = new ArrayList<>();
    act_as.add(party);
    return toProto(
        ledgerId,
        workflowId,
        applicationId,
        commandId,
        act_as,
        empty_read_as,
        minLedgerTimeAbsolute,
        minLedgerTimeRelative,
        deduplicationTime,
        Optional.of(submissionId),
        commands);
  }

  /**
   * Please use {@link #toProto(String, CommandsSubmission)}
   *
   * @deprecated
   * @since 2.5
   */
  @Deprecated
  public static CommandsOuterClass.Commands toProto(
      @NonNull String ledgerId,
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull Optional<Instant> minLedgerTimeAbsolute,
      @NonNull Optional<Duration> minLedgerTimeRelative,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull Command> commands) {
    List<String> empty_read_as = new ArrayList<>();
    List<String> act_as = new ArrayList<>();
    act_as.add(party);
    return toProto(
        ledgerId,
        workflowId,
        applicationId,
        commandId,
        act_as,
        empty_read_as,
        minLedgerTimeAbsolute,
        minLedgerTimeRelative,
        deduplicationTime,
        Optional.empty(),
        commands);
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
        && Objects.equals(commands, submitCommandsRequest1.commands);
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
        commands);
  }
}
