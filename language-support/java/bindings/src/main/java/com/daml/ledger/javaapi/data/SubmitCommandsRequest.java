// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.CommandsOuterClass;
import com.google.protobuf.Timestamp;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

public class SubmitCommandsRequest {

    private final String workflowId;

    private final String applicationId;

    private final String commandId;

    private final String party;
    private final List<String> actAs;
    private final List<String> readAs;

    private final Optional<Instant> minLedgerTimeAbsolute;
    private final Optional<Duration> minLedgerTimeRelative;
    private final Optional<Duration> deduplicationTime;
    private final List<Command> commands;

    public SubmitCommandsRequest(@NonNull String workflowId, @NonNull String applicationId,
                                 @NonNull String commandId, @NonNull String party, @NonNull Optional<Instant> minLedgerTimeAbsolute,
                                 @NonNull Optional<Duration> minLedgerTimeRelative, @NonNull Optional<Duration> deduplicationTime,
                                 @NonNull List<@NonNull Command> commands) {
        this(
                workflowId, applicationId, commandId, asList(party), asList(),
                minLedgerTimeAbsolute, minLedgerTimeRelative, deduplicationTime,
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
        this.commands = commands;
    }


    public static SubmitCommandsRequest fromProto(CommandsOuterClass.Commands commands) {
        String ledgerId = commands.getLedgerId();
        String workflowId = commands.getWorkflowId();
        String applicationId = commands.getApplicationId();
        String commandId = commands.getCommandId();
        String party = commands.getParty();
        List<String> actAs = commands.getActAsList();
        List<String> readAs = commands.getReadAsList();
        Optional<Instant> minLedgerTimeAbs = commands.hasMinLedgerTimeAbs() ?
                Optional.of(Instant.ofEpochSecond(commands.getMinLedgerTimeAbs().getSeconds(), commands.getMinLedgerTimeAbs().getNanos())) : Optional.empty();
        Optional<Duration> minLedgerTimeRel = commands.hasMinLedgerTimeRel() ?
                Optional.of(Duration.ofSeconds(commands.getMinLedgerTimeRel().getSeconds(), commands.getMinLedgerTimeRel().getNanos())) : Optional.empty();
        Optional<Duration> deduplicationTime = commands.hasDeduplicationTime() ?
                Optional.of(Duration.ofSeconds(commands.getDeduplicationTime().getSeconds(), commands.getDeduplicationTime().getNanos())) : Optional.empty();
        ArrayList<Command> listOfCommands = new ArrayList<>(commands.getCommandsCount());
        for (CommandsOuterClass.Command command : commands.getCommandsList()) {
            listOfCommands.add(Command.fromProtoCommand(command));
        }
        if (!actAs.contains(party)) {
            actAs.add(0, party);
        }
        return new SubmitCommandsRequest(
                workflowId, applicationId, commandId, actAs, readAs,
                minLedgerTimeAbs, minLedgerTimeRel, deduplicationTime,
                listOfCommands);
    }

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
        if (actAs.size() == 0) {
            throw new IllegalArgumentException("actAs must have at least one element");
        }
        ArrayList<CommandsOuterClass.Command> commandsConverted = new ArrayList<>(commands.size());
        for (Command command : commands) {
            commandsConverted.add(command.toProtoCommand());
        }
        CommandsOuterClass.Commands.Builder builder = CommandsOuterClass.Commands.newBuilder()
                .setLedgerId(ledgerId)
                .setWorkflowId(workflowId)
                .setApplicationId(applicationId)
                .setCommandId(commandId)
                .setParty(actAs.get(0))
                .addAllActAs(actAs)
                .addAllReadAs(readAs)
                .addAllCommands(commandsConverted);
        minLedgerTimeAbsolute.ifPresent(
                abs -> builder.setMinLedgerTimeAbs(
                        Timestamp.newBuilder()
                                .setSeconds(abs.getEpochSecond()).setNanos(abs.getNano())));
        minLedgerTimeRelative.ifPresent(
                rel -> builder.setMinLedgerTimeRel(
                        com.google.protobuf.Duration.newBuilder().
                                setSeconds(rel.getSeconds()).setNanos(rel.getNano())));
        deduplicationTime.ifPresent(
                dedup -> builder.setDeduplicationTime(
                        com.google.protobuf.Duration.newBuilder()
                                .setSeconds(dedup.getSeconds()).setNanos(dedup.getNano())));
        return builder.build();
    }

    public static CommandsOuterClass.Commands toProto(@NonNull String ledgerId,
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
        return toProto(ledgerId, workflowId, applicationId, commandId, act_as, empty_read_as, minLedgerTimeAbsolute, minLedgerTimeRelative, deduplicationTime, commands);
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
    public List<String> getActAs() { return actAs; }

    @NonNull
    public List<String> getReadAs() { return readAs; }

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
    public List<@NonNull Command> getCommands() {
        return commands;
    }

    @Override
    public String toString() {
        return "SubmitCommandsRequest{" +
                "workflowId='" + workflowId + '\'' +
                ", applicationId='" + applicationId + '\'' +
                ", commandId='" + commandId + '\'' +
                ", party='" + party + '\'' +
                ", minLedgerTimeAbs=" + minLedgerTimeAbsolute +
                ", minLedgerTimeRel=" + minLedgerTimeRelative +
                ", deduplicationTime=" + deduplicationTime +
                ", commands=" + commands +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SubmitCommandsRequest submitCommandsRequest1 = (SubmitCommandsRequest) o;
        return Objects.equals(workflowId, submitCommandsRequest1.workflowId) &&
                Objects.equals(applicationId, submitCommandsRequest1.applicationId) &&
                Objects.equals(commandId, submitCommandsRequest1.commandId) &&
                Objects.equals(party, submitCommandsRequest1.party) &&
                Objects.equals(actAs, submitCommandsRequest1.actAs) &&
                Objects.equals(readAs, submitCommandsRequest1.readAs) &&
                Objects.equals(minLedgerTimeAbsolute, submitCommandsRequest1.minLedgerTimeAbsolute) &&
                Objects.equals(minLedgerTimeRelative, submitCommandsRequest1.minLedgerTimeRelative) &&
                Objects.equals(deduplicationTime, submitCommandsRequest1.deduplicationTime) &&
                Objects.equals(commands, submitCommandsRequest1.commands);
    }

    @Override
    public int hashCode() {

        return Objects.hash(workflowId, applicationId, commandId, party, actAs, readAs, minLedgerTimeAbsolute, minLedgerTimeRelative, deduplicationTime, commands);
    }
}
