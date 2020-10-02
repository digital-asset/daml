// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

public class SubmitCommandsRequest {

    private final String workflowId;

    private final String applicationId;

    private final String commandId;

    private final String party;

    private final Optional<Instant> minLedgerTimeAbsolute;
    private final Optional<Duration> minLedgerTimeRelative;
    private final Optional<Duration> deduplicationTime;
    private final List<Command> commands;

    public SubmitCommandsRequest(@NonNull String workflowId, @NonNull String applicationId,
                                 @NonNull String commandId, @NonNull String party, @NonNull Optional<Instant> minLedgerTimeAbsolute,
                                 @NonNull Optional<Duration> minLedgerTimeRelative, @NonNull Optional<Duration> deduplicationTime,
                                 @NonNull List<@NonNull Command> commands) {
        this.workflowId = workflowId;
        this.applicationId = applicationId;
        this.commandId = commandId;
        this.party = party;
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
        return new SubmitCommandsRequest(workflowId, applicationId, commandId, party, minLedgerTimeAbs, minLedgerTimeRel, deduplicationTime, listOfCommands);
    }

    public static CommandsOuterClass.Commands toProto(@NonNull String ledgerId,
                                                      @NonNull String workflowId, @NonNull String applicationId,
                                                      @NonNull String commandId, @NonNull String party, @NonNull Optional<Instant> minLedgerTimeAbsolute,
                                                      @NonNull Optional<Duration> minLedgerTimeRelative, @NonNull Optional<Duration> deduplicationTime,
                                                      @NonNull List<@NonNull Command> commands) {
        ArrayList<CommandsOuterClass.Command> commandsConverted = new ArrayList<>(commands.size());
        for (Command command : commands) {
            commandsConverted.add(command.toProtoCommand());
        }
        CommandsOuterClass.Commands.Builder builder = CommandsOuterClass.Commands.newBuilder()
                .setLedgerId(ledgerId)
                .setWorkflowId(workflowId)
                .setApplicationId(applicationId)
                .setCommandId(commandId)
                .setParty(party)
                .addAllCommands(commandsConverted);
        minLedgerTimeAbsolute.ifPresent(abs -> builder.setMinLedgerTimeAbs(Timestamp.newBuilder().setSeconds(abs.getEpochSecond()).setNanos(abs.getNano())));
        minLedgerTimeRelative.ifPresent(rel -> builder.setMinLedgerTimeRel(com.google.protobuf.Duration.newBuilder().setSeconds(rel.getSeconds()).setNanos(rel.getNano())));
        deduplicationTime.ifPresent(dedup -> builder.setDeduplicationTime(com.google.protobuf.Duration.newBuilder().setSeconds(dedup.getSeconds()).setNanos(dedup.getNano())));
        return builder.build();
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
                Objects.equals(minLedgerTimeAbsolute, submitCommandsRequest1.minLedgerTimeAbsolute) &&
                Objects.equals(minLedgerTimeRelative, submitCommandsRequest1.minLedgerTimeRelative) &&
                Objects.equals(deduplicationTime, submitCommandsRequest1.deduplicationTime) &&
                Objects.equals(commands, submitCommandsRequest1.commands);
    }

    @Override
    public int hashCode() {

        return Objects.hash(workflowId, applicationId, commandId, party, minLedgerTimeAbsolute, minLedgerTimeRelative, deduplicationTime, commands);
    }
}
