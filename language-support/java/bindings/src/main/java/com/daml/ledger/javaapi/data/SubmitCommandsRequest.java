// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.digitalasset.ledger.api.v1.CommandsOuterClass;
import com.google.protobuf.Timestamp;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SubmitCommandsRequest {

    private final String workflowId;

    private final String applicationId;

    private final String commandId;

    private final String party;

    private final Instant ledgerEffectiveTime;

    private final Instant maximumRecordTime;

    private final List<Command> commands;

    public SubmitCommandsRequest(@NonNull String workflowId, @NonNull String applicationId,
                                 @NonNull String commandId, @NonNull String party, @NonNull Instant ledgerEffectiveTime,
                                 @NonNull Instant maximumRecordTime, @NonNull List<@NonNull Command> commands) {
        this.workflowId = workflowId;
        this.applicationId = applicationId;
        this.commandId = commandId;
        this.party = party;
        this.ledgerEffectiveTime = ledgerEffectiveTime;
        this.maximumRecordTime = maximumRecordTime;
        this.commands = commands;
    }

    public static SubmitCommandsRequest fromProto(CommandsOuterClass.Commands commands) {
        String ledgerId = commands.getLedgerId();
        String workflowId = commands.getWorkflowId();
        String applicationId = commands.getApplicationId();
        String commandId = commands.getCommandId();
        String party = commands.getParty();
        Timestamp ledgerEffectiveTime = commands.getLedgerEffectiveTime();
        Timestamp maximumRecordTime = commands.getMaximumRecordTime();
        ArrayList<Command> listOfCommands = new ArrayList<>(commands.getCommandsCount());
        for (CommandsOuterClass.Command command : commands.getCommandsList()) {
            listOfCommands.add(Command.fromProtoCommand(command));
        }
        return new SubmitCommandsRequest(workflowId, applicationId, commandId, party,
                Instant.ofEpochSecond(ledgerEffectiveTime.getSeconds(), ledgerEffectiveTime.getNanos()),
                Instant.ofEpochSecond(maximumRecordTime.getSeconds(), maximumRecordTime.getNanos()),
                listOfCommands);
    }

    public static CommandsOuterClass.Commands toProto(@NonNull String ledgerId,
                                                                       @NonNull String workflowId, @NonNull String applicationId,
                                                                       @NonNull String commandId, @NonNull String party, @NonNull Instant ledgerEffectiveTime,
                                                                       @NonNull Instant maximumRecordTime, @NonNull List<@NonNull Command> commands) {
        ArrayList<CommandsOuterClass.Command> commandsConverted = new ArrayList<>(commands.size());
        for (Command command : commands) {
            commandsConverted.add(command.toProtoCommand());
        }
        return CommandsOuterClass.Commands.newBuilder()
                .setLedgerId(ledgerId)
                .setWorkflowId(workflowId)
                .setApplicationId(applicationId)
                .setCommandId(commandId)
                .setParty(party)
                .setLedgerEffectiveTime(Timestamp.newBuilder().setSeconds(ledgerEffectiveTime.getEpochSecond()).setNanos(ledgerEffectiveTime.getNano()).build())
                .setMaximumRecordTime(Timestamp.newBuilder().setSeconds(maximumRecordTime.getEpochSecond()).setNanos(maximumRecordTime.getNano()).build())
                .addAllCommands(commandsConverted)
            .build();
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
    public Instant getLedgerEffectiveTime() {
        return ledgerEffectiveTime;
    }

    @NonNull
    public Instant getMaximumRecordTime() {
        return maximumRecordTime;
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
                ", ledgerEffectiveTime=" + ledgerEffectiveTime +
                ", maximumRecordTime=" + maximumRecordTime +
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
                Objects.equals(ledgerEffectiveTime, submitCommandsRequest1.ledgerEffectiveTime) &&
                Objects.equals(maximumRecordTime, submitCommandsRequest1.maximumRecordTime) &&
                Objects.equals(commands, submitCommandsRequest1.commands);
    }

    @Override
    public int hashCode() {

        return Objects.hash(workflowId, applicationId, commandId, party, ledgerEffectiveTime, maximumRecordTime, commands);
    }
}
