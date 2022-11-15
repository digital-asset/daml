// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Optional.empty;

import com.daml.ledger.javaapi.data.codegen.HasCommands;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * This class should be used to build a valid submission. It provides {@link #create(String, String, List)}
 * for initial creation and extra builder methods for additional parameters
 * e.g {@link #withActAs(List)}, {@link #withWorkflowId(String)} e.t.c
 *
 * Usage:
 * <pre>
 *   var params = CommandsBuilder.create("applicationId", "commandId", commands)
 *                                   .withAccessToken("token")
 *                                   .withParty("party")
 *                                   .with...
 * <pre/>
 */
public class CommandsBuilder {
  private String applicationId;
  private String commandId;
  private List<@NonNull ? extends HasCommands> commands;

  private String workflowId;
  private List<@NonNull String> actAs;
  private List<@NonNull String> readAs;
  private Optional<Instant> minLedgerTimeAbs;
  private Optional<Duration> minLedgerTimeRel;
  private Optional<Duration> deduplicationTime;
  private Optional<String> accessToken;

  private CommandsBuilder(
      String workflowId,
      String applicationId,
      String commandId,
      List<@NonNull String> actAs,
      List<@NonNull String> readAs,
      Optional<Instant> minLedgerTimeAbs,
      Optional<Duration> minLedgerTimeRel,
      Optional<Duration> deduplicationTime,
      List<@NonNull ? extends HasCommands> commands,
      Optional<String> accessToken) {
    this.workflowId = workflowId;
    this.applicationId = applicationId;
    this.commandId = commandId;
    this.actAs = actAs;
    this.readAs = readAs;
    this.minLedgerTimeAbs = minLedgerTimeAbs;
    this.minLedgerTimeRel = minLedgerTimeRel;
    this.deduplicationTime = deduplicationTime;
    this.commands = commands;
    this.accessToken = accessToken;
  }

  public static CommandsBuilder create(
      String applicationId, String commandId, List<@NonNull ? extends HasCommands> commands) {
    return new CommandsBuilder(
        "",
        applicationId,
        commandId,
        emptyList(),
        emptyList(),
        empty(),
        empty(),
        empty(),
        commands,
        empty());
  }

  public String getWorkflowId() {
    return workflowId;
  }

  public String getApplicationId() {
    return applicationId;
  }

  public String getCommandId() {
    return commandId;
  }

  public List<String> getActAs() {
    return actAs;
  }

  public List<String> getReadAs() {
    return readAs;
  }

  public Optional<Instant> getMinLedgerTimeAbs() {
    return minLedgerTimeAbs;
  }

  public Optional<Duration> getMinLedgerTimeRel() {
    return minLedgerTimeRel;
  }

  public Optional<Duration> getDeduplicationTime() {
    return deduplicationTime;
  }

  public List<? extends HasCommands> getCommands() {
    return commands;
  }

  public Optional<String> getAccessToken() {
    return accessToken;
  }

  public CommandsBuilder withWorkflowId(String workflowId) {
    return new CommandsBuilder(
        workflowId,
        applicationId,
        commandId,
        actAs,
        readAs,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        commands,
        accessToken);
  }

  public CommandsBuilder withParty(String party) {
    return new CommandsBuilder(
        workflowId,
        applicationId,
        commandId,
        singletonList(party),
        readAs,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        commands,
        accessToken);
  }

  public CommandsBuilder withActAs(List<@NonNull String> actAs) {
    return new CommandsBuilder(
        workflowId,
        applicationId,
        commandId,
        actAs,
        readAs,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        commands,
        accessToken);
  }

  public CommandsBuilder withReadAs(List<@NonNull String> readAs) {
    return new CommandsBuilder(
        workflowId,
        applicationId,
        commandId,
        actAs,
        readAs,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        commands,
        accessToken);
  }

  public CommandsBuilder withMinLedgerTimeAbs(Optional<Instant> minLedgerTimeAbs) {
    return new CommandsBuilder(
        workflowId,
        applicationId,
        commandId,
        actAs,
        readAs,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        commands,
        accessToken);
  }

  public CommandsBuilder withMinLedgerTimeRel(Optional<Duration> minLedgerTimeRel) {
    return new CommandsBuilder(
        workflowId,
        applicationId,
        commandId,
        actAs,
        readAs,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        commands,
        accessToken);
  }

  public CommandsBuilder withDeduplicationTime(Optional<Duration> deduplicationTime) {
    return new CommandsBuilder(
        workflowId,
        applicationId,
        commandId,
        actAs,
        readAs,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        commands,
        accessToken);
  }

  public CommandsBuilder withCommands(List<@NonNull ? extends HasCommands> commands) {
    return new CommandsBuilder(
        workflowId,
        applicationId,
        commandId,
        actAs,
        readAs,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        commands,
        accessToken);
  }

  public CommandsBuilder withAccessToken(Optional<String> accessToken) {
    return new CommandsBuilder(
        workflowId,
        applicationId,
        commandId,
        actAs,
        readAs,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        commands,
        accessToken);
  }
}
