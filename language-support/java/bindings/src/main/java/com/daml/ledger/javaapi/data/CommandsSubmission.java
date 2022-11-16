// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
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
 * e.g {@link #withActAs(List)}, {@link #withWorkflowId(String)} etc.
 *
 * Usage:
 * <pre>
 *   var params = CommandsSubmission.create("applicationId", "commandId", commands)
 *                                   .withAccessToken("token")
 *                                   .withParty("party")
 *                                   .with...
 * <pre/>
 */
public final class CommandsSubmission {
  private String applicationId;
  private String commandId;
  private List<@NonNull ? extends HasCommands> commands;

  private Optional<String> workflowId;
  private List<@NonNull String> actAs;
  private List<@NonNull String> readAs;
  private Optional<Instant> minLedgerTimeAbs;
  private Optional<Duration> minLedgerTimeRel;
  private Optional<Duration> deduplicationTime;
  private Optional<String> accessToken;

  private CommandsSubmission(
      Optional<String> workflowId,
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

  public static CommandsSubmission create(
      String applicationId, String commandId, List<@NonNull ? extends HasCommands> commands) {
    return new CommandsSubmission(
        Optional.empty(),
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

  public Optional<String> getWorkflowId() {
    return workflowId;
  }

  public String getApplicationId() {
    return applicationId;
  }

  public String getCommandId() {
    return commandId;
  }

  public List<String> getActAs() {
    return unmodifiableList(actAs);
  }

  public List<String> getReadAs() {
    return unmodifiableList(readAs);
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
    return unmodifiableList(commands);
  }

  public Optional<String> getAccessToken() {
    return accessToken;
  }

  public CommandsSubmission withWorkflowId(String workflowId) {
    return new CommandsSubmission(
        Optional.of(workflowId),
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

  public CommandsSubmission withActAs(String actAs) {
    return new CommandsSubmission(
        workflowId,
        applicationId,
        commandId,
        List.of(actAs),
        readAs,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        commands,
        accessToken);
  }

  public CommandsSubmission withActAs(List<@NonNull String> actAs) {
    return new CommandsSubmission(
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

  public CommandsSubmission withReadAs(List<@NonNull String> readAs) {
    return new CommandsSubmission(
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

  public CommandsSubmission withMinLedgerTimeAbs(Optional<Instant> minLedgerTimeAbs) {
    return new CommandsSubmission(
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

  public CommandsSubmission withMinLedgerTimeRel(Optional<Duration> minLedgerTimeRel) {
    return new CommandsSubmission(
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

  public CommandsSubmission withDeduplicationTime(Optional<Duration> deduplicationTime) {
    return new CommandsSubmission(
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

  public CommandsSubmission withCommands(List<@NonNull ? extends HasCommands> commands) {
    return new CommandsSubmission(
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

  public CommandsSubmission withAccessToken(Optional<String> accessToken) {
    return new CommandsSubmission(
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
