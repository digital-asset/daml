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
 * TODO: javadoc this
 * This class represents a set of parameters required for {@link com.daml.ledger.rxjava.CommandClient}
 *
 * Usage:
 * <pre>
 *   var params = CommandClientConfig.create("workflowId", "applicationId", "commandId")
 *                                   .params.withAccessToken("token")
 *
 *
 * <pre/>
 */
public class CommandClientConfig {
  // Required params
  private String applicationId;
  private String commandId;
  private List<@NonNull ? extends HasCommands> commands;

  // optional params
  private String workflowId;
  private List<@NonNull String> actAs;
  private List<@NonNull String> readAs;
  private Optional<Instant> minLedgerTimeAbs;
  private Optional<Duration> minLedgerTimeRel;
  private Optional<Duration> deduplicationTime;
  private Optional<String> accessToken;

  private CommandClientConfig(
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

  /**
   * TODO: java doc this
   *
   * @param applicationId
   * @param commandId
   * @param commands
   * @return
   */
  public static CommandClientConfig create(
      String applicationId, String commandId, List<@NonNull ? extends HasCommands> commands) {
    return new CommandClientConfig(
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

  public CommandClientConfig withWorkflowId(String workflowId) {
    return new CommandClientConfig(
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

  public CommandClientConfig withParty(String party) {
    return new CommandClientConfig(
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

  public CommandClientConfig withActAs(List<@NonNull String> actAs) {
    return new CommandClientConfig(
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

  public CommandClientConfig withReadAs(List<@NonNull String> readAs) {
    return new CommandClientConfig(
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

  public CommandClientConfig withMinLedgerTimeAbs(Optional<Instant> minLedgerTimeAbs) {
    return new CommandClientConfig(
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

  public CommandClientConfig withMinLedgerTimeRel(Optional<Duration> minLedgerTimeRel) {
    return new CommandClientConfig(
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

  public CommandClientConfig withDeduplicationTime(Optional<Duration> deduplicationTime) {
    return new CommandClientConfig(
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

  public CommandClientConfig withCommands(List<@NonNull ? extends HasCommands> commands) {
    return new CommandClientConfig(
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

  public CommandClientConfig withAccessToken(Optional<String> accessToken) {
    return new CommandClientConfig(
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
