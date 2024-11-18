// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

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
 * This class can be used to build a valid submission. It provides {@link #create(String, String, List)}
 * for initial creation and methods to set optional parameters
 * e.g {@link #withActAs(List)}, {@link #withWorkflowId(String)} etc.
 * <p>
 * Usage:
 * <pre>
 *   var submission = CommandsSubmission.create(applicationId, commandId, commands)
 *                                   .withAccessToken(token)
 *                                   .withParty(party)
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
  private List<DisclosedContract> disclosedContracts;
  private List<String> packageIdSelectionPreference;

  protected CommandsSubmission(
      String applicationId,
      String commandId,
      List<@NonNull ? extends HasCommands> commands,
      List<@NonNull String> actAs,
      List<@NonNull String> readAs,
      Optional<String> workflowId,
      Optional<Instant> minLedgerTimeAbs,
      Optional<Duration> minLedgerTimeRel,
      Optional<Duration> deduplicationTime,
      Optional<String> accessToken,
      List<@NonNull DisclosedContract> disclosedContracts,
      List<String> packageIdSelectionPreference) {
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
    this.disclosedContracts = disclosedContracts;
    this.packageIdSelectionPreference = packageIdSelectionPreference;
  }

  public static CommandsSubmission create(
      String applicationId, String commandId, List<@NonNull ? extends HasCommands> commands) {
    return new CommandsSubmission(
        applicationId,
        commandId,
        commands,
        emptyList(),
        emptyList(),
        empty(),
        empty(),
        Optional.empty(),
        empty(),
        empty(),
        emptyList(),
        emptyList());
  }

  public Optional<String> getWorkflowId() {
    return workflowId;
  }

  public List<String> getPackageIdSelectionPreference() {
    return unmodifiableList(packageIdSelectionPreference);
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

  public List<DisclosedContract> getDisclosedContracts() {
    return unmodifiableList(disclosedContracts);
  }

  public CommandsSubmission withWorkflowId(String workflowId) {
    return new CommandsSubmission(
        applicationId,
        commandId,
        commands,
        actAs,
        readAs,
        Optional.of(workflowId),
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        accessToken,
        disclosedContracts,
        packageIdSelectionPreference);
  }

  public CommandsSubmission withActAs(String actAs) {
    return new CommandsSubmission(
        applicationId,
        commandId,
        commands,
        List.of(actAs),
        readAs,
        workflowId,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        accessToken,
        disclosedContracts,
        packageIdSelectionPreference);
  }

  public CommandsSubmission withActAs(List<@NonNull String> actAs) {
    return new CommandsSubmission(
        applicationId,
        commandId,
        commands,
        actAs,
        readAs,
        workflowId,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        accessToken,
        disclosedContracts,
        packageIdSelectionPreference);
  }

  public CommandsSubmission withReadAs(List<@NonNull String> readAs) {
    return new CommandsSubmission(
        applicationId,
        commandId,
        commands,
        actAs,
        readAs,
        workflowId,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        accessToken,
        disclosedContracts,
        packageIdSelectionPreference);
  }

  public CommandsSubmission withMinLedgerTimeAbs(Optional<Instant> minLedgerTimeAbs) {
    return new CommandsSubmission(
        applicationId,
        commandId,
        commands,
        actAs,
        readAs,
        workflowId,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        accessToken,
        disclosedContracts,
        packageIdSelectionPreference);
  }

  public CommandsSubmission withMinLedgerTimeRel(Optional<Duration> minLedgerTimeRel) {
    return new CommandsSubmission(
        applicationId,
        commandId,
        commands,
        actAs,
        readAs,
        workflowId,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        accessToken,
        disclosedContracts,
        packageIdSelectionPreference);
  }

  public CommandsSubmission withDeduplicationTime(Optional<Duration> deduplicationTime) {
    return new CommandsSubmission(
        applicationId,
        commandId,
        commands,
        actAs,
        readAs,
        workflowId,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        accessToken,
        disclosedContracts,
        packageIdSelectionPreference);
  }

  public CommandsSubmission withCommands(List<@NonNull ? extends HasCommands> commands) {
    return new CommandsSubmission(
        applicationId,
        commandId,
        commands,
        actAs,
        readAs,
        workflowId,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        accessToken,
        disclosedContracts,
        packageIdSelectionPreference);
  }

  public CommandsSubmission withAccessToken(Optional<String> accessToken) {
    return new CommandsSubmission(
        applicationId,
        commandId,
        commands,
        actAs,
        readAs,
        workflowId,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        accessToken,
        disclosedContracts,
        packageIdSelectionPreference);
  }

  public CommandsSubmission withDisclosedContracts(List<DisclosedContract> disclosedContracts) {
    return new CommandsSubmission(
        applicationId,
        commandId,
        commands,
        actAs,
        readAs,
        workflowId,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        accessToken,
        disclosedContracts,
        packageIdSelectionPreference);
  }

  public CommandsSubmission withPackageIdSelectionPreference(List<String> packageIdSelectionPreference) {
    return new CommandsSubmission(
        applicationId,
        commandId,
        commands,
        actAs,
        readAs,
        workflowId,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        accessToken,
        disclosedContracts,
        packageIdSelectionPreference);
  }
}
