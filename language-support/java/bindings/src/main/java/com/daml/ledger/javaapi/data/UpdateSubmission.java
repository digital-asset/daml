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

import com.daml.ledger.javaapi.data.codegen.Update;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * This class can be used to build a valid submission for an Update. It provides {@link #create(String, String, Update)}
 * for initial creation and methods to set optional parameters
 * e.g {@link #withActAs(List)}, {@link #withWorkflowId(String)} etc.
 *
 * Usage:
 * <pre>
 *   var submission = UpdateSubmission.create(applicationId, commandId, update)
 *                                   .withAccessToken(token)
 *                                   .withParty(party)
 *                                   .with...
 * <pre/>
 */
public final class UpdateSubmission<U> {
  private String applicationId;
  private String commandId;
  private Update<U> update;

  private Optional<String> workflowId;
  private List<@NonNull String> actAs;
  private List<@NonNull String> readAs;
  private Optional<Instant> minLedgerTimeAbs;
  private Optional<Duration> minLedgerTimeRel;
  private Optional<Duration> deduplicationTime;
  private Optional<String> accessToken;

  private UpdateSubmission(
      String applicationId,
      String commandId,
      Update<U> update,
      List<@NonNull String> actAs,
      List<@NonNull String> readAs,
      Optional<String> workflowId,
      Optional<Instant> minLedgerTimeAbs,
      Optional<Duration> minLedgerTimeRel,
      Optional<Duration> deduplicationTime,
      Optional<String> accessToken) {
    this.workflowId = workflowId;
    this.applicationId = applicationId;
    this.commandId = commandId;
    this.actAs = actAs;
    this.readAs = readAs;
    this.minLedgerTimeAbs = minLedgerTimeAbs;
    this.minLedgerTimeRel = minLedgerTimeRel;
    this.deduplicationTime = deduplicationTime;
    this.update = update;
    this.accessToken = accessToken;
  }

  public static <U> UpdateSubmission<U> create(
      String applicationId, String commandId, Update<U> update) {
    return new UpdateSubmission<U>(
        applicationId,
        commandId,
        update,
        emptyList(),
        emptyList(),
        empty(),
        empty(),
        Optional.empty(),
        empty(),
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

  public Update<U> getUpdate() {
    return update;
  }

  public Optional<String> getAccessToken() {
    return accessToken;
  }

  public UpdateSubmission<U> withWorkflowId(String workflowId) {
    return new UpdateSubmission<U>(
        applicationId,
        commandId,
        update,
        actAs,
        readAs,
        Optional.of(workflowId),
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        accessToken);
  }

  public UpdateSubmission<U> withActAs(String actAs) {
    return new UpdateSubmission<U>(
        applicationId,
        commandId,
        update,
        List.of(actAs),
        readAs,
        workflowId,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        accessToken);
  }

  public UpdateSubmission<U> withActAs(List<@NonNull String> actAs) {
    return new UpdateSubmission<U>(
        applicationId,
        commandId,
        update,
        actAs,
        readAs,
        workflowId,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        accessToken);
  }

  public UpdateSubmission<U> withReadAs(List<@NonNull String> readAs) {
    return new UpdateSubmission<U>(
        applicationId,
        commandId,
        update,
        actAs,
        readAs,
        workflowId,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        accessToken);
  }

  public UpdateSubmission<U> withMinLedgerTimeAbs(Optional<Instant> minLedgerTimeAbs) {
    return new UpdateSubmission<U>(
        applicationId,
        commandId,
        update,
        actAs,
        readAs,
        workflowId,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        accessToken);
  }

  public UpdateSubmission<U> withMinLedgerTimeRel(Optional<Duration> minLedgerTimeRel) {
    return new UpdateSubmission<U>(
        applicationId,
        commandId,
        update,
        actAs,
        readAs,
        workflowId,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        accessToken);
  }

  public UpdateSubmission<U> withDeduplicationTime(Optional<Duration> deduplicationTime) {
    return new UpdateSubmission<U>(
        applicationId,
        commandId,
        update,
        actAs,
        readAs,
        workflowId,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        accessToken);
  }

  public UpdateSubmission<U> withAccessToken(Optional<String> accessToken) {
    return new UpdateSubmission<U>(
        applicationId,
        commandId,
        update,
        actAs,
        readAs,
        workflowId,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        accessToken);
  }
  public CommandsSubmission toCommandsSubmission() {
    return new CommandsSubmission(
      applicationId,
      commandId,
      update.commands(),
      actAs,
      readAs,
      workflowId,
      minLedgerTimeAbs,
      minLedgerTimeRel,
      deduplicationTime,
      accessToken
    );
  }
}
