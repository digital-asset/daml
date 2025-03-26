// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.Optional.empty;

import com.daml.ledger.javaapi.data.codegen.Update;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * This class can be used to build a valid submission for an Update. It provides {@link #create(String, String, Update)}
 * for initial creation and methods to set optional parameters
 * e.g {@link #withActAs(List)}, {@link #withWorkflowId(String)} etc.
 *
 * Usage:
 * <pre>
 *   var submission = UpdateSubmission.create(userId, commandId, update)
 *                                   .withAccessToken(token)
 *                                   .withParty(party)
 *                                   .with...
 * <pre/>
 */
public final class UpdateSubmission<U> {
  @NonNull private final String userId;
  @NonNull private final String commandId;
  @NonNull private final Update<U> update;

  @NonNull private final Optional<String> workflowId;
  @NonNull private final List<@NonNull String> actAs;
  @NonNull private final List<@NonNull String> readAs;
  @NonNull private final Optional<Instant> minLedgerTimeAbs;
  @NonNull private final Optional<Duration> minLedgerTimeRel;
  @NonNull private final Optional<Duration> deduplicationDuration;
  @NonNull private final Optional<Long> deduplicationOffset;
  @NonNull private final Optional<String> accessToken;
  @NonNull private final List<DisclosedContract> disclosedContracts;
  @NonNull private final List<@NonNull String> packageIdSelectionPreference;
  @NonNull private List<@NonNull PrefetchContractKey> prefetchContractKeys;

  @NonNull private final String synchronizerId;

  private UpdateSubmission(
      @NonNull String userId,
      @NonNull String commandId,
      @NonNull Update<U> update,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Optional<String> workflowId,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationDuration,
      @NonNull Optional<Long> deduplicationOffset,
      @NonNull Optional<String> accessToken,
      @NonNull String synchronizerId,
      @NonNull List<DisclosedContract> disclosedContracts,
      @NonNull List<@NonNull String> packageIdSelectionPreference,
      @NonNull List<@NonNull PrefetchContractKey> prefetchContractKeys) {
    this.workflowId = workflowId;
    this.userId = userId;
    this.commandId = commandId;
    this.actAs = actAs;
    this.readAs = readAs;
    this.minLedgerTimeAbs = minLedgerTimeAbs;
    this.minLedgerTimeRel = minLedgerTimeRel;
    this.deduplicationDuration = deduplicationDuration;
    this.deduplicationOffset = deduplicationOffset;
    this.update = update;
    this.accessToken = accessToken;
    this.synchronizerId = synchronizerId;
    this.disclosedContracts = disclosedContracts;
    this.packageIdSelectionPreference = packageIdSelectionPreference;
    this.prefetchContractKeys = prefetchContractKeys;
  }

  public static <U> UpdateSubmission<U> create(String userId, String commandId, Update<U> update) {
    return new UpdateSubmission<U>(
        userId,
        commandId,
        update,
        emptyList(),
        emptyList(),
        empty(),
        empty(),
        Optional.empty(),
        empty(),
        empty(),
        empty(),
        "",
        emptyList(),
        emptyList(),
        emptyList());
  }

  public Optional<String> getWorkflowId() {
    return workflowId;
  }

  public String getUserId() {
    return userId;
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

  public Optional<Duration> getDeduplicationDuration() {
    return deduplicationDuration;
  }

  public Optional<Long> getDeduplicationOffset() {
    return deduplicationOffset;
  }

  public Update<U> getUpdate() {
    return update;
  }

  public Optional<String> getAccessToken() {
    return accessToken;
  }

  public String getSynchronizerId() {
    return synchronizerId;
  }

  public @NonNull List<DisclosedContract> getDisclosedContracts() {
    return unmodifiableList(disclosedContracts);
  }

  public List<String> getPackageIdSelectionPreference() {
    return unmodifiableList(packageIdSelectionPreference);
  }

  public List<PrefetchContractKey> getPrefetchContractKeys() {
    return unmodifiableList(prefetchContractKeys);
  }

  public UpdateSubmission<U> withWorkflowId(String workflowId) {
    return new UpdateSubmission<U>(
        userId,
        commandId,
        update,
        actAs,
        readAs,
        Optional.of(workflowId),
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationDuration,
        deduplicationOffset,
        accessToken,
        synchronizerId,
        disclosedContracts,
        packageIdSelectionPreference,
        prefetchContractKeys);
  }

  public UpdateSubmission<U> withActAs(String actAs) {
    return new UpdateSubmission<U>(
        userId,
        commandId,
        update,
        List.of(actAs),
        readAs,
        workflowId,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationDuration,
        deduplicationOffset,
        accessToken,
        synchronizerId,
        disclosedContracts,
        packageIdSelectionPreference,
        prefetchContractKeys);
  }

  public UpdateSubmission<U> withActAs(List<@NonNull String> actAs) {
    return new UpdateSubmission<U>(
        userId,
        commandId,
        update,
        actAs,
        readAs,
        workflowId,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationDuration,
        deduplicationOffset,
        accessToken,
        synchronizerId,
        disclosedContracts,
        packageIdSelectionPreference,
        prefetchContractKeys);
  }

  public UpdateSubmission<U> withReadAs(List<@NonNull String> readAs) {
    return new UpdateSubmission<U>(
        userId,
        commandId,
        update,
        actAs,
        readAs,
        workflowId,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationDuration,
        deduplicationOffset,
        accessToken,
        synchronizerId,
        disclosedContracts,
        packageIdSelectionPreference,
        prefetchContractKeys);
  }

  public UpdateSubmission<U> withMinLedgerTimeAbs(Optional<Instant> minLedgerTimeAbs) {
    return new UpdateSubmission<U>(
        userId,
        commandId,
        update,
        actAs,
        readAs,
        workflowId,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationDuration,
        deduplicationOffset,
        accessToken,
        synchronizerId,
        disclosedContracts,
        packageIdSelectionPreference,
        prefetchContractKeys);
  }

  public UpdateSubmission<U> withMinLedgerTimeRel(Optional<Duration> minLedgerTimeRel) {
    return new UpdateSubmission<U>(
        userId,
        commandId,
        update,
        actAs,
        readAs,
        workflowId,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationDuration,
        deduplicationOffset,
        accessToken,
        synchronizerId,
        disclosedContracts,
        packageIdSelectionPreference,
        prefetchContractKeys);
  }

  public UpdateSubmission<U> withDeduplicationDuration(Optional<Duration> deduplicationDuration) {
    return new UpdateSubmission<U>(
        userId,
        commandId,
        update,
        actAs,
        readAs,
        workflowId,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationDuration,
        deduplicationOffset,
        accessToken,
        synchronizerId,
        disclosedContracts,
        packageIdSelectionPreference,
        prefetchContractKeys);
  }

  public UpdateSubmission<U> withDeduplicationOffset(Optional<Long> deduplicationOffset) {
    return new UpdateSubmission<U>(
        userId,
        commandId,
        update,
        actAs,
        readAs,
        workflowId,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationDuration,
        deduplicationOffset,
        accessToken,
        synchronizerId,
        disclosedContracts,
        packageIdSelectionPreference,
        prefetchContractKeys);
  }

  public UpdateSubmission<U> withAccessToken(Optional<String> accessToken) {
    return new UpdateSubmission<U>(
        userId,
        commandId,
        update,
        actAs,
        readAs,
        workflowId,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationDuration,
        deduplicationOffset,
        accessToken,
        synchronizerId,
        disclosedContracts,
        packageIdSelectionPreference,
        prefetchContractKeys);
  }

  public UpdateSubmission<U> withSynchronizerId(String synchronizerId) {
    return new UpdateSubmission<U>(
        userId,
        commandId,
        update,
        actAs,
        readAs,
        workflowId,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationDuration,
        deduplicationOffset,
        accessToken,
        this.synchronizerId,
        disclosedContracts,
        packageIdSelectionPreference,
        prefetchContractKeys);
  }

  public UpdateSubmission<U> withDisclosedContracts(
      List<@NonNull DisclosedContract> disclosedContracts) {
    return new UpdateSubmission<U>(
        userId,
        commandId,
        update,
        actAs,
        readAs,
        workflowId,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationDuration,
        deduplicationOffset,
        accessToken,
        synchronizerId,
        disclosedContracts,
        packageIdSelectionPreference,
        prefetchContractKeys);
  }

  public UpdateSubmission<U> withPackageIdSelectionPreference(
      List<@NonNull String> packageIdSelectionPreference) {
    return new UpdateSubmission<U>(
        userId,
        commandId,
        update,
        actAs,
        readAs,
        workflowId,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationDuration,
        deduplicationOffset,
        accessToken,
        synchronizerId,
        disclosedContracts,
        packageIdSelectionPreference,
        prefetchContractKeys);
  }

  public UpdateSubmission<U> withPrefetchContractKeys(
      List<PrefetchContractKey> prefetchContractKeys) {
    return new UpdateSubmission<U>(
        userId,
        commandId,
        update,
        actAs,
        readAs,
        workflowId,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationDuration,
        deduplicationOffset,
        accessToken,
        synchronizerId,
        disclosedContracts,
        packageIdSelectionPreference,
        prefetchContractKeys);
  }

  public CommandsSubmission toCommandsSubmission() {
    return new CommandsSubmission(
        workflowId,
        userId,
        commandId,
        update.commands(),
        deduplicationDuration,
        deduplicationOffset,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        actAs,
        readAs,
        empty(),
        emptyList(),
        synchronizerId,
        accessToken,
        packageIdSelectionPreference,
        prefetchContractKeys);
  }
}
