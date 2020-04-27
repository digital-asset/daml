// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.components.helpers;

import com.daml.ledger.javaapi.data.Identifier;
import com.daml.ledger.javaapi.data.SubmitCommandsRequest;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.pcollections.HashTreePMap;
import org.pcollections.PMap;
import org.pcollections.PSet;

import java.util.Collections;
import java.util.Objects;

/**
 * Represents the {@link SubmitCommandsRequest} together with the contracts that should be considered
 * pending while the submitCommandsRequest are processed.
 */
public class CommandsAndPendingSet {

    private final SubmitCommandsRequest submitCommandsRequest;
    private final PMap<Identifier, PSet<String>> contractIdsPendingIfSucceed;

    // we use this as "invalid" value to signal that no submitCommandsRequest have been emitted by the bot
    public final static CommandsAndPendingSet empty = new CommandsAndPendingSet(new SubmitCommandsRequest("", "",
            "", "", java.util.Optional.empty(), java.util.Optional.empty(), java.util.Optional.empty(), Collections.emptyList()),
            HashTreePMap.empty());

    public CommandsAndPendingSet(@NonNull SubmitCommandsRequest submitCommandsRequest, @NonNull PMap<Identifier, PSet<String>> contractIdsPendingIfSucceed) {
        this.submitCommandsRequest = submitCommandsRequest;
        this.contractIdsPendingIfSucceed = contractIdsPendingIfSucceed;
    }

    @NonNull
    public SubmitCommandsRequest getSubmitCommandsRequest() {
        return submitCommandsRequest;
    }

    @NonNull
    public PMap<Identifier, PSet<String>> getContractIdsPendingIfSucceed() {
        return contractIdsPendingIfSucceed;
    }

    @Override
    public String toString() {
        return "CommandsAndPendingSet{" +
                "submitCommandsRequest=" + submitCommandsRequest +
                ", contractIdsPendingIfSucceed=" + contractIdsPendingIfSucceed +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CommandsAndPendingSet that = (CommandsAndPendingSet) o;
        return Objects.equals(submitCommandsRequest, that.submitCommandsRequest) &&
                Objects.equals(contractIdsPendingIfSucceed, that.contractIdsPendingIfSucceed);
    }

    @Override
    public int hashCode() {

        return Objects.hash(submitCommandsRequest, contractIdsPendingIfSucceed);
    }
}
