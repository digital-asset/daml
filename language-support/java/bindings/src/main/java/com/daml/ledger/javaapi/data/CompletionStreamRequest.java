// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.digitalasset.ledger.api.v1.CommandCompletionServiceOuterClass;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class CompletionStreamRequest {

    private final String ledgerId;

    private final String applicationId;

    private final Set<String> parties;

    private final LedgerOffset offset;

    public static CompletionStreamRequest fromProto(CommandCompletionServiceOuterClass.CompletionStreamRequest request) {
        String ledgerId = request.getLedgerId();
        String applicationId = request.getApplicationId();
        HashSet<String> parties = new HashSet<>(request.getPartiesList());
        LedgerOffset offset = LedgerOffset.fromProto(request.getOffset());
        return new CompletionStreamRequest(ledgerId, applicationId, parties, offset);
    }

    public CommandCompletionServiceOuterClass.CompletionStreamRequest toProto() {
        return CommandCompletionServiceOuterClass.CompletionStreamRequest.newBuilder()
                .setLedgerId(this.ledgerId)
                .setApplicationId(this.applicationId)
                .addAllParties(this.parties)
                .setOffset(this.offset.toProto())
                .build();
    }

    @Override
    public String toString() {
        return "CompletionStreamRequest{" +
                "ledgerId='" + ledgerId + '\'' +
                ", applicationId='" + applicationId + '\'' +
                ", parties=" + parties +
                ", offset=" + offset +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CompletionStreamRequest that = (CompletionStreamRequest) o;
        return Objects.equals(ledgerId, that.ledgerId) &&
                Objects.equals(applicationId, that.applicationId) &&
                Objects.equals(parties, that.parties) &&
                Objects.equals(offset, that.offset);
    }

    @Override
    public int hashCode() {

        return Objects.hash(ledgerId, applicationId, parties, offset);
    }

    public String getLedgerId() {

        return ledgerId;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public Set<String> getParties() {
        return parties;
    }

    public LedgerOffset getOffset() {
        return offset;
    }

    public CompletionStreamRequest(String ledgerId, String applicationId, Set<String> parties, LedgerOffset offset) {

        this.ledgerId = ledgerId;
        this.applicationId = applicationId;
        this.parties = parties;
        this.offset = offset;
    }
}
