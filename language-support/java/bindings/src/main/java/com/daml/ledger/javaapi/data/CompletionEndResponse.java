// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.digitalasset.ledger.api.v1.CommandCompletionServiceOuterClass;

import java.util.Objects;

public final class CompletionEndResponse {

    private final LedgerOffset offset;

    public CompletionEndResponse(LedgerOffset offset) {
        this.offset = offset;
    }

    public static CompletionEndResponse fromProto(CommandCompletionServiceOuterClass.CompletionEndResponse response) {
        return new CompletionEndResponse(LedgerOffset.fromProto(response.getOffset()));
    }

    public LedgerOffset getOffset() {
        return offset;
    }

    @Override
    public String toString() {
        return "CompletionEndResponse{" +
                "offset=" + offset +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CompletionEndResponse that = (CompletionEndResponse) o;
        return Objects.equals(offset, that.offset);
    }

    @Override
    public int hashCode() {

        return Objects.hash(offset);
    }
}
