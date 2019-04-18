// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.digitalasset.ledger.api.v1.CommandCompletionServiceOuterClass;
import com.digitalasset.ledger.api.v1.CompletionOuterClass;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class CompletionStreamResponse {

    private final Optional<Checkpoint> checkpoint;

    private final List<CompletionOuterClass.Completion> completions;

    public CompletionStreamResponse(Optional<Checkpoint> checkpoint, List<CompletionOuterClass.Completion> completions) {
        this.checkpoint = checkpoint;
        this.completions = completions;
    }

    public static CompletionStreamResponse fromProto(CommandCompletionServiceOuterClass.CompletionStreamResponse response) {
        if (response.hasCheckpoint()) {
            Checkpoint checkpoint = Checkpoint.fromProto(response.getCheckpoint());
            return new CompletionStreamResponse(Optional.of(checkpoint), response.getCompletionsList());
        } else {
            return new CompletionStreamResponse(Optional.empty(), response.getCompletionsList());
        }
    }

    public CommandCompletionServiceOuterClass.CompletionStreamResponse toProto() {
        CommandCompletionServiceOuterClass.CompletionStreamResponse.Builder builder = CommandCompletionServiceOuterClass.CompletionStreamResponse.newBuilder();
        this.checkpoint.ifPresent(c -> builder.setCheckpoint(c.toProto()));
        builder.addAllCompletions(this.completions);
        return builder.build();
    }


    public Optional<Checkpoint> getCheckpoint() {
        return checkpoint;
    }


    public List<CompletionOuterClass.Completion> getCompletions() {
        return completions;
    }

    @Override
    public String toString() {
        return "CompletionStreamResponse{" +
                "checkpoint=" + checkpoint +
                ", completions=" + completions +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CompletionStreamResponse that = (CompletionStreamResponse) o;
        return Objects.equals(checkpoint, that.checkpoint) &&
                Objects.equals(completions, that.completions);
    }

    @Override
    public int hashCode() {

        return Objects.hash(checkpoint, completions);
    }
}
