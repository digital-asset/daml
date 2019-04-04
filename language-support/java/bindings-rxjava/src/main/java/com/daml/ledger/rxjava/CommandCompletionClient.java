// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

import com.daml.ledger.javaapi.data.CompletionEndResponse;
import com.daml.ledger.javaapi.data.CompletionStreamResponse;
import com.daml.ledger.javaapi.data.LedgerOffset;
import io.reactivex.Flowable;
import io.reactivex.Single;

import java.util.Set;

/**
 * An RxJava version of {@link com.digitalasset.ledger.api.v1.CommandCompletionServiceGrpc}
 */
public interface CommandCompletionClient {

    Flowable<CompletionStreamResponse> completionStream(String applicationId, LedgerOffset offset, Set<String> parties);

    Single<CompletionEndResponse> completionEnd();
}
