// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

import com.daml.ledger.javaapi.data.CompletionStreamResponse;
import io.reactivex.Flowable;
import java.util.List;

/** An RxJava version of {@link com.daml.ledger.api.v2.CommandCompletionServiceGrpc} */
public interface CommandCompletionClient {

  Flowable<CompletionStreamResponse> completionStream(
      String applicationId, Long offset, List<String> parties);

  Flowable<CompletionStreamResponse> completionStream(
      String applicationId, Long offset, List<String> parties, String accessToken);

  Flowable<CompletionStreamResponse> completionStream(String applicationId, List<String> parties);

  Flowable<CompletionStreamResponse> completionStream(
      String applicationId, List<String> parties, String accessToken);
}
