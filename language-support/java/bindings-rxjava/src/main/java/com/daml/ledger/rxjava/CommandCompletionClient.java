// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

import com.daml.ledger.javaapi.data.CompletionStreamResponseV2;
import com.daml.ledger.javaapi.data.ParticipantOffsetV2;
import io.reactivex.Flowable;
import java.util.List;

/** An RxJava version of {@link com.daml.ledger.api.v1.CommandCompletionServiceGrpc} */
public interface CommandCompletionClient {

  Flowable<CompletionStreamResponseV2> completionStream(
      String applicationId, ParticipantOffsetV2 offset, List<String> parties);

  Flowable<CompletionStreamResponseV2> completionStream(
      String applicationId, ParticipantOffsetV2 offset, List<String> parties, String accessToken);

  Flowable<CompletionStreamResponseV2> completionStream(String applicationId, List<String> parties);

  Flowable<CompletionStreamResponseV2> completionStream(
      String applicationId, List<String> parties, String accessToken);

}
