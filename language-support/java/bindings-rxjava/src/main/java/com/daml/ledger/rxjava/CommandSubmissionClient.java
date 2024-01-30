// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

import com.daml.ledger.javaapi.data.CommandsSubmissionV2;
import com.daml.ledger.api.v2.CommandSubmissionServiceOuterClass.SubmitResponse;
import com.google.protobuf.Empty;
import io.reactivex.Single;

/** An RxJava version of {@link com.daml.ledger.api.v2.CommandSubmissionServiceGrpc} */
public interface CommandSubmissionClient {

  Single<SubmitResponse> submit(CommandsSubmissionV2 submission);
}
