// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

import com.daml.ledger.api.v2.CommandSubmissionServiceOuterClass.SubmitResponse;
import com.daml.ledger.javaapi.data.CommandsSubmission;
import io.reactivex.Single;

/** An RxJava version of {@link com.daml.ledger.api.v2.CommandSubmissionServiceGrpc} */
@Deprecated
public interface CommandSubmissionClient {

  Single<SubmitResponse> submit(CommandsSubmission submission);
}
