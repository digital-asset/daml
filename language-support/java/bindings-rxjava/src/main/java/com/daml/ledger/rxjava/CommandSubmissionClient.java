// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

import com.daml.ledger.javaapi.data.CommandsSubmission;
import com.google.protobuf.Empty;
import io.reactivex.Single;

/** An RxJava version of {@link com.daml.ledger.api.v1.CommandSubmissionServiceGrpc} */
public interface CommandSubmissionClient {

  Single<Empty> submit(CommandsSubmission submission);
}
