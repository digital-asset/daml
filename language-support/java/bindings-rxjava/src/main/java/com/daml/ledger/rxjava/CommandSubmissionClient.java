// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

import com.daml.ledger.javaapi.data.Command;
import com.google.protobuf.Empty;
import io.reactivex.Single;

import java.time.Instant;
import java.util.List;

/**
 * An RxJava version of {@link com.digitalasset.ledger.api.v1.CommandSubmissionServiceGrpc}
 */
public interface CommandSubmissionClient {

    Single<Empty> submit(String workflowId,
                         String applicationId,
                         String commandId,
                         String party,
                         Instant ledgerEffectiveTime,
                         Instant maximumRecordTime,
                         List<Command> commands);
}
