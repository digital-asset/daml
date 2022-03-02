// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.util;

import com.daml.grpc.adapter.ExecutionSequencerFactory;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.reactivex.Single;
import org.checkerframework.checker.nullness.qual.Nullable;

public class CreateSingle {

  public static <Resp> Single<Resp> fromFuture(
      ListenableFuture<Resp> listenableFuture, ExecutionSequencerFactory esf) {
    return Single.create(
        emitter ->
            Futures.addCallback(
                listenableFuture,
                new FutureCallback<>() {
                  @Override
                  public void onSuccess(@Nullable Resp resp) {
                    emitter.onSuccess(resp);
                  }

                  @Override
                  public void onFailure(Throwable throwable) {
                    emitter.onError(throwable);
                  }
                },
                esf.getExecutionSequencer()::sequence));
  }
}
