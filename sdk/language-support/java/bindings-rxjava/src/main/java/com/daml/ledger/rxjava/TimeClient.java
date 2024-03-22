// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

import com.google.protobuf.Empty;
import io.reactivex.Flowable;
import io.reactivex.Single;
import java.time.Instant;

public interface TimeClient {

  Single<Empty> setTime(Instant currentTime, Instant newTime);

  Single<Empty> setTime(Instant currentTime, Instant newTime, String accessToken);

  Flowable<Instant> getTime();

  Flowable<Instant> getTime(String accessToken);
}
