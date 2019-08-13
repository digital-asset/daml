// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

import com.google.protobuf.Empty;
import io.reactivex.Flowable;
import io.reactivex.Single;

import java.time.Instant;

public interface TimeClient {

    Single<Empty> setTime(Instant currentTime, Instant newTime);

    Flowable<Instant> getTime();
}
