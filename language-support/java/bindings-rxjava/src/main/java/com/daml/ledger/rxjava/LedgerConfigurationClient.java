// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

import com.daml.ledger.api.v1.LedgerConfigurationServiceOuterClass;
import io.reactivex.Flowable;

/** An RxJava version of {@link com.daml.ledger.api.v1.LedgerConfigurationServiceGrpc} */
public interface LedgerConfigurationClient {

  Flowable<LedgerConfigurationServiceOuterClass.LedgerConfiguration> getLedgerConfiguration();

  Flowable<LedgerConfigurationServiceOuterClass.LedgerConfiguration> getLedgerConfiguration(
      String accessToken);
}
