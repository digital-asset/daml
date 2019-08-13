// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

import com.digitalasset.ledger.api.v1.LedgerConfigurationServiceOuterClass;
import io.reactivex.Flowable;

/**
 * An RxJava version of {@link com.digitalasset.ledger.api.v1.LedgerConfigurationServiceGrpc}
 */
public interface LedgerConfigurationClient {

    Flowable<LedgerConfigurationServiceOuterClass.LedgerConfiguration> getLedgerConfiguration();
}
