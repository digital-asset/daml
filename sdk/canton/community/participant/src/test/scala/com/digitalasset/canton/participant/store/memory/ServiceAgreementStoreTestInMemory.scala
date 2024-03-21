// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.participant.store.ServiceAgreementStoreTest
import org.scalatest.wordspec.AsyncWordSpec

class ServiceAgreementStoreTestInMemory
    extends AsyncWordSpec
    with BaseTest
    with ServiceAgreementStoreTest {

  "InMemoryServiceAgreementStore" should {
    behave like serviceAgreementStore(() => new InMemoryServiceAgreementStore(loggerFactory))
  }

}
