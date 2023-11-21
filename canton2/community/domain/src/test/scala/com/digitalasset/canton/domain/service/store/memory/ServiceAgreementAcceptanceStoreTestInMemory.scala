// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.service.store.memory

import com.digitalasset.canton.domain.service.store.ServiceAgreementAcceptanceStoreTest
import org.scalatest.wordspec.AsyncWordSpec

class ServiceAgreementAcceptanceStoreTestInMemory
    extends AsyncWordSpec
    with ServiceAgreementAcceptanceStoreTest {
  "InMemoryServiceAgreementAcceptanceStore" should {
    behave like serviceAgreementAcceptanceStore(
      new InMemoryServiceAgreementAcceptanceStore(loggerFactory)
    )
  }

}
