// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache

import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

// TODO LLP: Extract unit tests for `push` from [[MutableCacheBackedContractStoreSpec]]
class ContractStateCachesSpec extends AnyFlatSpec with Matchers with MockitoSugar {
  private val loggingContext = LoggingContext.ForTesting
  behavior of classOf[ContractStateCaches].getSimpleName

  it should "reset the caches on `reset`" in {
    val keyStateCache = mock[StateCache[Key, ContractKeyStateValue]]
    val contractStateCache = mock[StateCache[ContractId, ContractStateValue]]

    val contractStateCaches = new ContractStateCaches(
      keyStateCache,
      contractStateCache,
    )(loggingContext)

    val someOffset = Offset.fromHexString(Ref.HexString.assertFromString("aabbcc"))

    contractStateCaches.reset(someOffset)
    verify(keyStateCache).reset(someOffset)
    verify(contractStateCache).reset(someOffset)
  }
}
