// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.util

import com.daml.ledger.javaapi.data.codegen.ContractId
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, TestHash}
import com.digitalasset.canton.protocol.{
  AuthenticatedContractIdVersionV10,
  ExampleTransactionFactory,
  Unicum,
}
import org.scalatest.wordspec.AsyncWordSpec

class JavaCodegenUtilTest extends AsyncWordSpec with BaseTest {
  "JavaCodegenUtil" when {
    "converting between API and LF types" should {
      import com.digitalasset.canton.participant.util.JavaCodegenUtil.ContractIdSyntax
      "work both ways" in {
        val discriminator = ExampleTransactionFactory.lfHash(1)
        val hash =
          Hash.build(TestHash.testHashPurpose, HashAlgorithm.Sha256).add(0).finish()
        val unicum = Unicum(hash)
        val lfCid = AuthenticatedContractIdVersionV10.fromDiscriminator(discriminator, unicum)

        val apiCid = new ContractId(lfCid.coid)
        val lfCid2 = apiCid.toLf

        lfCid2 shouldBe lfCid
      }
    }
  }
}
