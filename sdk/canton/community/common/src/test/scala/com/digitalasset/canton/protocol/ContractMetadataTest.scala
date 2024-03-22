// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AnyWordSpec

class ContractMetadataTest extends AnyWordSpec with BaseTest {

  "ContractMetadata" must {
    "reject creation" when {
      "maintainers are not signatories" in {
        val err = leftOrFail(
          ContractMetadata.create(
            Set.empty,
            Set(ExampleTransactionFactory.submitter),
            Some(
              ExampleTransactionFactory.globalKeyWithMaintainers(
                maintainers = Set(ExampleTransactionFactory.submitter)
              )
            ),
          )
        )("non-signatory maintainer")
        err should include("Maintainers are not signatories: ")
      }

      "signatories are not stakeholders" in {
        val err = leftOrFail(
          ContractMetadata.create(Set(ExampleTransactionFactory.submitter), Set.empty, None)
        )("non-stakeholder signatory")
        err should include("Signatories are not stakeholders: ")
      }
    }

    "deserialize to what it was serialized from" in {
      val metadata = ContractMetadata.tryCreate(
        Set(ExampleTransactionFactory.submitter, ExampleTransactionFactory.signatory),
        Set(
          ExampleTransactionFactory.submitter,
          ExampleTransactionFactory.signatory,
          ExampleTransactionFactory.observer,
        ),
        Some(
          ExampleTransactionFactory.globalKeyWithMaintainers(
            maintainers = Set(ExampleTransactionFactory.submitter)
          )
        ),
      )
      val serialization = metadata.toProtoVersioned(testedProtocolVersion)

      ContractMetadata.fromProtoVersioned(serialization) shouldBe Right(metadata)
    }
  }
}
