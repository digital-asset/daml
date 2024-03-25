// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.HashPurpose
import com.digitalasset.canton.protocol.TransferDomainId
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.mutable

class SignedProtocolMessageContentTest extends AnyWordSpec with BaseTest {

  "hashPurpose" must {
    "be different for each subclass" in {

      val subclasses = Seq[SignedProtocolMessageContent](
        mock[MediatorResponse],
        mock[TransactionResultMessage],
        mock[TransferResult[TransferDomainId]],
        mock[AcsCommitment],
        mock[MalformedMediatorRequestResult],
      )

      subclasses.foreach(subclass => when(subclass.hashPurpose).thenCallRealMethod())

      val hashPurposes = mutable.Set.empty[HashPurpose]
      subclasses.foreach { subclass =>
        val hashPurpose = subclass.hashPurpose
        assert(
          hashPurposes.add(hashPurpose),
          s"Clash on hash purpose ID $hashPurpose for ${HashPurpose.description(hashPurpose)}",
        )
      }
    }
  }

}
