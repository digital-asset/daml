// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.{Fingerprint, Signature, SignatureFormat, TestHash}
import com.digitalasset.canton.topology.transaction.TopologyTransaction.TxHash
import com.digitalasset.canton.topology.transaction.{
  MultiTransactionSignature,
  SignedTopologyTransaction,
  SingleTransactionSignature,
}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

class MultiHashTopologyTransactionsTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  object Factory
      extends TopologyTransactionTestFactory(
        loggerFactory,
        parallelExecutionContext,
        multiHash = true,
      )

  private val transactionHash = Factory.dns1.transaction.hash

  private val multiHash = MultiTransactionSignature(
    transactionHashes = NonEmpty.mk(Set, transactionHash, TxHash(TestHash.digest("test"))),
    signature = Signature.noSignature,
  )

  private def makeSig(content: String, fingerprint: String) =
    Signature.create(
      SignatureFormat.Symbolic,
      ByteString.copyFromUtf8(content),
      Fingerprint.tryFromString(fingerprint),
      None,
    )
  "signed topology transaction" should {
    "only return valid signatures" in {
      val signature1 = makeSig("sig1", "fingerprint1")
      val signature2 = makeSig("sig2", "fingerprint2")

      val hashes = NonEmpty.mk(Set, TxHash(TestHash.digest("test")))
      val multiHash = MultiTransactionSignature(
        transactionHashes = hashes,
        signature = signature2,
      )

      val signedTx = SignedTopologyTransaction
        .create(
          Factory.dns1.transaction,
          NonEmpty.mk(Set, signature1, Signature.noSignature),
          isProposal = false,
        )(
          SignedTopologyTransaction.protocolVersionRepresentativeFor(BaseTest.testedProtocolVersion)
        )
        .addSignatures(NonEmpty.mk(Set, multiHash))

      // Critically, multiHash is not there because it does not cover the transaction
      signedTx.allUnvalidatedSignaturesCoveringHash.map(
        _.signature
      ) should contain theSameElementsAs Set(
        signature1,
        Signature.noSignature,
      )
    }

    "successfully merge single into single signature" in {
      val signedTx = SignedTopologyTransaction.create(
        Factory.dns1.transaction,
        NonEmpty.mk(Set, Signature.noSignature),
        isProposal = false,
      )(SignedTopologyTransaction.protocolVersionRepresentativeFor(BaseTest.testedProtocolVersion))

      val newSingleSignature = makeSig("new_sig", "no-fingerprint")

      signedTx.addSingleSignatures(
        NonEmpty.mk(Set, newSingleSignature)
      ) shouldBe SignedTopologyTransaction.create(
        Factory.dns1.transaction,
        NonEmpty.mk(
          Set,
          Signature.noSignature,
          newSingleSignature,
        ),
        isProposal = false,
      )(
        SignedTopologyTransaction.protocolVersionRepresentativeFor(BaseTest.testedProtocolVersion)
      )
    }

    "successfully merge multi into single signature" in {
      val signedTx = SignedTopologyTransaction.create(
        Factory.dns1.transaction,
        NonEmpty.mk(Set, Signature.noSignature),
        isProposal = false,
      )(SignedTopologyTransaction.protocolVersionRepresentativeFor(BaseTest.testedProtocolVersion))

      signedTx
        .addSignatures(NonEmpty.mk(Set, multiHash)) shouldBe SignedTopologyTransaction
        .create(
          Factory.dns1.transaction,
          NonEmpty.mk(
            Set,
            multiHash,
            SingleTransactionSignature(Factory.dns1.transaction.hash, Signature.noSignature),
          ),
          isProposal = false,
          BaseTest.testedProtocolVersion,
        )
    }

    "successfully merge single into multi signature" in {
      val signedTx = SignedTopologyTransaction
        .create(
          Factory.dns1.transaction,
          NonEmpty.mk(Set, multiHash),
          isProposal = false,
          BaseTest.testedProtocolVersion,
        )

      val newSingleSignature = makeSig("new_sig", "no-fingerprint")

      signedTx.addSingleSignatures(
        NonEmpty.mk(Set, newSingleSignature)
      ) shouldBe SignedTopologyTransaction
        .create(
          Factory.dns1.transaction,
          NonEmpty.mk(
            Set,
            multiHash,
            SingleTransactionSignature(Factory.dns1.transaction.hash, newSingleSignature),
          ),
          isProposal = false,
          BaseTest.testedProtocolVersion,
        )
    }

    "successfully merge multi into multi signature" in {
      val signedTx = SignedTopologyTransaction
        .create(
          Factory.dns1.transaction,
          NonEmpty.mk(Set, multiHash),
          isProposal = false,
          BaseTest.testedProtocolVersion,
        )

      val newSingleSignature = makeSig("new_sig", "no-fingerprint")

      val multiHash2 = MultiTransactionSignature(
        transactionHashes =
          NonEmpty.mk(Set, Factory.dns1.transaction.hash, TxHash(TestHash.digest("test"))),
        signature = newSingleSignature,
      )

      signedTx
        .addSignatures(NonEmpty.mk(Set, multiHash2)) shouldBe SignedTopologyTransaction
        .create(
          Factory.dns1.transaction,
          NonEmpty.mk(Set, multiHash, multiHash2),
          isProposal = false,
          BaseTest.testedProtocolVersion,
        )
    }
  }
}
