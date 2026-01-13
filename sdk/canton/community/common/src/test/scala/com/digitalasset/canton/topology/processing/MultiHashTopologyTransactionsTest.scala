// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
    signature = makeSig("multi-sig", "multi-sig-fingerprint"),
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
        .withSignatures(
          Factory.dns1.transaction,
          NonEmpty.mk(Seq, signature1, Signature.noSignature),
          isProposal = false,
          testedProtocolVersion,
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
      val signedTx = SignedTopologyTransaction.withSignatures(
        Factory.dns1.transaction,
        NonEmpty.mk(Seq, Signature.noSignature),
        isProposal = false,
        testedProtocolVersion,
      )

      val newSingleSignature = makeSig("new_sig", "no-fingerprint-2")

      signedTx.addSingleSignatures(
        NonEmpty.mk(Set, newSingleSignature)
      ) shouldBe SignedTopologyTransaction.withSignatures(
        Factory.dns1.transaction,
        NonEmpty.mk(
          Seq,
          Signature.noSignature,
          newSingleSignature,
        ),
        isProposal = false,
        testedProtocolVersion,
      )
    }

    "successfully merge multi into single signature" in {
      val signedTx = SignedTopologyTransaction.withSignatures(
        Factory.dns1.transaction,
        NonEmpty.mk(Seq, Signature.noSignature),
        isProposal = false,
        testedProtocolVersion,
      )

      signedTx
        .addSignatures(NonEmpty.mk(Set, multiHash)) shouldBe SignedTopologyTransaction
        .withTopologySignatures(
          Factory.dns1.transaction,
          NonEmpty.mk(
            Seq,
            multiHash,
            SingleTransactionSignature(Factory.dns1.transaction.hash, Signature.noSignature),
          ),
          isProposal = false,
          BaseTest.testedProtocolVersion,
        )
    }

    "successfully merge single into multi signature" in {
      val signedTx = SignedTopologyTransaction
        .withTopologySignatures(
          Factory.dns1.transaction,
          NonEmpty.mk(Seq, multiHash),
          isProposal = false,
          BaseTest.testedProtocolVersion,
        )

      val newSingleSignature = makeSig("new_sig", "no-fingerprint-2")

      signedTx.addSingleSignatures(
        NonEmpty.mk(Set, newSingleSignature)
      ) shouldBe SignedTopologyTransaction
        .withTopologySignatures(
          Factory.dns1.transaction,
          NonEmpty.mk(
            Seq,
            multiHash,
            SingleTransactionSignature(Factory.dns1.transaction.hash, newSingleSignature),
          ),
          isProposal = false,
          BaseTest.testedProtocolVersion,
        )
    }

    "successfully merge multi into multi signature" in {
      val signedTx = SignedTopologyTransaction
        .withTopologySignatures(
          Factory.dns1.transaction,
          NonEmpty.mk(Seq, multiHash),
          isProposal = false,
          BaseTest.testedProtocolVersion,
        )

      val newSingleSignature = makeSig("new_sig", "no-fingerprint-2")

      val multiHash2 = MultiTransactionSignature(
        transactionHashes =
          NonEmpty.mk(Set, Factory.dns1.transaction.hash, TxHash(TestHash.digest("test"))),
        signature = newSingleSignature,
      )

      signedTx
        .addSignatures(NonEmpty.mk(Set, multiHash2)) shouldBe SignedTopologyTransaction
        .withTopologySignatures(
          Factory.dns1.transaction,
          NonEmpty.mk(Seq, multiHash, multiHash2),
          isProposal = false,
          BaseTest.testedProtocolVersion,
        )
    }
  }
}
