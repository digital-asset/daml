// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command.interactive

import com.daml.ledger.api.v2.interactive.transaction.v1.interactive_submission_data.Node.NodeType
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.platform.apiserver.services.command.interactive.InteractiveSubmissionGenerators.*
import com.digitalasset.canton.platform.apiserver.services.command.interactive.PreparedTransactionCodec.*
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.transaction.{Node, NodeId, VersionedTransaction}
import com.digitalasset.daml.lf.value.test.ValueGenerators
import magnolify.scalacheck.auto.genArbitrary
import org.scalacheck.Arbitrary
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class PreparedTransactionCodecV1Spec
    extends AnyWordSpec
    with Matchers
    with BaseTest
    with ScalaCheckPropertyChecks
    with HasExecutionContext {

  private implicit val loggingContext: LoggingContextWithTrace = LoggingContextWithTrace.ForTesting

  private val encoder = new PreparedTransactionEncoder(loggerFactory)
  private val decoder = new PreparedTransactionDecoder(loggerFactory)

  "Prepared transaction" should {
    "round trip encode and decode any LF transaction" in {
      forAll { (transaction: VersionedTransaction, nodeSeeds: Option[ImmArray[(NodeId, Hash)]]) =>
        val result = for {
          encoded <- encoder.serializeTransaction(transaction, nodeSeeds)
          decoded <- decoder.transactionTransformer
            .transform(encoded)
            .toFutureWithLoggedFailures("Failed to decode transaction", logger)
        } yield {
          decoded shouldEqual transaction
        }

        timeouts.default.await_("Round trip")(result)
      }
    }

    "support interfaceId on exercise node" in {
      implicit val nodeGen: Arbitrary[Node.Exercise] = Arbitrary(
        for {
          exerciseNode <- ValueGenerators.danglingRefExerciseNodeGen
          normalized = normalizeNodeForV1(exerciseNode).copy(
            interfaceId = Some(ValueGenerators.idGen.sample.value)
          )
        } yield normalized
      )

      forAll { (node: Node.Exercise) =>
        val encoded =
          encoder.v1.exerciseTransformer(LanguageVersion.v2_1).transform(node).asEither.value
        decoder.v1.exerciseTransformer.transform(encoded).asEither.value shouldEqual node
      }
    }

    "support interfaceId on fetch node" in {
      implicit val nodeGen: Arbitrary[Node.Fetch] = Arbitrary(
        for {
          fetchNode <- ValueGenerators.fetchNodeGen
          normalized = normalizeNodeForV1(fetchNode).copy(
            interfaceId = Some(ValueGenerators.idGen.sample.value)
          )
        } yield normalized
      )

      forAll { (node: Node.Fetch) =>
        val encoded =
          encoder.v1.fetchTransformer(LanguageVersion.v2_1).transform(node).asEither.value
        decoder.v1.fetchTransformer.transform(encoded).asEither.value shouldEqual node
      }
    }

    "sort sets of parties" in {
      forAll { (transaction: VersionedTransaction, nodeSeeds: Option[ImmArray[(NodeId, Hash)]]) =>
        val result = for {
          encoded <- encoder.serializeTransaction(transaction, nodeSeeds)
        } yield {
          val partiesLists = encoded.nodes.flatMap {
            _.versionedNode.v1.value.nodeType match {
              case NodeType.Empty => Seq.empty
              case NodeType.Create(value) => Seq(value.signatories, value.stakeholders)
              case NodeType.Fetch(value) =>
                Seq(value.signatories, value.stakeholders, value.actingParties)
              case NodeType.Exercise(value) =>
                Seq(
                  value.signatories,
                  value.stakeholders,
                  value.actingParties,
                  value.choiceObservers,
                )
              case NodeType.Rollback(_) => Seq.empty
            }
          }
          partiesLists.foreach { partiesList =>
            partiesList.sorted should contain theSameElementsInOrderAs (partiesList)
          }
        }

        timeouts.default.await_("Round trip")(result)
      }
    }
  }
}
