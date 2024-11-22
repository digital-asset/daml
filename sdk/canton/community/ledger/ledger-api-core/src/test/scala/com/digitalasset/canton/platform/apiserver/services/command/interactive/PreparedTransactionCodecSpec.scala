// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command.interactive

import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.platform.apiserver.services.command.interactive.PreparedTransactionCodec.*
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import com.digitalasset.daml.lf.crypto
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.transaction.{Node, NodeId, VersionedTransaction}
import com.digitalasset.daml.lf.value.test.ValueGenerators
import com.digitalasset.daml.lf.value.test.ValueGenerators.*
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class PreparedTransactionCodecSpec
    extends AnyWordSpec
    with Matchers
    with BaseTest
    with ScalaCheckPropertyChecks
    with HasExecutionContext {

  private implicit val loggingContext: LoggingContextWithTrace = LoggingContextWithTrace.ForTesting

  private val encoder = new PreparedTransactionEncoder(loggerFactory)
  private val decoder = new PreparedTransactionDecoder(loggerFactory)

  // Node generators that filter out fields not supported in LF 2.1
  private val createTransactionGen = ValueGenerators
    .malformedCreateNodeGen()
    .map(
      _.copy(
        packageVersion = None,
        keyOpt = None,
      )
    )
  private val fetchNodeGen = ValueGenerators.fetchNodeGen.map(
    _.copy(
      keyOpt = None,
      byKey = false,
      interfaceId = None,
    )
  )
  private val exerciseNodeGen = ValueGenerators.danglingRefExerciseNodeGen.map(
    _.copy(
      keyOpt = None,
      byKey = false,
      choiceAuthorizers = None,
    )
  )
  private val rollbackNodeGen = ValueGenerators.danglingRefRollbackNodeGen
  private val nodeGenerator = Gen.frequency[Node](
    1 -> createTransactionGen,
    1 -> fetchNodeGen,
    1 -> exerciseNodeGen,
    1 -> rollbackNodeGen,
  )
  private val nodeIdGen = Arbitrary.arbInt.arbitrary.map(NodeId(_))
  private val nodeWithNodeIdGen = for {
    nodeId <- nodeIdGen
    node <- nodeGenerator
  } yield (nodeId, node)

  private val transactionGenerator = for {
    version <- transactionVersionGen()
    nodes <- Gen.listOf(nodeWithNodeIdGen).map(_.toMap)
    roots <- Gen.listOf(nodeIdGen).map(ImmArray.from)
  } yield VersionedTransaction(version, nodes, roots)

  private implicit val transactionArb: Arbitrary[VersionedTransaction] = Arbitrary(
    transactionGenerator
  )

  private implicit val genHash: Gen[crypto.Hash] =
    Gen
      .containerOfN[Array, Byte](
        crypto.Hash.underlyingHashLength,
        arbitrary[Byte],
      ) map crypto.Hash.assertFromByteArray

  private implicit val nodeSeed: Gen[(NodeId, Hash)] = for {
    nodeId <- nodeIdGen
    hash <- genHash
  } yield (nodeId, hash)

  private implicit val nodeSeedsGen: Gen[Option[ImmArray[(NodeId, Hash)]]] = for {
    seeds <- Gen.listOf(nodeSeed).map(ImmArray.from)
    optSeeds <- Gen.option(seeds)
  } yield optSeeds

  private implicit val nodeSeedsArbitrary: Arbitrary[Option[ImmArray[(NodeId, Hash)]]] = Arbitrary(
    nodeSeedsGen
  )

  "Prepared transaction codecs" should {
    "round trip any LF transaction" in {
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
  }
}
