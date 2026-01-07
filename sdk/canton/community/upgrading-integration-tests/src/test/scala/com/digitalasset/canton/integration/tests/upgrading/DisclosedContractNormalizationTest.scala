// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrading

import com.daml.logging.LoggingContext
import com.digitalasset.canton.damltests.upgrade.v2.java.upgrade.Upgrading
import com.digitalasset.canton.ledger.api.util.TimeProvider
import com.digitalasset.canton.ledger.participant.state.index.ContractStore
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.apiserver.configuration.EngineLoggingConfig
import com.digitalasset.canton.platform.apiserver.execution.{
  StoreBackedCommandInterpreter,
  TestDynamicSynchronizerParameterGetter,
}
import com.digitalasset.canton.platform.config.CommandServiceConfig
import com.digitalasset.canton.protocol.{AuthenticatedContractIdVersionV10, LfFatContractInst}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.util.{ContractValidator, TestEngine}
import com.digitalasset.canton.{BaseTest, FailOnShutdown, HasExecutionContext}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.engine.*
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.transaction.{FatContractInstance, Node, TransactionCoder}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.ContractId
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

import java.util.Optional
import scala.concurrent.ExecutionContext

class DisclosedContractNormalizationTest
    extends AsyncWordSpec
    with HasExecutionContext
    with FailOnShutdown
    with BaseTest {

  private val ec: ExecutionContext = executorService
  private implicit val loggingContext: LoggingContext = LoggingContextWithTrace(loggerFactory)

  val engine = new Engine(
    EngineConfig(allowedLanguageVersions = LanguageVersion.allLfVersionsRange)
  )

  private val testEngine = new TestEngine(packagePaths = Seq(UpgradingBaseTest.UpgradeV2))

  private def buildUpgrading(
      alice: String,
      value: Long,
  ): (Upgrading.ContractId, LfFatContractInst) = {
    val command = new Upgrading(alice, alice, value, Optional.empty()).create.commands.loneElement
    val (tx, _) = testEngine.submitAndConsume(command, alice)
    val createNode = tx.nodes.values.collect { case e: Node.Create => e }.loneElement
    val fat = testEngine.suffix(createNode)
    (new Upgrading.ContractId(fat.contractId.coid), fat)
  }

  // Simulate a (denormalized) V10 contract, starting from a current contract
  private def buildV10upgrading(
      alice: String,
      value: Long,
  ): (Upgrading.ContractId, LfFatContractInst) = {

    val (_, fat) = buildUpgrading(alice, value)

    val enrichedArg: Value =
      testEngine.enrichContract(Upgrading.TEMPLATE_ID_WITH_PACKAGE_ID, fat.createArg)

    inside(enrichedArg) { case Value.ValueRecord(_, fields) =>
      inside(fields.last._2) {
        case Value.ValueOptional(None) => succeed
        case other => fail(s"Expected last field to be an optional none, got: $other")
      }
    }

    val enrichedFat = FatContractInstance.fromCreateNode(
      fat.toCreateNode.copy(arg = enrichedArg),
      fat.createdAt,
      fat.authenticationData,
    )

    val v10contractId = AuthenticatedContractIdVersionV10.fromDiscriminator(
      fat.contractId.asInstanceOf[ContractId.V1].discriminator,
      testEngine.recomputeUnicum(enrichedFat, AuthenticatedContractIdVersionV10),
    )

    val v10fat = FatContractInstance.fromCreateNode(
      enrichedFat.toCreateNode.mapCid(_ => v10contractId),
      enrichedFat.createdAt,
      enrichedFat.authenticationData,
    )

    // Here we recode to strip any type info that does not make it into the blob
    val v10fatRecoded = TransactionCoder
      .decodeFatContractInstance(TransactionCoder.encodeFatContractInstance(v10fat).value)
      .value
      .asInstanceOf[LfFatContractInst]

    (new Upgrading.ContractId(v10contractId.coid), v10fatRecoded)
  }

  val alice = "alice"

  "disclosed contract processing of command interpretation" should {

    val validator =
      ContractValidator(testEngine.cryptoOps, testEngine.engine, testEngine.packageResolver)

    val underTest =
      new StoreBackedCommandInterpreter(
        engine = testEngine.engine,
        participant = Ref.ParticipantId.assertFromString("anId"),
        packageResolver = testEngine.packageResolver,
        contractStore = mock[ContractStore],
        metrics = LedgerApiServerMetrics.ForTesting,
        contractAuthenticator = validator.authenticateHash,
        config = EngineLoggingConfig(),
        prefetchingRecursionLevel = CommandServiceConfig.DefaultContractPrefetchingDepth,
        loggerFactory = loggerFactory,
        dynParamGetter =
          new TestDynamicSynchronizerParameterGetter(NonNegativeFiniteDuration.Zero)(ec),
        timeProvider = TimeProvider.UTC,
      )(ec)

    def interpretDisclosure(cId: Upgrading.ContractId, fat: LfFatContractInst): Assertion = {

      val command = cId.exerciseUpgrading_Fetch(alice).commands().loneElement
      val commands = testEngine.validateCommand(command, alice, disclosedContracts = Seq(fat))

      val result = underTest
        .interpret(commands, testEngine.randomHash())(
          LoggingContextWithTrace(loggerFactory),
          ec,
        )
        .futureValueUS
        .value

      val disclosedFat = result.processedDisclosedContracts.toSeq.loneElement
      disclosedFat shouldBe fat
    }

    def authenticateContract(fat: LfFatContractInst): Assertion =
      validator.authenticate(fat, fat.templateId.packageId).futureValueUS shouldBe Right(())

    val (cid, fat) = buildUpgrading(alice, 7)
    val (v10Cid, v10fat) = buildV10upgrading(alice, 7)

    "authenticate normalized contracts" in {
      authenticateContract(fat)
    }

    "authenticate unnormalized v10 contracts" in {
      authenticateContract(v10fat)
    }

    "interpret normalized contracts" in {
      interpretDisclosure(cid, fat)
    }

    "interpret unnormalized V10 contracts" in {
      interpretDisclosure(v10Cid, v10fat)
    }

  }

}
