// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_17

import com.daml.ledger.api.testtool.infrastructure.Allocation.{
  Participant,
  Participants,
  SingleParty,
  allocate,
}
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers.{
  createdEvents,
  exercisedEvents,
}
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.infrastructure.{Dars, LedgerTestSuite}
import com.daml.ledger.api.testtool.suites.v1_17.UpgradingIT.EnrichedCommands
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsRequest
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.v1.transaction_filter.{
  Filters,
  InclusiveFilters,
  TemplateFilter,
  TransactionFilter,
}
import com.daml.ledger.api.v1.transaction_service.GetTransactionsRequest
import com.daml.ledger.api.v1.value.Identifier.toJavaProto
import com.daml.ledger.api.v1.value.{Identifier => ScalaPbIdentifier}
import com.daml.ledger.api.v1.{transaction, value}
import com.daml.ledger.javaapi.data.codegen.{ContractCompanion, ValueDecoder}
import com.daml.ledger.javaapi.data.{DamlRecord, Unit => _, _}
import com.daml.ledger.test.java.upgrade.v1_0_0.upgrade.{UA => UA_V1}
import com.daml.ledger.test.java.upgrade.v2_0_0.upgrade.{UA => UA_V2, UB => UB_V2}
import com.daml.ledger.test.java.upgrade.v3_0_0.upgrade.{UB => UB_V3}
import com.daml.ledger.test.java.upgrade_fetch.v1_0_0.upgradefetch.{Fetcher => FetcherV1, Fetch}
import com.daml.ledger.test.java.upgrade_fetch.v2_0_0.upgradefetch.{Fetcher => FetcherV2}
import com.daml.ledger.test.{UpgradeFetchTestDar1_0_0, UpgradeFetchTestDar2_0_0}
import com.daml.ledger.test.{UpgradeTestDar1_0_0, UpgradeTestDar2_0_0, UpgradeTestDar3_0_0}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{PackageName, PackageRef}

import java.util.Optional
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

class UpgradingIT extends LedgerTestSuite {
  implicit val upgradingUA_V1Companion
      : ContractCompanion.WithoutKey[UA_V1.Contract, UA_V1.ContractId, UA_V1] =
    UA_V1.COMPANION
  implicit val upgradingUA_V2Companion
      : ContractCompanion.WithoutKey[UA_V2.Contract, UA_V2.ContractId, UA_V2] =
    UA_V2.COMPANION
  implicit val upgradingUB_V2Companion
      : ContractCompanion.WithoutKey[UB_V2.Contract, UB_V2.ContractId, UB_V2] =
    UB_V2.COMPANION
  implicit val upgradingUB_V3Companion
      : ContractCompanion.WithoutKey[UB_V3.Contract, UB_V3.ContractId, UB_V3] =
    UB_V3.COMPANION

  implicit val upgradingFetcherV1_Companion
      : ContractCompanion.WithoutKey[FetcherV1.Contract, FetcherV1.ContractId, FetcherV1] =
    FetcherV1.COMPANION

  private val PkgNameRef = PackageRef.Name(PackageName.assertFromString("upgrade-tests"))
  private val UA_Identifier = ScalaPbIdentifier
    .fromJavaProto(UA_V1.TEMPLATE_ID.toProto)
    .withPackageId(PkgNameRef.toString)
  private val UB_Identifier = ScalaPbIdentifier
    .fromJavaProto(UB_V2.TEMPLATE_ID.toProto)
    .withPackageId(PkgNameRef.toString)

  private val PkgRefId_UA_V1 =
    PackageRef.Id(Ref.PackageId.assertFromString(UA_V1.PACKAGE_ID))

  test(
    "USubscriptionsUnknownPackageNames",
    "Subscriptions are failed if created for package names that are not known to the participant",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    import ledger._

    val UnknownPackageNameIdentifier = ScalaPbIdentifier.of(
      Ref.PackageRef.Name(Ref.PackageName.assertFromString("unknownPkgName")).toString,
      "module",
      "entity",
    )

    for {
      failedFlatTransactionsPackageNameNotFound <- flatTransactions(
        txRequest(ledger, UnknownPackageNameIdentifier, party, continuous = true)
      ).mustFail("Package-name not found")
      failedActiveContractsPackageNameNotFound <- activeContracts(
        getActiveContractsRequest(ledger, UnknownPackageNameIdentifier, party)
      ).mustFail("Package-name not found")
    } yield {

      // TODO(#16651): Switch to asserting error codes as well
      assert(
        failedFlatTransactionsPackageNameNotFound.getMessage.contains(
          "The following package names do not match upgradable packages uploaded on this participant: [unknownPkgName]."
        )
      )
      assert(
        failedActiveContractsPackageNameNotFound.getMessage.contains(
          "The following package names do not match upgradable packages uploaded on this participant: [unknownPkgName]."
        )
      )
    }
  })

  test(
    "USubscriptionsNoTemplatesForPackageName",
    "Subscriptions are failed if created for package names that have no known templates with the specified qualified name",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    import ledger._

    val knownPackageName = "model-tests"
    val unknownIdentifierQualifiedNameForPackageName = ScalaPbIdentifier.of(
      Ref.PackageRef.Name(Ref.PackageName.assertFromString(knownPackageName)).toString,
      "unknownModule",
      "unknownEntity",
    )

    for {
      failedFlatTransactionsPackageNameNotFound <- flatTransactions(
        txRequest(ledger, unknownIdentifierQualifiedNameForPackageName, party, continuous = true)
      ).mustFail("Template not found")
      failedActiveContractsPackageNameNotFound <- activeContracts(
        getActiveContractsRequest(ledger, unknownIdentifierQualifiedNameForPackageName, party)
      ).mustFail("Template not found")
    } yield {

      assert(
        failedFlatTransactionsPackageNameNotFound.getMessage.contains(
          "The following package-name/template qualified-name pairs do not reference any template-id uploaded on this participant: [(model-tests,unknownModule:unknownEntity)]."
        )
      )
      assert(
        failedActiveContractsPackageNameNotFound.getMessage.contains(
          "The following package-name/template qualified-name pairs do not reference any template-id uploaded on this participant: [(model-tests,unknownModule:unknownEntity)]."
        )
      )
    }
  })

  test(
    "UDynamicTemplates",
    "Template-id resolution is updated on package upload during ongoing subscriptions",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      // Upload 1.0.0 package
      _ <- upload(ledger, UpgradeTestDar1_0_0.path)

      // Start ongoing UA subscriptions
      subscriptions_UA_no_blob = new Subscriptions(
        "UA without created event blob filter",
        ledger,
        party,
        UA_Identifier,
        includeCreatedEventBlob = false,
        expectedCreatesSize = 5,
      )
      subscriptions_UA_blob = new Subscriptions(
        "UA with created event blob filter",
        ledger,
        party,
        UA_Identifier,
        includeCreatedEventBlob = true,
        expectedCreatesSize = 5,
      )

      // Create UA#1: UA 1.0.0 contract arguments and use package-name scoped type in command
      payloadUA_1 = new UA_V1(party, party, 0L)
      _ <- createContract(ledger, party, payloadUA_1, Some(PkgNameRef))

      // Create UA#2: UA 2.0.0 contract arguments and use explicit downgrade type (Upgrading V1) in command
      payloadUA_2 = new UA_V2(party, party, 0L, Optional.empty())
      _ <- createContract(ledger, party, payloadUA_2, Some(PkgRefId_UA_V1))

      // Upload 2.0.0 package
      // 2.0.0 becomes the default package preference on the ledger
      _ <- upload(ledger, UpgradeTestDar2_0_0.path)

      // Start ongoing UB subscriptions
      subscriptions_UB_no_blob = new Subscriptions(
        "UB without createdEventBlob filter",
        ledger,
        party,
        UB_Identifier,
        includeCreatedEventBlob = false,
        expectedCreatesSize = 2,
      )
      subscriptions_UB_blob = new Subscriptions(
        "UB with createdEventBlob filter",
        ledger,
        party,
        UB_Identifier,
        includeCreatedEventBlob = true,
        expectedCreatesSize = 2,
      )

      // Create UA#3: UA 1.0.0 contract arguments and use package-name scoped type in command
      //            expecting an upgrade to V2
      payloadUA_3 = new UA_V1(party, party, 0L)
      _ <- createContract(ledger, party, payloadUA_3, Some(PkgNameRef))

      // Create UA#4: UA 2.0.0 contract arguments and use package-name scoped type in command
      //            expecting a record on ledger of V2
      payloadUA_4 = new UA_V2(party, party, 0L, Optional.of(Seq("more").asJava))
      _ <- createContract(ledger, party, payloadUA_4, Some(PkgNameRef))

      // Create UA#5: UA 1.0.0 contract arguments with its default type in command
      payloadUA_5 = new UA_V1(party, party, 0L)
      _ <- createContract(ledger, party, payloadUA_5)

      // Create UB#1: UB 2.0.0 contract arguments with its default type in command
      payloadUB_1 = new UB_V2(party, 0L)
      _ <- createContract(ledger, party, payloadUB_1)

      // Upload 3.0.0 package
      // 3.0.0 becomes the default package preference on the ledger
      _ <- upload(ledger, UpgradeTestDar3_0_0.path)

      // Create UB#2: UB 3.0.0 contract arguments with its default type in command
      payloadUB_2 = new UB_V3(party, 0L, Optional.of(Seq("extra").asJava))
      _ <- createContract(ledger, party, payloadUB_2)

      // Wait for all UA transactions to be visible in the transaction streams
      creates_UA_noBlob <- subscriptions_UA_no_blob.createsF
      creates_UA_blob <- subscriptions_UA_blob.createsF

      // Wait for all UB transactions to be visible in the transaction streams
      creates_UB_noBlob <- subscriptions_UB_no_blob.createsF
      creates_UB_blob <- subscriptions_UB_blob.createsF
    } yield {
      def assertUACreates(expectedCreatedEventBlob: Boolean): Vector[CreatedEvent] => Unit = {
        case Vector(create1, create2, create3, create4, create5) =>
          assertPayloadEquals(
            "UA create 1",
            create1,
            payloadUA_1,
            UA_V1.valueDecoder(),
            UA_V1.TEMPLATE_ID_WITH_PACKAGE_ID,
            expectedCreatedEventBlob,
          )
          assertPayloadEquals(
            "UA create 2",
            create2,
            new UA_V1(party, party, 0L),
            UA_V1.valueDecoder(),
            UA_V1.TEMPLATE_ID_WITH_PACKAGE_ID,
            expectedCreatedEventBlob,
          )
          assertPayloadEquals(
            "UA create 3",
            create3,
            new UA_V2(party, party, 0L, Optional.empty()),
            UA_V2.valueDecoder(),
            UA_V2.TEMPLATE_ID_WITH_PACKAGE_ID,
            expectedCreatedEventBlob,
          )
          assertPayloadEquals(
            "UA create 4",
            create4,
            payloadUA_4,
            UA_V2.valueDecoder(),
            UA_V2.TEMPLATE_ID_WITH_PACKAGE_ID,
            expectedCreatedEventBlob,
          )
          assertPayloadEquals(
            "UA create 5",
            create5,
            payloadUA_5,
            // The default is now to create the contract with a package name in the template id
            // So the contract ends up being interpreted as the V2 version.
            UA_V2.valueDecoder(),
            UA_V2.TEMPLATE_ID_WITH_PACKAGE_ID,
            expectedCreatedEventBlob,
          )
        case other => fail(s"Expected five create events, got ${other.size}")
      }

      def assertUBCreates(expectedCreatedEventBlob: Boolean): Vector[CreatedEvent] => Unit = {
        case Vector(create1, create2) =>
          assertPayloadEquals(
            "UB create 1",
            create1,
            payloadUB_1,
            UB_V2.valueDecoder(),
            UB_V2.TEMPLATE_ID_WITH_PACKAGE_ID,
            expectedCreatedEventBlob,
          )
          assertPayloadEquals(
            "UB create 2",
            create2,
            payloadUB_2,
            UB_V3.valueDecoder(),
            UB_V3.TEMPLATE_ID_WITH_PACKAGE_ID,
            expectedCreatedEventBlob,
          )
        case other => fail(s"Expected two create events, got ${other.size}")
      }

      assertUACreates(expectedCreatedEventBlob = false)(creates_UA_noBlob)
      assertUACreates(expectedCreatedEventBlob = true)(creates_UA_blob)

      assertUBCreates(expectedCreatedEventBlob = false)(creates_UB_noBlob)
      assertUBCreates(expectedCreatedEventBlob = true)(creates_UB_blob)

      // TODO(#16651): Check for transaction trees as well in 3.x
    }
  })

  test(
    "ChoicePackageId",
    "Populate choicePackageId with package of the choice exercised, and template id with package with which contract was created",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      // v1 is the only package for Fetcher
      _ <- upload(ledger, UpgradeFetchTestDar1_0_0.path)

      cid <- ledger.create(party, new FetcherV1(party))

      // Exercised as per v1 implementation of choice
      _ <- ledger.exercise(party, cid.exerciseFetch(new Fetch(cid)))

      // v2 becomes the default package for Fetcher
      _ <- upload(ledger, UpgradeFetchTestDar2_0_0.path)

      // Exercised as per v2 implementation of choice
      _ <- ledger.exercise(party, cid.exerciseFetch(new Fetch(cid)))

      Vector(exercised1, exercised2) <- ledger
        .transactionTrees(party)
        .map(_.flatMap(exercisedEvents))
    } yield {
      val v1TmplId = FetcherV1.TEMPLATE_ID_WITH_PACKAGE_ID
      val v2TmplId = FetcherV2.TEMPLATE_ID_WITH_PACKAGE_ID

      // Both exercise events's template-id should match the create template-id (i.e. v1)
      assertEquals(toJavaProto(exercised1.templateId.get), v1TmplId.toProto)
      // TODO(i21823): Uncomment
      // assertEquals(toJavaProto(exercised2.templateId.get), v1TmplId.toProto)

      // The first exercise has a result shape per v1, and the second per v2
      assertExerciseResult(exercised1.exerciseResult.get, v1TmplId, new FetcherV1(party))
      assertExerciseResult(
        exercised2.exerciseResult.get,
        v2TmplId,
        new FetcherV2(party, Optional.empty()),
      )

      // The first exercise has a choicePackageId of v1 and the second of v2
      // TODO(i21913): Uncomment
      // assertEquals(toJavaProto(ex1.choicePackageId.get), FetcherV1.PACKAGE_ID.toProto)
      // assertEquals(toJavaProto(ex2.choicePackageId.get), FetcherV2.PACKAGE_ID.toProto)
    }
  })

  private class Subscriptions(
      context: String,
      ledger: ParticipantTestContext,
      party: Party,
      filterIdentifier: ScalaPbIdentifier,
      includeCreatedEventBlob: Boolean,
      expectedCreatesSize: Int,
  )(implicit ec: ExecutionContext) {
    import ledger._

    private val flatTxsF: Future[Vector[transaction.Transaction]] = flatTransactions(
      take = expectedCreatesSize,
      txRequest(
        ledger,
        filterIdentifier,
        party,
        continuous = true,
        includeCreatedEventBlob = includeCreatedEventBlob,
      ),
    )

    def createsF: Future[Vector[CreatedEvent]] = {
      val acsAtQueryTime = activeContracts(
        getActiveContractsRequest(ledger, filterIdentifier, party, includeCreatedEventBlob)
      )

      flatTxsF.map(_.flatMap(createdEvents)) zip acsAtQueryTime
        .map(_._2) map { case (flatTxs, acsCreates) =>
        val flatTxsCreates = flatTxs
        assertLength(s"$context: Flat transactions creates", expectedCreatesSize, flatTxsCreates)
        assertLength(s"$context: ACS creates", expectedCreatesSize, acsCreates)
        assertSameElements(flatTxsCreates, acsCreates)
        acsCreates
      }
    }
  }

  private def assertExerciseResult[T <: Template](
      got: value.Value,
      wantRecordId: Identifier,
      wantPayload: T,
  ): Unit = {
    val gotPb = value.Value.toJavaProto(got)
    val wantPb = new DamlRecord(wantRecordId, wantPayload.toValue.getFields).toProto
    assertEquals(gotPb, wantPb)
  }

  private def haveSamePopulatedFields[A <: Template, B <: Template](a: A, b: B) = {
    val aFields = a.toValue.getFields.asScala
    val bFields = b.toValue.getFields.asScala
    val count = aFields.size min bFields.size
    def fieldIsNone(f: DamlRecord.Field) = f.getValue.equals(DamlOptional.EMPTY)
    aFields.slice(0, count).equals(bFields.slice(0, count)) &&
    aFields.slice(count, aFields.size).forall(fieldIsNone) &&
    bFields.slice(count, bFields.size).forall(fieldIsNone)
  }

  private def assertPayloadEquals[I <: Template, O <: Template](
      context: String,
      createdEvent: CreatedEvent,
      payload: I,
      valueDecoder: ValueDecoder[O],
      templateId: Identifier,
      expectedCreatedEventBlob: Boolean,
  ): Unit = {
    assertEquals(context, toJavaProto(createdEvent.templateId.get), templateId.toProto)

    assert(
      haveSamePopulatedFields(
        valueDecoder.decode(
          DamlRecord.fromProto(value.Record.toJavaProto(createdEvent.getCreateArguments))
        ),
        payload,
      ),
      context,
    )

    if (expectedCreatedEventBlob)
      assert(!createdEvent.createdEventBlob.isEmpty, s"$context: createdEventBlob was empty")
    else assert(createdEvent.createdEventBlob.isEmpty, s"$context: createdEventBlob was non-empty")
  }

  private def upload(ledger: ParticipantTestContext, darPath: String)(implicit
      ec: ExecutionContext
  ): Future[Unit] =
    ledger
      .uploadDarFile(Dars.read(darPath))
      // Wait for the package vetting topology transaction to finish
      .map(_ => Thread.sleep(1000L))

  private def createContract(
      ledger: ParticipantTestContext,
      party: Party,
      template: Template,
      overrideTypeO: Option[Ref.PackageRef] = None,
  ): Future[Unit] = {
    val commands = template.create().commands()

    ledger.submitAndWait(
      ledger.submitAndWaitRequest(
        party,
        overrideTypeO
          .map(overrideType => commands.overridePackageId(overrideType.toString))
          .getOrElse(commands),
      )
    )
  }

  private def getActiveContractsRequest(
      ledger: ParticipantTestContext,
      identifier: ScalaPbIdentifier,
      party: Party,
      includeCreatedEventBlobs: Boolean = false,
  ) =
    GetActiveContractsRequest(
      ledgerId = ledger.ledgerId,
      filter = Some(transactionFilter(identifier, party, includeCreatedEventBlobs)),
      verbose = true,
    )

  private def txRequest(
      ledger: ParticipantTestContext,
      identifier: ScalaPbIdentifier,
      party: Party,
      continuous: Boolean,
      includeCreatedEventBlob: Boolean = false,
  ): GetTransactionsRequest =
    new GetTransactionsRequest(
      ledgerId = ledger.ledgerId,
      begin = Some(ledger.begin),
      end = if (continuous) None else Some(ledger.end),
      filter = Some(transactionFilter(identifier, party, includeCreatedEventBlob)),
      verbose = true,
    )

  private def transactionFilter(
      identifier: ScalaPbIdentifier,
      party: Party,
      includeCreatedEventBlobs: Boolean,
  ) =
    TransactionFilter(
      filtersByParty = Map(
        party.getValue -> Filters(
          Some(
            InclusiveFilters(
              templateFilters = Seq(
                TemplateFilter(Some(identifier), includeCreatedEventBlob = includeCreatedEventBlobs)
              )
            )
          )
        )
      )
    )
}

object UpgradingIT {
  implicit class EnrichedCommands(commands: java.util.List[Command]) {
    def overridePackageId(packageIdOverride: String): java.util.List[Command] =
      commands.asScala
        .map {
          case cmd: CreateCommand =>
            new CreateCommand(
              identifierWithPackageIdOverride(packageIdOverride, cmd.getTemplateId),
              cmd.getCreateArguments,
            ): Command
          case other => fail(s"Unexpected command $other")
        }
        .toList
        .asJava
  }

  private def identifierWithPackageIdOverride(packageIdOverride: String, templateId: Identifier) =
    new Identifier(
      packageIdOverride,
      templateId.getModuleName,
      templateId.getEntityName,
    )
}
