// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.suites.v2_2

import com.daml.ledger.api.testtool.infrastructure.Allocation.{
  Participant,
  Participants,
  SingleParty,
  allocate,
}
import com.daml.ledger.api.testtool.infrastructure.Assertions.{verifyLength, *}
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers.{
  createdEvents,
  exercisedEvents,
}
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.infrastructure.{
  Dars,
  LedgerTestSuite,
  Party,
  UpgradeFetchTestDar1_0_0,
  UpgradeFetchTestDar2_0_0,
  UpgradeIfaceDar,
  UpgradeTestDar1_0_0,
  UpgradeTestDar2_0_0,
  UpgradeTestDar3_0_0,
}
import com.daml.ledger.api.testtool.suites.v2_2.UpgradingIT.{
  EnrichedIdentifier,
  SubInterface,
  SubTemplate,
  Subscriptions,
  acsF,
  createContract,
  getActiveContractsRequest,
  txRequest,
  upload,
}
import com.daml.ledger.api.v2.event.Event.Event.Created
import com.daml.ledger.api.v2.event.{CreatedEvent, Event}
import com.daml.ledger.api.v2.event_query_service.GetEventsByContractIdRequest
import com.daml.ledger.api.v2.state_service.GetActiveContractsRequest
import com.daml.ledger.api.v2.transaction_filter.CumulativeFilter.IdentifierFilter
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.{
  TRANSACTION_SHAPE_ACS_DELTA,
  TRANSACTION_SHAPE_LEDGER_EFFECTS,
}
import com.daml.ledger.api.v2.transaction_filter.{
  CumulativeFilter,
  EventFormat,
  Filters,
  InterfaceFilter,
  TemplateFilter,
  TransactionFormat,
  UpdateFormat,
}
import com.daml.ledger.api.v2.update_service.GetUpdatesRequest
import com.daml.ledger.api.v2.value.Identifier as ScalaPbIdentifier
import com.daml.ledger.api.v2.value.Identifier.toJavaProto
import com.daml.ledger.api.v2.{transaction, transaction_filter, value}
import com.daml.ledger.javaapi.data.codegen.{ContractCompanion, ContractId, ValueDecoder}
import com.daml.ledger.javaapi.data.{DamlRecord, Unit as _, *}
import com.daml.ledger.test.java.model.test.Dummy
import com.daml.ledger.test.java.upgrade_1_0_0.upgrade.UA as UA_V1
import com.daml.ledger.test.java.upgrade_2_0_0.upgrade.{UA as UA_V2, UB as UB_V2}
import com.daml.ledger.test.java.upgrade_3_0_0.upgrade.{UA as UA_V3, UB as UB_V3}
import com.daml.ledger.test.java.upgrade_fetch_1_0_0.upgradefetch.{Fetch, Fetcher as Fetcher_V1}
import com.daml.ledger.test.java.upgrade_fetch_2_0_0.upgradefetch.Fetcher as Fetcher_V2
import com.daml.ledger.test.java.upgrade_iface.iface
import com.digitalasset.canton.ledger.api.TransactionShape.LedgerEffects
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{PackageId, PackageRef, TypeConRef}
import org.scalatest.Inside.inside

import java.util.Optional
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*

class UpgradingIT extends LedgerTestSuite {
  PackageRef.Id(PackageId.assertFromString(UA_V1.PACKAGE_ID))
  PackageRef.Id(PackageId.assertFromString(UB_V2.PACKAGE_ID))
  PackageRef.Id(PackageId.assertFromString(UB_V3.PACKAGE_ID))
  private val Iface1_Ref = ScalaPbIdentifier.fromJavaProto(iface.Iface1.TEMPLATE_ID.toProto)
  private val UA_Ref = ScalaPbIdentifier.fromJavaProto(UA_V1.TEMPLATE_ID.toProto)
  private val UB_Ref = ScalaPbIdentifier.fromJavaProto(UB_V2.TEMPLATE_ID.toProto)

  test(
    "USubscriptionsUnknownPackageNames",
    "Subscriptions are failed if created for package names that are not known to the participant",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    import ledger.*

    val TemplateSubscription =
      SubTemplate(TypeConRef.assertFromString("#unknownPkgName:module:entity"))

    for {
      failedFlatTransactionsPackageNameNotFound <- transactions(
        txRequest(ledger, TemplateSubscription, party)
      ).mustFail("Package-name not found")
      end <- ledger.currentEnd()
      failedActiveContractsPackageNameNotFound <- activeContracts(
        getActiveContractsRequest(TemplateSubscription, party, end)
      ).mustFail("Package-name not found")
      failedTransactionLedgerEffectsPackageNameNotFound <- transactions(
        txRequest(
          ledger = ledger,
          subscriptionFilter = TemplateSubscription,
          party = party,
          transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
        )
      ).mustFail("Package-name not found")
    } yield {

      assertGrpcError(
        failedFlatTransactionsPackageNameNotFound,
        RequestValidationErrors.NotFound.PackageNamesNotFound,
        Some(
          "The following package names do not match upgradable packages uploaded on this participant: [unknownPkgName]."
        ),
      )
      assertGrpcError(
        failedActiveContractsPackageNameNotFound,
        RequestValidationErrors.NotFound.PackageNamesNotFound,
        Some(
          "The following package names do not match upgradable packages uploaded on this participant: [unknownPkgName]."
        ),
      )
      assertGrpcError(
        failedTransactionLedgerEffectsPackageNameNotFound,
        RequestValidationErrors.NotFound.PackageNamesNotFound,
        Some(
          "The following package names do not match upgradable packages uploaded on this participant: [unknownPkgName]."
        ),
      )
    }
  })

  test(
    "USubscriptionsNoTemplatesForPackageName",
    "Subscriptions are failed if created for package names that have no known templates with the specified qualified name",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    import ledger.*

    val knownPackageName = Dummy.PACKAGE_NAME
    val unknownSubscriptionQualifiedNameForPackageName = SubTemplate(
      TypeConRef.assertFromString(s"#$knownPackageName:unknownModule:unknownEntity")
    )

    for {
      failedFlatTransactionsPackageNameNotFound <- transactions(
        txRequest(ledger, unknownSubscriptionQualifiedNameForPackageName, party)
      ).mustFail("Template not found")
      end <- ledger.currentEnd()
      failedActiveContractsPackageNameNotFound <- activeContracts(
        getActiveContractsRequest(unknownSubscriptionQualifiedNameForPackageName, party, end)
      ).mustFail("Template not found")
      failedTransactionLedgerEffectsPackageNameNotFound <- transactions(
        txRequest(
          ledger = ledger,
          subscriptionFilter = unknownSubscriptionQualifiedNameForPackageName,
          party = party,
          transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
        )
      ).mustFail("Template not found")
    } yield {

      assertGrpcError(
        failedFlatTransactionsPackageNameNotFound,
        RequestValidationErrors.NotFound.NoTemplatesForPackageNameAndQualifiedName,
        Some(
          "The following package-name/template qualified-name pairs do not reference any template-id uploaded on this participant: [(model-tests,unknownModule:unknownEntity)]."
        ),
      )
      assertGrpcError(
        failedActiveContractsPackageNameNotFound,
        RequestValidationErrors.NotFound.NoTemplatesForPackageNameAndQualifiedName,
        Some(
          "The following package-name/template qualified-name pairs do not reference any template-id uploaded on this participant: [(model-tests,unknownModule:unknownEntity)]."
        ),
      )
      assertGrpcError(
        failedTransactionLedgerEffectsPackageNameNotFound,
        RequestValidationErrors.NotFound.NoTemplatesForPackageNameAndQualifiedName,
        Some(
          "The following package-name/template qualified-name pairs do not reference any template-id uploaded on this participant: [(model-tests,unknownModule:unknownEntity)]."
        ),
      )
    }
  })

  // TODO(#25385): Add assertions for transaction pointwise queries
  test(
    "UDynamicSubscriptions",
    "Template-id and interface-id resolution is updated on package upload during ongoing subscriptions",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    implicit val upgradingUA_V1Companion
        : ContractCompanion.WithoutKey[UA_V1.Contract, UA_V1.ContractId, UA_V1] =
      UA_V1.COMPANION
    implicit val upgradingUA_V2Companion
        : ContractCompanion.WithoutKey[UA_V2.Contract, UA_V2.ContractId, UA_V2] =
      UA_V2.COMPANION
    implicit val upgradingUA_V3Companion
        : ContractCompanion.WithoutKey[UA_V3.Contract, UA_V3.ContractId, UA_V3] =
      UA_V3.COMPANION
    implicit val upgradingUB_V2Companion
        : ContractCompanion.WithoutKey[UB_V2.Contract, UB_V2.ContractId, UB_V2] =
      UB_V2.COMPANION
    implicit val upgradingUB_V3Companion
        : ContractCompanion.WithoutKey[UB_V3.Contract, UB_V3.ContractId, UB_V3] =
      UB_V3.COMPANION

    for {
      _ <- upload(ledger, UpgradeIfaceDar.path)
      // Upload 1.0.0 package (with the first implementation of UA)
      _ <- upload(ledger, UpgradeTestDar1_0_0.path)

      // TODO(#16651): Assert that subscriptions fail if subscribing for non-existing template-name
      //               but for known package-name

      // Start ongoing Iface1 subscriptions after uploading the V1 package
      subscriptions_Iface1_at_v1 = new Subscriptions(
        "Iface1 at v1",
        ledger,
        party,
        SubInterface(
          Iface1_Ref,
          expectedTemplatesInResponses = Set(UA_Ref.toTypeConRef),
        ),
        expectedTxsSize = 3,
      )

      // Start ongoing UA subscriptions
      subscriptions_UA = new Subscriptions(
        "UA",
        ledger,
        party,
        SubTemplate(UA_Ref.toTypeConRef),
        expectedTxsSize = 4,
      )

      // Create UA#1: UA 1.0.0 contract arguments
      payloadUA_1 = new UA_V1(party, party, 0L)
      payloadUA_1_contractId <- createContract(ledger, party, payloadUA_1)

      fetchPayloadUA1_byContractId = () =>
        ledger
          .getEventsByContractId(
            GetEventsByContractIdRequest(
              payloadUA_1_contractId.contractId,
              Some(
                ledger.eventFormat(
                  verbose = true,
                  Some(Seq(party)),
                  interfaceFilters = Seq(iface.Iface1.TEMPLATE_ID -> true),
                )
              ),
            )
          )

      acs_before_v2_upload <- acsF(ledger, party, SubInterface(Iface1_Ref))
      _ <- fetchPayloadUA1_byContractId()
        .mustFailWith("contract not found", RequestValidationErrors.NotFound.ContractEvents)
      subscriptions_Iface1_at_v1_after_v1_create = new Subscriptions(
        "Iface1 after v1 create",
        ledger,
        party,
        SubInterface(
          Iface1_Ref,
          expectedTemplatesInResponses = Set(UA_Ref.toTypeConRef),
        ),
        expectedTxsSize = 3,
      )

      // Upload 2.0.0 package (with the first implementation of UB)
      // 2.0.0 becomes the default package preference on the ledger
      _ <- upload(ledger, UpgradeTestDar2_0_0.path)
      acs_after_v2_upload <- acsF(ledger, party, SubInterface(Iface1_Ref))
      create1_fetched_by_contract_id_after_v2_upload <- fetchPayloadUA1_byContractId()

      subscriptions_Iface1_at_v2 = new Subscriptions(
        "Iface1 at v2",
        ledger,
        party,
        SubInterface(
          Iface1_Ref,
          expectedTemplatesInResponses = Set(UA_Ref.toTypeConRef),
        ),
        expectedTxsSize = 4,
      )

      // Start ongoing UB subscriptions
      subscriptions_UB = new Subscriptions(
        "UB",
        ledger,
        party,
        SubTemplate(UB_Ref.toTypeConRef),
        expectedTxsSize = 2,
      )

      // Create UA#2: UA 2.0.0 contract
      payloadUA_2 = new UA_V2(party, party, 0L, Optional.of(Seq("more").asJava))
      _ <- createContract(ledger, party, payloadUA_2)

      // Create UA#3: UA 1.0.0 (by means of explicit package-id)
      payloadUA_3 = new UA_V1(party, party, 0L)
      _ <- createContract(
        ledger,
        party,
        payloadUA_3,
        Some(PackageRef.Id(Ref.PackageId.assertFromString(UA_V1.PACKAGE_ID))),
      )

      acs_before_v3_upload <- acsF(ledger, party, SubInterface(Iface1_Ref))

      // Create UB#1: UB 2.0.0 contract
      payloadUB_1 = new UB_V2(party, 0L)
      _ <- createContract(ledger, party, payloadUB_1)

      // Upload 3.0.0 package
      // 3.0.0 becomes the default package preference on the ledger
      _ <- upload(ledger, UpgradeTestDar3_0_0.path)
      acs_after_v3_upload <- acsF(ledger, party, SubInterface(Iface1_Ref))
      create1_fetched_by_contract_id_after_v3_upload <- fetchPayloadUA1_byContractId()

      // Create UA#4: UA 3.0.0 contract
      payloadUA_4 = new UA_V3(party, party, 0L, Optional.of(Seq("more").asJava))
      _ <- createContract(ledger, party, payloadUA_4)

      acs_after_create_4 <- acsF(ledger, party, SubInterface(Iface1_Ref))

      subscriptions_Iface1_at_v3 = new Subscriptions(
        "Iface1 at v3",
        ledger,
        party,
        SubInterface(
          Iface1_Ref,
          expectedTemplatesInResponses = Set(UA_Ref.toTypeConRef),
        ),
        expectedTxsSize = 4,
      )

      // Create UB#2: UB 3.0.0 contract
      payloadUB_2 = new UB_V3(party, 0L, Optional.of(Seq("extra").asJava))
      _ <- createContract(ledger, party, payloadUB_2)

      // Wait for all UA transactions to be visible in the transaction streams
      creates_UA <- subscriptions_UA.createsF(4)

      // Wait for all UB transactions to be visible in the transaction streams
      creates_UB <- subscriptions_UB.createsF(2)

      creates_subscriptions_Iface1_at_v1 <- subscriptions_Iface1_at_v1.transactionsF(3)

      creates_subscriptions_Iface1_at_v1_after_v1_create <-
        subscriptions_Iface1_at_v1_after_v1_create.transactionsF(3)

      creates_subscriptions_Iface1_at_v2 <- subscriptions_Iface1_at_v2.transactionsF(4)

      creates_subscriptions_Iface1_at_v3 <- subscriptions_Iface1_at_v3.transactionsF(4)
    } yield {
      def assertCreate[
          TCid <: ContractId[T],
          T <: Template,
      ](
          payload: T,
          context: String,
          create: CreatedEvent,
          expectedInterfaceViewValue: Option[String] = None,
      )(implicit companion: ContractCompanion[?, TCid, T]): Unit =
        assertPayloadEquals(
          context,
          create,
          payload,
          ContractCompanion.valueDecoder(companion),
          companion.TEMPLATE_ID_WITH_PACKAGE_ID,
          assertCreateArgs = expectedInterfaceViewValue.isEmpty,
          expectedInterfaceViewValue.toList
            .map(expectedViewValue =>
              Identifier.fromProto(iface.Iface1.TEMPLATE_ID_WITH_PACKAGE_ID.toProto) -> {
                (record: DamlRecord) =>
                  assertEquals(
                    s"Iface1 view for create 1 - $context",
                    iface.Iface1View.valueDecoder().decode(record).name,
                    expectedViewValue,
                  )
              }
            )
            .toMap,
        )

      // Assert GetEventsByContractId
      assertCreate(
        payload = payloadUA_1,
        context = "GetEventsByContractId at v2",
        create = create1_fetched_by_contract_id_after_v2_upload.getCreated.getCreatedEvent,
        expectedInterfaceViewValue = Some("Iface1-UAv2"),
      )

      assertCreate(
        payload = payloadUA_1,
        context = "GetEventsByContractId at v3",
        create = create1_fetched_by_contract_id_after_v3_upload.getCreated.getCreatedEvent,
        expectedInterfaceViewValue = Some("Iface1-UAv3"),
      )

      // Assert interface subscriptions
      inside(creates_subscriptions_Iface1_at_v1) { case Vector(create2, create3, create4) =>
        assertCreate(payloadUA_2, "2 - IFace1 subscription at v1", create2, Some("Iface1-UAv2"))
        assertCreate(payloadUA_3, "3 - IFace1 subscription at v1", create3, Some("Iface1-UAv2"))
        assertCreate(payloadUA_4, "4 - IFace1 subscription at v1", create4, Some("Iface1-UAv2"))
      }

      inside(creates_subscriptions_Iface1_at_v1_after_v1_create) {
        case Vector(create2, create3, create4) =>
          assertCreate(
            payloadUA_2,
            "2 - IFace1 subscription after v1 create",
            create2,
            Some("Iface1-UAv2"),
          )
          assertCreate(
            payloadUA_3,
            "3 - IFace1 subscription after v1 create",
            create3,
            Some("Iface1-UAv2"),
          )
          assertCreate(
            payloadUA_4,
            "4 - IFace1 subscription after v1 create",
            create4,
            Some("Iface1-UAv2"),
          )
      }

      inside(creates_subscriptions_Iface1_at_v2) {
        case Vector(create1, create2, create3, create4) =>
          assertCreate(payloadUA_1, "1 - Iface1 subscription at v2", create1, Some("Iface1-UAv2"))
          assertCreate(payloadUA_2, "2 - Iface1 subscription at v2", create2, Some("Iface1-UAv2"))
          assertCreate(payloadUA_3, "3 - Iface1 subscription at v2", create3, Some("Iface1-UAv2"))
          assertCreate(payloadUA_4, "4 - Iface1 subscription at v2", create4, Some("Iface1-UAv2"))
      }

      inside(creates_subscriptions_Iface1_at_v3) {
        case Vector(create1, create2, create3, create4) =>
          assertCreate(payloadUA_1, "1 - Iface1 subscription at v3", create1, Some("Iface1-UAv3"))
          assertCreate(payloadUA_2, "2 - Iface1 subscription at v3", create2, Some("Iface1-UAv3"))
          assertCreate(payloadUA_3, "3 - Iface1 subscription at v3", create3, Some("Iface1-UAv3"))
          assertCreate(payloadUA_4, "4 - Iface1 subscription at v2", create4, Some("Iface1-UAv3"))
      }

      // Assert ACS interface subscription
      assertIsEmpty(acs_before_v2_upload)
      inside(acs_after_v2_upload) { case Vector(create1) =>
        assertCreate(payloadUA_1, "1 - ACS after v2 upload", create1, Some("Iface1-UAv2"))
      }
      inside(acs_before_v3_upload) { case Vector(create1, create2, create3) =>
        assertCreate(payloadUA_1, "1 - ACS after create 3", create1, Some("Iface1-UAv2"))
        assertCreate(payloadUA_2, "2 - ACS after create 3", create2, Some("Iface1-UAv2"))
        assertCreate(payloadUA_3, "3 - ACS after create 3", create3, Some("Iface1-UAv2"))
      }
      inside(acs_after_v3_upload) { case Vector(create1, create2, create3) =>
        assertCreate(payloadUA_1, "1 - ACS after v3 upload", create1, Some("Iface1-UAv3"))
        assertCreate(payloadUA_2, "2 - ACS after v3 upload", create2, Some("Iface1-UAv3"))
        assertCreate(payloadUA_3, "3 - ACS after v3 upload", create3, Some("Iface1-UAv3"))
      }
      inside(acs_after_create_4) { case Vector(create1, create2, create3, create4) =>
        assertCreate(payloadUA_1, "1 - ACS after create 4", create1, Some("Iface1-UAv3"))
        assertCreate(payloadUA_2, "2 - ACS after create 4", create2, Some("Iface1-UAv3"))
        assertCreate(payloadUA_3, "3 - ACS after create 4", create3, Some("Iface1-UAv3"))
        assertCreate(payloadUA_4, "4 - ACS after create 4", create4, Some("Iface1-UAv3"))
      }

      // Assert template subscriptions
      inside(creates_UA) {
        case Vector(create1, create2, create3, create4) =>
          assertCreate(payloadUA_1, "UA create 1", create1)
          assertCreate(payloadUA_2, "UA create 2", create2)
          assertCreate(payloadUA_3, "UA create 3", create3)
          assertCreate(payloadUA_4, "UA create 4", create4)
        case other => fail(s"Expected 4 create events, got ${other.size}")
      }

      inside(creates_UB) {
        case Vector(create1, create2) =>
          assertCreate(payloadUB_1, "UB create 1", create1)
          assertCreate(payloadUB_2, "UB create 2", create2)
        case other => fail(s"Expected two create events, got ${other.size}")
      }
    }
  })

  test(
    "UPackageNameInEvents",
    "Package-name is populated for Ledger API events",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val dummy = new Dummy(party)
    implicit val dummyCompanion
        : ContractCompanion.WithoutKey[Dummy.Contract, Dummy.ContractId, Dummy] =
      Dummy.COMPANION

    val dummyTemplateSubscriptions = new Subscriptions(
      "Dummy template",
      ledger,
      party,
      SubTemplate(Dummy.TEMPLATE_ID.toV1.toTypeConRef),
      expectedTxsSize = 2,
    )
    for {
      dummy <- createContract(ledger, party, dummy)
      _ <- ledger.submitAndWait(
        ledger.submitAndWaitRequest(party, dummy.exerciseDummyNonConsuming().commands())
      )
      acsBeforeArchive <- acsF(
        ledger = ledger,
        party = party,
        subscriptionFilter = SubTemplate(Dummy.TEMPLATE_ID.toV1.toTypeConRef),
      )
      _ <- ledger.submitAndWait(
        ledger.submitAndWaitRequest(party, dummy.exerciseArchive().commands())
      )
      acsDeltaTxs <- dummyTemplateSubscriptions.acsDeltaTxsF
      ledgerEffectsTxs <- dummyTemplateSubscriptions.ledgerEffectsTxsF
    } yield {
      verifyLength("Dummy template ACS before archive", 1, acsBeforeArchive)
      assertEquals(
        "Dummy template package name",
        acsBeforeArchive.head.packageName,
        Dummy.PACKAGE_NAME,
      )

      verifyLength("Acs Delta transactions for Dummy template", 2, acsDeltaTxs)
      verifyLength("Ledger Effects transactions for Dummy template", 2, ledgerEffectsTxs)
      acsDeltaTxs
        .flatMap(
          _.events.flatMap(ev =>
            ev.event.created
              .map(_.packageName)
              .toList ++ ev.event.archived.map(_.packageName).toList
          )
        )
        .foreach(packageName =>
          assertEquals("Package name in event", packageName, Dummy.PACKAGE_NAME)
        )

      ledgerEffectsTxs
        .flatMap(_.events)
        .flatMap(event =>
          event.event.created.map(_.packageName).toList ++ event.event.exercised
            .map(_.packageName)
            .toList
        )
        .foreach(packageName =>
          assertEquals("Package name in event", packageName, Dummy.PACKAGE_NAME)
        )
    }
  })

  test(
    "URepresentativePackageIdInEvents",
    "The representative package-id is populated for Ledger API create events",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val dummy = new Dummy(party)
    implicit val dummyCompanion
        : ContractCompanion.WithoutKey[Dummy.Contract, Dummy.ContractId, Dummy] =
      Dummy.COMPANION

    val dummyTemplateSubscriptions = new Subscriptions(
      "Dummy template",
      ledger,
      party,
      SubTemplate(Dummy.TEMPLATE_ID.toV1.toTypeConRef),
      expectedTxsSize = 1,
    )
    for {
      _ <- createContract(ledger, party, dummy)
      acs <- acsF(
        ledger = ledger,
        party = party,
        subscriptionFilter = SubTemplate(Dummy.TEMPLATE_ID.toV1.toTypeConRef),
      )
      acsDeltaTxs <- dummyTemplateSubscriptions.acsDeltaTxsF
      ledgerEffectsTxs <- dummyTemplateSubscriptions.ledgerEffectsTxsF
    } yield {
      assertEquals(
        "ACS created event representative package-id",
        assertSingleton("Only one create in the ACS", acs).representativePackageId,
        // For create events stemming from command submissions,
        // the representative package-id is the same as the contract's package-id
        Dummy.PACKAGE_ID,
      )

      val acsDeltaTx = assertSingleton("Acs Delta transactions for Dummy template", acsDeltaTxs)
      val ledgerFxTx =
        assertSingleton("Ledger Effects transactions for Dummy template", ledgerEffectsTxs)
      assertEquals(
        "ACS delta TX created event representative package-id",
        assertSingleton(
          "ACS delta events",
          acsDeltaTx.events,
        ).event.created.get.representativePackageId,
        Dummy.PACKAGE_ID,
      )

      assertEquals(
        "Ledger Effects TX created event representative package-id",
        assertSingleton(
          "Ledger Effects events",
          ledgerFxTx.events,
        ).event.created.get.representativePackageId,
        Dummy.PACKAGE_ID,
      )
    }
  })

  test(
    "UChoicePackageId",
    "Report package id of the template exercised and use it to render the result",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    implicit val upgradingFetcherV1_Companion
        : ContractCompanion.WithoutKey[Fetcher_V1.Contract, Fetcher_V1.ContractId, Fetcher_V1] =
      Fetcher_V1.COMPANION
    for {
      // v1 is the only package for Fetcher
      _ <- upload(ledger, UpgradeFetchTestDar1_0_0.path)

      cid <- ledger.create(party, new Fetcher_V1(party))

      // Exercised as per v1 implementation of choice
      _ <- ledger.exercise(party, cid.exerciseFetch(new Fetch(cid)))

      // v2 becomes the default package for Fetcher
      _ <- upload(ledger, UpgradeFetchTestDar2_0_0.path)

      // Exercised as per v2 implementation of choice
      _ <- ledger.exercise(party, cid.exerciseFetch(new Fetch(cid)))

      case Vector(exercised1, exercised2) <- ledger
        .transactions(LedgerEffects, party)
        .map(_.flatMap(exercisedEvents))
    } yield {
      val v1TmplId = Fetcher_V1.TEMPLATE_ID_WITH_PACKAGE_ID
      val v2TmplId = Fetcher_V2.TEMPLATE_ID_WITH_PACKAGE_ID

      // The first exercise reports template with package id per v1, and the second per v2
      assertEquals(toJavaProto(exercised1.templateId.get), v1TmplId.toProto)
      assertEquals(toJavaProto(exercised2.templateId.get), v2TmplId.toProto)

      // The first exercise has a result shape per v1, and the second per v2
      assertExerciseResult(exercised1.exerciseResult.get, v1TmplId, new Fetcher_V1(party))
      assertExerciseResult(
        exercised2.exerciseResult.get,
        v2TmplId,
        new Fetcher_V2(party, Optional.empty()),
      )
    }
  })

  // We ignore recordId as it is not transferred over to Json API
  private def assertExerciseResult[T <: Template](
      got: value.Value,
      recordIdIfGiven: Identifier,
      wantPayload: T,
  ): Unit = {
    import com.daml.ledger.api.testtool.infrastructure.RemoveTrailingNone.Implicits
    val gotPb = value.Value.toJavaProto(got)
    val wantFields = wantPayload.toValue.withoutTrailingNoneFields.getFields
    val wantPb =
      if (!gotPb.getRecord.hasRecordId)
        new DamlRecord(wantFields).toProto
      else
        new DamlRecord(recordIdIfGiven, wantFields).toProto

    assertEquals(gotPb, wantPb)
  }

  private def assertPayloadEquals[T](
      context: String,
      createdEvent: CreatedEvent,
      payload: T,
      valueDecoder: ValueDecoder[T],
      templateId: Identifier,
      assertCreateArgs: Boolean,
      assertViewDecoding: Map[Identifier, DamlRecord => Unit],
  ): Unit = {
    assertEquals(context, toJavaProto(createdEvent.templateId.get), templateId.toProto)

    if (assertCreateArgs)
      assertEquals(
        context,
        valueDecoder.decode(
          DamlRecord.fromProto(value.Record.toJavaProto(createdEvent.getCreateArguments))
        ),
        payload,
      )

    createdEvent.interfaceViews.foreach { ifaceView =>
      val viewRecord = DamlRecord.fromProto(value.Record.toJavaProto(ifaceView.getViewValue))
      assertViewDecoding(Identifier.fromProto(toJavaProto(ifaceView.getInterfaceId)))(viewRecord)
    }
  }
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

  private def acsF(
      ledger: ParticipantTestContext,
      party: Party,
      subscriptionFilter: TemplateOrInterfaceWithImpls,
  )(implicit executionContext: ExecutionContext) =
    for {
      end <- ledger.currentEnd()
      acs <- ledger.activeContracts(
        getActiveContractsRequest(subscriptionFilter, party, end)
      )
    } yield acs

  private class Subscriptions(
      context: String,
      ledger: ParticipantTestContext,
      party: Party,
      subscriptionFilter: TemplateOrInterfaceWithImpls,
      expectedTxsSize: Int,
  )(implicit ec: ExecutionContext) {
    import ledger.*

    val acsDeltaTxsF: Future[Vector[transaction.Transaction]] = transactions(
      take = expectedTxsSize,
      txRequest(
        ledger,
        subscriptionFilter,
        party,
      ),
    )

    val ledgerEffectsTxsF: Future[Vector[transaction.Transaction]] = transactions(
      take = expectedTxsSize,
      txRequest(
        ledger,
        subscriptionFilter,
        party,
        transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
      ),
    )

    def transactionsF(
        expectedCreatesSize: Int
    ): Future[Vector[CreatedEvent]] =
      for {
        acsDeltaTxs <- acsDeltaTxsF
        ledgerEffectsTxs <- ledgerEffectsTxsF
        acsDeltaTxCreates = acsDeltaTxs.flatMap(createdEvents)
        ledgerEffectsTxCreates = ledgerEffectsTxs
          .flatMap(createdEvents)
          .filter { created =>
            subscriptionFilter.expectedTemplatesInResponses
              .exists(_ matches (created.templateId.get, created.packageName))
          }
      } yield {
        verifyLength(
          s"$context: Acs Delta transactions creates",
          expectedCreatesSize,
          acsDeltaTxCreates,
        )
        verifyLength(
          s"$context: Ledger Effects transactions creates",
          expectedCreatesSize,
          ledgerEffectsTxCreates,
        )
        assertSameElements(
          actual = acsDeltaTxCreates,
          expected = ledgerEffectsTxCreates,
          context = context,
        )

        ledgerEffectsTxCreates
      }

    def createsF(expectedCreatesSize: Int): Future[Vector[CreatedEvent]] =
      for {
        acsCreates <- acsF(ledger, party, subscriptionFilter)
        txCreates <- transactionsF(expectedCreatesSize)
      } yield {
        verifyLength(s"$context: ACS creates", expectedCreatesSize, acsCreates)

        assertSameElements(txCreates, acsCreates)

        acsCreates
      }
  }

  def createContract[
      TCid <: ContractId[T],
      T <: Template,
  ](
      ledger: ParticipantTestContext,
      party: Party,
      template: T,
      overrideTypeO: Option[Ref.PackageRef] = None,
  )(implicit companion: ContractCompanion[?, TCid, T], ec: ExecutionContext): Future[TCid] = {
    val commands = template.create().commands()

    ledger
      .submitAndWaitForTransaction(
        ledger.submitAndWaitForTransactionRequest(
          party,
          overrideTypeO
            .map(overrideType => commands.overridePackageId(overrideType.toString))
            .getOrElse(commands),
        )
      )
      .map(
        _.getTransaction.events
          .collectFirst { case Event(Created(e)) =>
            companion.toContractId(new ContractId(e.contractId))
          }
          .getOrElse(fail("No contract created"))
      )
  }

  private def getActiveContractsRequest(
      subscriptionFilter: TemplateOrInterfaceWithImpls,
      party: Party,
      activeAtOffset: Long,
      includeCreatedEventBlobs: Boolean = false,
  ) =
    GetActiveContractsRequest(
      activeAtOffset = activeAtOffset,
      eventFormat = Some(eventFormat(subscriptionFilter, party, includeCreatedEventBlobs)),
    )

  def txRequest(
      ledger: ParticipantTestContext,
      subscriptionFilter: TemplateOrInterfaceWithImpls,
      party: Party,
      includeCreatedEventBlob: Boolean = false,
      transactionShape: transaction_filter.TransactionShape = TRANSACTION_SHAPE_ACS_DELTA,
  ): GetUpdatesRequest =
    new GetUpdatesRequest(
      beginExclusive = ledger.begin,
      endInclusive = None,
      updateFormat = Some(
        UpdateFormat(
          includeTransactions = Some(
            TransactionFormat(
              eventFormat = Some(
                eventFormat(subscriptionFilter, party, includeCreatedEventBlob)
              ),
              transactionShape = transactionShape,
            )
          ),
          includeReassignments = None,
          includeTopologyEvents = None,
        )
      ),
    )

  def eventFormat(
      subscriptionFilter: TemplateOrInterfaceWithImpls,
      party: Party,
      includeCreatedEventBlobs: Boolean,
  ): EventFormat =
    EventFormat(
      filtersByParty = Map(
        party.getValue -> Filters(
          Seq(
            CumulativeFilter(
              subscriptionFilter match {
                case SubTemplate(template) =>
                  IdentifierFilter.TemplateFilter(
                    TemplateFilter(
                      Some(template.toScalaPbIdentifier),
                      includeCreatedEventBlob = includeCreatedEventBlobs,
                    )
                  )
                case SubInterface(interface, _) =>
                  IdentifierFilter.InterfaceFilter(
                    InterfaceFilter(
                      Some(interface),
                      includeInterfaceView = true,
                      includeCreatedEventBlob = includeCreatedEventBlobs,
                    )
                  )
              }
            )
          )
        )
      ),
      filtersForAnyParty = None,
      verbose = true,
    )

  // SubscriptionFilterWithExpectations
  sealed trait TemplateOrInterfaceWithImpls {
    def expectedTemplatesInResponses: Set[Ref.TypeConRef]
  }

  case class SubTemplate(tpl: Ref.TypeConRef) extends TemplateOrInterfaceWithImpls {
    override def expectedTemplatesInResponses: Set[Ref.TypeConRef] =
      Set(tpl)
  }

  case class SubInterface(
      iface: ScalaPbIdentifier,
      // Used to filter out other templates that can not be excluded in the trees filters
      expectedTemplatesInResponses: Set[Ref.TypeConRef] = Set.empty,
  ) extends TemplateOrInterfaceWithImpls

  private def upload(ledger: ParticipantTestContext, darPath: String): Future[Unit] =
    ledger.uploadDarFileAndVetOnConnectedSynchronizers(Dars.read(darPath))

  implicit class EnrichedTypeConRef(val typeConRef: Ref.TypeConRef) {
    def toScalaPbIdentifier: ScalaPbIdentifier =
      ScalaPbIdentifier.of(
        typeConRef.pkg.toString,
        typeConRef.qualifiedName.module.toString(),
        typeConRef.qualifiedName.name.toString(),
      )

    def matches(other: ScalaPbIdentifier, packageName: String): Boolean =
      (typeConRef.pkg match {
        case PackageRef.Name(name) => name == packageName
        case PackageRef.Id(id) => other.packageId == id
      }) && typeConRef.qualifiedName.module.toString() == other.moduleName &&
        typeConRef.qualifiedName.name.toString() == other.entityName
  }

  implicit class EnrichedIdentifier(val identifier: ScalaPbIdentifier) {
    def toTypeConRef: Ref.TypeConRef =
      TypeConRef.assertFromString(
        s"${identifier.packageId}:${identifier.moduleName}:${identifier.entityName}"
      )
  }
}
