// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrading

import com.daml.ledger.api.v2.event.CreatedEvent
import com.daml.ledger.api.v2.transaction_filter.CumulativeFilter.IdentifierFilter
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_ACS_DELTA
import com.daml.ledger.api.v2.transaction_filter.{
  CumulativeFilter,
  EventFormat,
  Filters,
  InterfaceFilter,
  TransactionFormat,
  UpdateFormat,
}
import com.daml.ledger.api.v2.value.Value.toJavaProto
import com.daml.ledger.api.v2.value.{Identifier as ScalaPbIdentifier, Record}
import com.daml.ledger.javaapi.data
import com.daml.ledger.javaapi.data.{Identifier, Template}
import com.digitalasset.base.error.ErrorCode
import com.digitalasset.base.error.utils.DecodedCantonError
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService.UpdateWrapper
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.damltests.holding.v1.java.holdingv1.{
  Holding as HoldingV1,
  HoldingView as HoldingViewV1,
}
import com.digitalasset.canton.damltests.holding.v2.java.holdingv2.{
  Holding as HoldingV2,
  HoldingView as HoldingViewV2,
}
import com.digitalasset.canton.damltests.token.v1.java.token.Token as TokenV1
import com.digitalasset.canton.damltests.token.v2.java.token.Token as TokenV2
import com.digitalasset.canton.damltests.token.v3.java.token.Token as TokenV3
import com.digitalasset.canton.damltests.token.v4.java.token.Token as TokenV4
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.util.PartiesAllocator
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.ledger.error.LedgerApiErrors.NoVettedInterfaceImplementationPackage
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors.Interpreter.FailureStatus
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors.Preprocessing.PreprocessingFailed
import com.digitalasset.canton.networking.grpc.RecordingStreamObserver
import com.digitalasset.canton.participant.sync.SyncServiceInjectionError.NotConnectedToAnySynchronizer
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.{ParticipantPermission, VettedPackage}
import com.digitalasset.canton.util.SetupPackageVetting
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.daml.lf.data.Ref
import monocle.Monocle.toAppliedFocusOps
import org.scalatest.Assertion

import java.time.{Duration, Instant}
import java.util.Optional
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Future
import scala.jdk.CollectionConverters.CollectionHasAsScala

class AcquiredInterfacesIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .withSetup(setup)
      .addConfigTransform(
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.ledgerApi.topologyAwarePackageSelection.enabled).replace(true)
        )
      )

  protected def setup(env: TestConsoleEnvironment): Unit = {
    import env.*
    participant1.synchronizers.connect_local(sequencer1, alias = daName)
  }

  private def tokenV1(assetId: String)(implicit party: PartyId) =
    new TokenV1(party.toProtoPrimitive, assetId)
  private def tokenV2(assetId: String)(implicit party: PartyId) =
    new TokenV2(party.toProtoPrimitive, assetId)
  private def tokenV3(assetId: String)(implicit party: PartyId) =
    new TokenV3(party.toProtoPrimitive, assetId, Optional.of(Instant.ofEpochSecond(1337L)))
  private def tokenV4(assetId: String)(implicit party: PartyId) =
    new TokenV4(party.toProtoPrimitive, assetId, Optional.of(Instant.ofEpochSecond(1338L)))

  private implicit val viewDecoders: Map[data.Identifier, data.Value => Any] = Map(
    HoldingV1.TEMPLATE_ID_WITH_PACKAGE_ID -> { (v: data.Value) =>
      HoldingViewV1.valueDecoder.decode(v)
    },
    HoldingV2.TEMPLATE_ID_WITH_PACKAGE_ID -> { (v: data.Value) =>
      HoldingViewV2.valueDecoder.decode(v)
    },
  )

  private val partyHintSuffixRef = new AtomicInteger(0)

  "The Ledger API" when {
    "an interface subscription comes before an acquired interface instance is vetted" should {
      "return failed interface views" in withNewParty { implicit env => implicit party =>
        import env.*

        setupVettedPackages(HoldingV1.PACKAGE_ID, TokenV1.PACKAGE_ID)

        // Ensure Token V2 which defines an instance for the Holding is uploaded (but not vetted)
        participant1.dars.upload(
          UpgradingBaseTest.TokenV2,
          vetAllPackages = false,
          synchronizeVetting = false,
        )

        create(tokenV1("asset"))

        val createdEvent =
          interfaceSubscribe(Seq(HoldingV1.TEMPLATE_ID)).loneElement.createEvents.toSeq.loneElement

        val expectedFailure =
          (
            NoVettedInterfaceImplementationPackage,
            (_: String) should include regex
              s"""No vetted package for rendering the interface view for package-name '${TokenV1.PACKAGE_NAME}'.*Reason: No synchronizer satisfies the vetting requirements.*No vetted package candidate satisfies the package-id filter 'Package-ids with interface instances for the requested interface'=${TokenV1.PACKAGE_NAME} -> ${TokenV2.PACKAGE_ID.toPackageId.show}.*
                 |Candidates: ${TokenV1.PACKAGE_ID.toPackageId.show}.*""".stripMargin,
          )
        assertViews(createdEvent)(
          HoldingV1.TEMPLATE_ID_WITH_PACKAGE_ID -> Left(expectedFailure)
        )
      }
    }

    "only the upgraded template that adds the interface instance is vetted but the create is for the original template" should {
      "render the interface view for the create" in withNewParty { implicit env => implicit party =>
        setupVettedPackages(HoldingV1.PACKAGE_ID, TokenV1.PACKAGE_ID)

        create(tokenV1("asset"))

        setupVettedPackages(HoldingV1.PACKAGE_ID, TokenV2.PACKAGE_ID)

        val createEvent =
          interfaceSubscribe(Seq(HoldingV1.TEMPLATE_ID)).loneElement.createEvents.toSeq.loneElement

        assertViews(createEvent)(
          HoldingV1.TEMPLATE_ID_WITH_PACKAGE_ID -> Right(
            new HoldingViewV1(party.toProtoPrimitive, "asset")
          )
        )
      }
    }

    "a subscription for two interfaces is created" should {
      "allow switch-over from old interface version to new one by memoizing the selected package-id per interface" in withNewParty {
        implicit env => implicit party =>
          import env.*

          // Note: This test must be run before other tests that require TokenV3 to ensure it's the first one that uploads it
          //
          // Scenario:
          //   * HoldingV1, HoldingV2, TokenV2 uploaded and vetted
          //   * Subscription for HoldingV1, HoldingV2
          //   * Create TokenV2
          //   * Upload TokenV3
          //   * Vet TokenV3
          //   * Create TokenV3
          // Expectation: Subscription should see a valid rendered view for each of the creates

          setupVettedPackages(HoldingV1.PACKAGE_ID, HoldingV2.PACKAGE_ID, TokenV2.PACKAGE_ID)

          val subscriptionF = Future(
            interfaceSubscribe(
              Seq(HoldingV1.TEMPLATE_ID, HoldingV2.TEMPLATE_ID),
              completeAfter = 2,
            )
          )

          create(tokenV2("asset1"))

          setupVettedPackages(HoldingV1.PACKAGE_ID, HoldingV2.PACKAGE_ID, TokenV3.PACKAGE_ID)

          val asset2 = tokenV3("asset2")
          create(asset2)

          val subscriptionResult = subscriptionF.futureValue.map(_.createEvents.toSeq.loneElement)

          inside(subscriptionResult) { case Seq(create1, create2) =>
            assertViews(create1)(
              HoldingV1.TEMPLATE_ID_WITH_PACKAGE_ID -> Right(
                new HoldingViewV1(party.toProtoPrimitive, "asset1")
              )
            )

            assertViews(create2)(
              HoldingV1.TEMPLATE_ID_WITH_PACKAGE_ID -> Left(
                (
                  PreprocessingFailed,
                  // TODO(#25385): Consider refining the error
                  _ should include(
                    "An optional contract field with a value of Some may not be dropped during downgrading"
                  ),
                )
              ),
              HoldingV2.TEMPLATE_ID_WITH_PACKAGE_ID -> Right(
                new HoldingViewV2(party.toProtoPrimitive, asset2.assetId, asset2.expiry)
              ),
            )
          }
      }
    }

    s"vetting end bound is specified" should {
      "consider it in the view package resolution" in withNewParty {
        implicit env => implicit party =>
          import env.*

          SetupPackageVetting(
            Set(UpgradingBaseTest.HoldingV1, UpgradingBaseTest.TokenV2, UpgradingBaseTest.TokenV3),
            targetTopology = Map(
              synchronizer1Id -> Map(
                participant1 -> Set(
                  HoldingV1.PACKAGE_ID.toPackageId.withNoVettingBounds,
                  TokenV2.PACKAGE_ID.toPackageId.withNoVettingBounds,
                  TokenV3.PACKAGE_ID.toPackageId.withVettingEndsAt(
                    CantonTimestamp.now().add(Duration.ofDays(1L))
                  ),
                )
              )
            ),
          )

          create(tokenV2("asset1"))
          val create1 = interfaceSubscribe(
            Seq(HoldingV1.TEMPLATE_ID)
          ).loneElement.createEvents.toSeq.loneElement

          // Verify that Token V2 is used for rendering (V3 is discarded since it has the validUntil not at max)
          assertViews(create1)(
            HoldingV1.TEMPLATE_ID_WITH_PACKAGE_ID -> Right(
              new HoldingViewV1(party.toProtoPrimitive, "asset1")
            )
          )

          // Set the validity open-ended
          SetupPackageVetting(
            Set(UpgradingBaseTest.HoldingV1, UpgradingBaseTest.TokenV2, UpgradingBaseTest.TokenV3),
            targetTopology = Map(
              synchronizer1Id -> Map(
                participant1 -> Set(
                  HoldingV1.PACKAGE_ID.toPackageId.withNoVettingBounds,
                  TokenV2.PACKAGE_ID.toPackageId.withNoVettingBounds,
                  TokenV3.PACKAGE_ID.toPackageId.withNoVettingBounds,
                )
              )
            ),
          )

          // Verify that Token V3 is used for rendering when its validity is open-ended
          assertViews(
            interfaceSubscribe(
              Seq(HoldingV1.TEMPLATE_ID)
            ).loneElement.createEvents.toSeq.loneElement
          )(
            HoldingV1.TEMPLATE_ID_WITH_PACKAGE_ID ->
              Left((FailureStatus, _ should include("Use HoldingV2")))
          )
      }
    }

    "there is no vetted package for the package-name of the interface instance of the view being rendered" should {
      "fail the stream" in withNewParty { implicit env => implicit party =>
        setupVettedPackages(HoldingV1.PACKAGE_ID, TokenV2.PACKAGE_ID)

        create(tokenV2("asset"))

        // Now unvet the sole Token package
        setupVettedPackages(HoldingV1.PACKAGE_ID)

        val createdEvent =
          interfaceSubscribe(Seq(HoldingV1.TEMPLATE_ID)).loneElement.createEvents.toSeq.loneElement

        val expectedFailure =
          (
            NoVettedInterfaceImplementationPackage,
            (_: String) should include regex
              s"No vetted package for rendering the interface view for package-name '${TokenV1.PACKAGE_NAME}'. Reason: No synchronizer satisfies the vetting requirements. Discarded synchronizers:.*${env.daId},No package with package-name '${TokenV1.PACKAGE_NAME}' is consistently vetted by all hosting participants of party ${env.participant1.id.adminParty.show}",
          )
        assertViews(createdEvent)(
          HoldingV1.TEMPLATE_ID_WITH_PACKAGE_ID -> Left(expectedFailure)
        )

      }
    }

    "the interface subscription is started before the implementation roll-out" should {
      "observe the event and render its view" in withNewParty { implicit env => implicit party =>
        import env.*

        setupVettedPackages(HoldingV1.PACKAGE_ID)
        val subscriptionF = Future(interfaceSubscribe(Seq(HoldingV1.TEMPLATE_ID)))
        setupVettedPackages(HoldingV1.PACKAGE_ID, TokenV1.PACKAGE_ID, TokenV2.PACKAGE_ID)

        // Creates a Token V1 payload
        create(
          tokenV1("asset"),
          // Ensure V1 package-id is used to demonstrate that it is then upgraded and
          // appears in the interface subscription
          userPackageSelectionPreference = Seq(TokenV1.PACKAGE_ID),
        )

        val createdEvent = subscriptionF.futureValue.loneElement.createEvents.toSeq.loneElement
        val expectedViewValue = new HoldingViewV1(party.toProtoPrimitive, "asset")
        assertViews(createdEvent)(
          HoldingV1.TEMPLATE_ID_WITH_PACKAGE_ID -> Right(expectedViewValue)
        )
      }
    }

    "not connected to any synchronizer" should {
      "deliver view failures with a descriptive correct error" in withNewParty {
        implicit env => implicit party =>
          setupVettedPackages(HoldingV1.PACKAGE_ID, TokenV2.PACKAGE_ID)

          create(tokenV2("asset"))

          // Subscribe for the interface without a synchronizer connected
          val createdEvent = withParticipantDisconnected {
            interfaceSubscribe(
              Seq(HoldingV1.TEMPLATE_ID)
            ).loneElement.createEvents.toSeq.loneElement
          }

          assertViews(createdEvent)(
            HoldingV1.TEMPLATE_ID_WITH_PACKAGE_ID -> Left(
              (
                NotConnectedToAnySynchronizer,
                _ should include(
                  "Could not compute a package-id for rendering the interface view. Root cause: This participant is not connected to any synchronizer."
                ),
              )
            )
          )
      }
    }

    "a DAR is uploaded but not vetted" should {
      "not be used for rendering the interface view" in withNewParty {
        implicit env => implicit party =>
          import env.*

          setupVettedPackages(HoldingV1.PACKAGE_ID, TokenV2.PACKAGE_ID)

          create(tokenV2("asset"))

          participant1.dars.upload(
            UpgradingBaseTest.TokenV3,
            synchronizeVetting = false,
          )

          val createdEvent =
            interfaceSubscribe(
              Seq(HoldingV1.TEMPLATE_ID)
            ).loneElement.createEvents.toSeq.loneElement

          val expectedViewValue = new HoldingViewV1(party.toProtoPrimitive, "asset")
          assertViews(createdEvent)(
            HoldingV1.TEMPLATE_ID_WITH_PACKAGE_ID -> Right(expectedViewValue)
          )
      }
    }

    "an interface view is rendered for an already seen template package-id" should {
      "memoize the used package-id and re-use it" in withNewParty {
        implicit env => implicit party =>
          import env.*

          // vet only Token V2
          setupVettedPackages(HoldingV1.PACKAGE_ID, TokenV2.PACKAGE_ID)
          create(tokenV2("asset1"))

          val streamObserver = new RecordingStreamObserver[UpdateWrapper](completeAfter = 2)
          val subscriptionF =
            Future(
              participant1.mkResult(
                call = participant1.ledger_api.updates
                  .subscribe_updates(streamObserver, updateFormat(Seq(HoldingV1.TEMPLATE_ID))),
                requestDescription = "getUpdates",
                observer = streamObserver,
                timeout = participant1.consoleEnvironment.commandTimeouts.ledgerCommand,
              )
            )

          // Wait to be sure the first event is seen and its interface views computation
          // memoized the view package-id
          eventually()(streamObserver.responseCount.get() shouldBe 1)

          // vet Token V3 as well
          setupVettedPackages(HoldingV1.PACKAGE_ID, TokenV2.PACKAGE_ID, TokenV3.PACKAGE_ID)

          // Create tokenV2 again with explicit package selection to ensure not triggering the view 3 computation failure
          create(tokenV2("asset2"), userPackageSelectionPreference = Seq(TokenV2.PACKAGE_ID))

          val subscriptionResult = subscriptionF.futureValue
          inside(subscriptionResult.flatMap(_.createEvents)) { case Seq(create1, create2) =>
            val expectedView1Value = new HoldingViewV1(party.toProtoPrimitive, "asset1")
            assertViews(create1)(
              HoldingV1.TEMPLATE_ID_WITH_PACKAGE_ID -> Right(expectedView1Value)
            )

            val expectedView2Value = new HoldingViewV1(party.toProtoPrimitive, "asset2")
            assertViews(create2)(
              HoldingV1.TEMPLATE_ID_WITH_PACKAGE_ID -> Right(expectedView2Value)
            )
          }
      }
    }

    "the create argument cannot be downgraded to the selected interface view package-id" should {
      "return a failed interface view" in withNewParty { implicit env => implicit party =>
        import env.*

        // Vet only Token V4 and Holding V1
        setupVettedPackages(HoldingV1.PACKAGE_ID, TokenV4.PACKAGE_ID)

        create(tokenV4("asset1"))

        // Unvet Token V4 and vet Token V2 which doesn't have the expiry time argument
        setupVettedPackages(HoldingV1.PACKAGE_ID, TokenV2.PACKAGE_ID)

        val streamObserver = new RecordingStreamObserver[UpdateWrapper](completeAfter = 2)
        val subscriptionF =
          Future(
            participant1.mkResult(
              call = participant1.ledger_api.updates
                .subscribe_updates(streamObserver, updateFormat(Seq(HoldingV1.TEMPLATE_ID))),
              requestDescription = "getUpdates",
              observer = streamObserver,
              timeout = participant1.consoleEnvironment.commandTimeouts.ledgerCommand,
            )
          )

        // Wait for the first create to be rendered and vet V4 again
        eventually()(streamObserver.responseCount.get() shouldBe 1)
        setupVettedPackages(HoldingV1.PACKAGE_ID, TokenV4.PACKAGE_ID)

        // Create a new Token V4
        create(tokenV4("asset2"))

        val failuresExpectation = HoldingV1.TEMPLATE_ID_WITH_PACKAGE_ID -> Left(
          (
            // TODO(#26411): Assert a proper interface view computation error when the issue is addressed
            CommandExecutionErrors.Preprocessing.PreprocessingFailed,
            (_: String) should include(
              "An optional contract field with a value of Some may not be dropped during downgrading"
            ),
          )
        )

        inside(subscriptionF.futureValue.flatMap(_.createEvents)) { case Seq(create1, create2) =>
          // The first view cannot be computed because the contract argument cannot be downgraded to the view's selected package-id
          assertViews(create1)(failuresExpectation)

          // Even though a compatible package-id is vetted when the second create should be rendered,
          // the first resolution is memoized for the Token package name so the second view computation fails as well
          assertViews(create2)(failuresExpectation)
        }
      }
    }
  }

  def assertViews(createdEvent: CreatedEvent)(
      viewsExpectations: (
          data.Identifier /* interface-id */,
          Either[
            (ErrorCode, String => Assertion),
            Any, /* the expected decoded view value */
          ],
      )*
  )(implicit viewDecoders: Map[data.Identifier, data.Value => Any]): Unit = {
    val interfaceViews = createdEvent.interfaceViews
    interfaceViews should have size viewsExpectations.size.toLong

    val viewsById = createdEvent.interfaceViews.view
      .map(ifaceView => ifaceView.getInterfaceId -> ifaceView)
      .toMap

    viewsExpectations.foreach {
      case (interfaceId, Right(expectedValue)) =>
        val interfaceView = viewsById
          .get(interfaceId.toScalaProto)
          .valueOrFail(s"Not found $interfaceId in $viewsById")
        val actualDecodedViewValue =
          viewDecoders(interfaceId)(viewValueToJavaApi(interfaceView.getViewValue))

        actualDecodedViewValue shouldBe expectedValue
      case (interfaceId, Left((expectedCode, causeAssert))) =>
        val interfaceView = viewsById
          .get(interfaceId.toScalaProto)
          .valueOrFail(s"Not found $interfaceId in $viewsById")
        val decodedErrorFromFailedViewStatus = DecodedCantonError
          .fromGrpcStatus(interfaceView.getViewStatus)
          .value

        decodedErrorFromFailedViewStatus.code.id shouldBe expectedCode.id
        causeAssert(decodedErrorFromFailedViewStatus.cause)
    }
  }

  def withParticipantDisconnected[T](f: => T)(implicit env: FixtureParam): T = {
    env.participant1.synchronizers.disconnect_all()
    try f
    finally env.participant1.synchronizers.reconnect(env.daName)
  }

  private def withNewParty[T](f: FixtureParam => PartyId => T): FixtureParam => T =
    env => {
      import env.*

      val partyHint = s"alice-${partyHintSuffixRef.getAndIncrement()}"
      val party = PartiesAllocator(
        participants = Set(participant1)
      )(
        newParties = Seq(partyHint -> participant1.id),
        targetTopology = Map(
          partyHint -> Map(
            synchronizer1Id -> (PositiveInt.one, Set(
              participant1.id -> ParticipantPermission.Submission
            ))
          )
        ),
      ).headOption.valueOrFail("Expected Alice to be allocated")
      f(env)(party)
    }

  private def create(
      templateInstance: Template,
      userPackageSelectionPreference: Seq[LfPackageId] = Seq.empty,
  )(implicit env: FixtureParam, party: PartyId): Unit =
    env.participant1.ledger_api.javaapi.commands
      .submit(
        Seq(party),
        templateInstance.create().commands().asScala.toList,
        userPackageSelectionPreference = userPackageSelectionPreference,
      )
      .discard

  private def setupVettedPackages(
      vettedPackageIds: String*
  )(implicit env: FixtureParam): Unit = {
    import env.*
    SetupPackageVetting(
      Set(
        UpgradingBaseTest.HoldingV1,
        UpgradingBaseTest.HoldingV2,
        UpgradingBaseTest.TokenV1,
        UpgradingBaseTest.TokenV2,
        UpgradingBaseTest.TokenV3,
        UpgradingBaseTest.TokenV4,
      ),
      targetTopology = Map(
        synchronizer1Id -> Map(
          participant1 -> vettedPackageIds.map(_.toPackageId withNoVettingBounds).toSet
        )
      ),
    )
  }

  private def viewValueToJavaApi(viewValue: Record) =
    data.Value.fromProto(
      toJavaProto(
        com.daml.ledger.api.v2.value.Value(
          com.daml.ledger.api.v2.value.Value.Sum
            .Record(viewValue)
        )
      )
    )

  private def interfaceSubscribe(
      interfaceIds: Seq[data.Identifier],
      includeInterfaceView: Boolean = true,
      completeAfter: Int = 1,
  )(implicit env: FixtureParam, party: PartyId): Seq[UpdateService.UpdateWrapper] = {
    import env.*

    participant1.ledger_api.updates.transactions_with_tx_format(
      transactionFormat =
        updateFormat(interfaceIds, includeInterfaceView).includeTransactions.value,
      completeAfter = completeAfter,
    )
  }

  private def updateFormat(interfaceIds: Seq[Identifier], includeInterfaceView: Boolean = true)(
      implicit party: PartyId
  ) =
    UpdateFormat(
      includeTransactions = Some(
        TransactionFormat(
          eventFormat = Some(
            EventFormat(
              filtersByParty = Map(
                party.toProtoPrimitive -> Filters(
                  interfaceIds.map(interfaceId =>
                    CumulativeFilter(
                      IdentifierFilter.InterfaceFilter(
                        InterfaceFilter(
                          interfaceId = Some(interfaceId.toScalaProto),
                          includeCreatedEventBlob = false,
                          includeInterfaceView = includeInterfaceView,
                        )
                      )
                    )
                  )
                )
              ),
              filtersForAnyParty = None,
              verbose = false,
            )
          ),
          transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
        )
      ),
      includeReassignments = None,
      includeTopologyEvents = None,
    )
  private implicit class JavaIdentifierOps(val identifier: data.Identifier) {
    def toScalaProto: ScalaPbIdentifier = ScalaPbIdentifier.fromJavaProto(identifier.toProto)
  }

  private implicit class StrToPkgId(packageIdStr: String) {
    def toPackageId: LfPackageId = Ref.PackageId.assertFromString(packageIdStr)
  }

  private implicit class PackageIdVettingExtensions(packageId: LfPackageId) {
    def withNoVettingBounds: VettedPackage = VettedPackage(
      packageId = packageId,
      validFromInclusive = None,
      validUntilExclusive = None,
    )

    def withVettingEndsAt(validUntil: CantonTimestamp): VettedPackage = VettedPackage(
      packageId = packageId,
      validFromInclusive = None,
      validUntilExclusive = Some(validUntil),
    )
  }
}
