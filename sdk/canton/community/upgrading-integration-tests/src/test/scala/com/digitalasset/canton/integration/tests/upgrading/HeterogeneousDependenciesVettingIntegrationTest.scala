// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrading

import com.daml.ledger.api.v2.event.CreatedEvent
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS
import com.daml.ledger.api.v2.value.Identifier.toJavaProto
import com.daml.ledger.javaapi.data
import com.daml.ledger.javaapi.data.Identifier
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{LocalParticipantReference, ParticipantReference}
import com.digitalasset.canton.damltests.{dvpassets, dvpoffer}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.util.PartiesAllocator
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.ledger.error.LedgerApiErrors.NoPreferredPackagesFound
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission
import com.digitalasset.canton.topology.transaction.VettedPackage
import com.digitalasset.canton.util.SetupPackageVetting
import com.digitalasset.canton.util.collection.MapsUtil
import com.digitalasset.canton.{LfPackageId, LfPackageName}

import scala.jdk.CollectionConverters.{CollectionHasAsScala, MapHasAsJava, MapHasAsScala}
import scala.jdk.OptionConverters.{RichOption, RichOptional}

import dvpassets.v1.java.assets.Share as ShareV1
import dvpassets.v2.java.assets.Share as ShareV2
import dvpassets.v1.java.assets.Iou as IouV1
import dvpassets.v2.java.assets.Iou as IouV2
import dvpassets.v2.java.assets.Meta as AssetsMeta
import dvpoffer.v1.java.dvpoffer.DvpOffer as DvpOfferV1
import dvpoffer.v2.java.dvpoffer.DvpOffer as DvpOfferV2
import UpgradingBaseTest.Syntax.*

class HeterogeneousDependenciesVettingIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  @volatile var registryParticipant1, registryParticipant2, sellerParticipant,
      buyerParticipant: LocalParticipantReference = _
  @volatile var registry, seller, buyer: PartyId = _
  @volatile var assetFactoryV1PkgId, assetFactoryV2PkgId: LfPackageId = _
  private val AllDars = Set(
    UpgradingBaseTest.DvpAssetFactoryV1,
    UpgradingBaseTest.DvpAssetFactoryV2,
    UpgradingBaseTest.DvpAssetsV1,
    UpgradingBaseTest.DvpOffersV1,
    UpgradingBaseTest.DvpAssetsV2,
    UpgradingBaseTest.DvpOffersV2,
  )

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P4_S1M1
      .withSetup { implicit env =>
        import env.*

        // Disambiguate participants
        registryParticipant1 = participant1
        registryParticipant2 = participant2
        sellerParticipant = participant3
        buyerParticipant = participant4

        participants.all.synchronizers.connect_local(sequencer1, alias = daName)

        // TODO(#25523): Use Java codegen once module name conflicts can be resolved
        assetFactoryV1PkgId = LfPackageId.assertFromString(
          registryParticipant1.dars
            .upload(UpgradingBaseTest.DvpAssetFactoryV1, vetAllPackages = false)
        )
        assetFactoryV2PkgId = LfPackageId.assertFromString(
          registryParticipant1.dars
            .upload(UpgradingBaseTest.DvpAssetFactoryV2, vetAllPackages = false)
        )

        // Setup the party topology state
        inside(
          PartiesAllocator(
            Set(registryParticipant1, registryParticipant2, sellerParticipant, buyerParticipant)
          )(
            newParties = Seq(
              // In a real DvP setup, there would be two registries: one for Share and one for IOU
              // For simplicity, in this test, we use a single registry for both
              "registry" -> registryParticipant1,
              "seller" -> sellerParticipant,
              "buyer" -> buyerParticipant,
            ),
            targetTopology = Map(
              // Registry is multi-hosted
              "registry" -> Map(
                daId ->
                  // TODO(#25385): Use threshold of two once PartiesAllocator supports it
                  (PositiveInt.one, Set(
                    registryParticipant1.id -> Submission,
                    registryParticipant2.id -> Submission,
                  ))
              ),
              "seller" -> Map(
                daId -> (PositiveInt.one, Set(sellerParticipant.id -> Submission))
              ),
              "buyer" -> Map(
                daId -> (PositiveInt.one, Set(buyerParticipant.id -> Submission))
              ),
            ),
          )
        ) { case Seq(p_registry, p_seller, p_buyer) =>
          registry = p_registry
          seller = p_seller
          buyer = p_buyer
        }
      }

  private def registryParticipants(
      vettedPackages: Set[VettedPackage]
  ): Map[ParticipantReference, Set[VettedPackage]] =
    Map(registryParticipant1 -> vettedPackages, registryParticipant2 -> vettedPackages)

  private lazy val AssetIssuanceAllVetted: Map[ParticipantReference, Set[VettedPackage]] = (Seq(
    sellerParticipant -> Set(
      ShareV1.PACKAGE_ID.toPackageId.withNoVettingBounds,
      ShareV2.PACKAGE_ID.toPackageId.withNoVettingBounds,
    ),
    buyerParticipant -> Set(
      IouV1.PACKAGE_ID.toPackageId.withNoVettingBounds,
      IouV2.PACKAGE_ID.toPackageId.withNoVettingBounds,
    ),
  ) ++ registryParticipants(
    Set(
      assetFactoryV1PkgId.withNoVettingBounds,
      assetFactoryV2PkgId.withNoVettingBounds,
      ShareV1.PACKAGE_ID.toPackageId.withNoVettingBounds,
      ShareV2.PACKAGE_ID.toPackageId.withNoVettingBounds,
    )
  )).toMap

  private lazy val DvpAllVetted: Map[ParticipantReference, Set[VettedPackage]] =
    MapsUtil.mergeMapsOfSets(
      AssetIssuanceAllVetted,
      Map(
        sellerParticipant -> Set(
          DvpOfferV1.PACKAGE_ID.toPackageId.withNoVettingBounds,
          DvpOfferV2.PACKAGE_ID.toPackageId.withNoVettingBounds,
        ),
        buyerParticipant -> Set(
          DvpOfferV1.PACKAGE_ID.toPackageId.withNoVettingBounds,
          DvpOfferV2.PACKAGE_ID.toPackageId.withNoVettingBounds,
        ),
      ),
    )

  "Asset issuance" when {
    /*
    The asset issuance tree:
     - Create AssetFactory (r)
     - AssetFactory_IssueAssets (r)
          |
          |-- Share (r, s)
          |
          `-- IOU (r, b)
    where:
      - if AssetFactory is V1, Share and IOU can be only V1 (package-version wise)
      - if AssetFactory is V2, Share and IOU can be V1 or V2, independently of each other based on the version inputs
     */
    "the buyer and seller vetted only Assets V1" should {
      "issue V1 Assets" in { implicit env =>
        import env.*

        SetupPackageVetting(
          darPaths = AllDars,
          targetTopology = Map(
            daId -> AssetIssuanceAllVetted
              // Buyer and seller did not yet vet V2 assets
              .updated(sellerParticipant, Set(ShareV1.PACKAGE_ID.toPackageId.withNoVettingBounds))
              .updated(buyerParticipant, Set(ShareV1.PACKAGE_ID.toPackageId.withNoVettingBounds))
          ),
        )

        issueAssetsTest(
          expectedShareTemplateId = ShareV1.TEMPLATE_ID_WITH_PACKAGE_ID,
          expectedIouTemplateId = IouV1.TEMPLATE_ID_WITH_PACKAGE_ID,
        ).discard
      }
    }

    "registry vetted only AssetFactory V1" should {
      "issue V1 Assets" in { implicit env =>
        import env.*

        SetupPackageVetting(
          darPaths = AllDars,
          targetTopology = Map(
            daId -> AssetIssuanceAllVetted.updated(
              registryParticipant1,
              Set(
                assetFactoryV1PkgId.withNoVettingBounds,
                ShareV1.PACKAGE_ID.toPackageId.withNoVettingBounds,
                ShareV2.PACKAGE_ID.toPackageId.withNoVettingBounds,
              ),
            )
          ),
        )

        issueAssetsTest(
          expectedShareTemplateId = ShareV1.TEMPLATE_ID_WITH_PACKAGE_ID,
          expectedIouTemplateId = IouV1.TEMPLATE_ID_WITH_PACKAGE_ID,
        ).discard
      }
    }

    "buyer only vetted Assets V1" should {
      "issue V1 Assets" in { implicit env =>
        import env.*

        SetupPackageVetting(
          darPaths = AllDars,
          targetTopology = Map(
            daId -> AssetIssuanceAllVetted.updated(
              buyerParticipant,
              Set(ShareV1.PACKAGE_ID.toPackageId.withNoVettingBounds),
            )
          ),
        )

        issueAssetsTest(
          expectedShareTemplateId = ShareV1.TEMPLATE_ID_WITH_PACKAGE_ID,
          expectedIouTemplateId = IouV1.TEMPLATE_ID_WITH_PACKAGE_ID,
        ).discard
      }
    }

    "buyer only vetted Assets V1 and Seller V2 with forced V2 issuance for seller" should {
      "succeed with Seller's create at V2 and buyer's at V1" in { implicit env =>
        import env.*

        SetupPackageVetting(
          darPaths = AllDars,
          targetTopology = Map(
            daId -> AssetIssuanceAllVetted
              .updated(
                buyerParticipant,
                Set(ShareV1.PACKAGE_ID.toPackageId.withNoVettingBounds),
              )
              .updated(
                sellerParticipant,
                Set(ShareV2.PACKAGE_ID.toPackageId.withNoVettingBounds),
              )
          ),
        )

        issueAssetsTest(
          expectedShareTemplateId = ShareV2.TEMPLATE_ID_WITH_PACKAGE_ID,
          expectedIouTemplateId = IouV1.TEMPLATE_ID_WITH_PACKAGE_ID,
          // Not considering the common Assets vetted package for Buyer and Seller
          // in the preferred package version query allows the registry participant
          // to issue different versions of the assets (Seller -> V2, Buyer -> V1)
          //
          // Note: This is possible since the package selection for IssueAssets is successful from the first pass
          // which only takes into consideration the registry and the asset factory package-name only.
          // If this decision were to be pushed to the second pass, the package selection would fail
          // since the buyer and seller would have disjoint Assets vetted packages
          overrideIssueAssetsV2 = true,
          expectedShareMeta = Some(new AssetsMeta(Map("tag" -> "V2").asJava)),
        ).discard
      }
    }
  }

  "DvP flow" when {
    "all vetted V2" should {
      "conclude the DvP flow in V2" in { implicit env =>
        tradeTest(vettingSetup = DvpAllVetted)(
          expectedShareTemplateId = ShareV2.TEMPLATE_ID_WITH_PACKAGE_ID,
          expectedIouTemplateId = IouV2.TEMPLATE_ID_WITH_PACKAGE_ID,
          expectedOfferTemplateId = DvpOfferV2.TEMPLATE_ID_WITH_PACKAGE_ID,
          expectedBuyersIouMeta = Some(new AssetsMeta(Map("tag" -> "V2").asJava)),
          expectedSellersShareMeta = Some(new AssetsMeta(Map("tag" -> "V2").asJava)),
          expectedBuyersShareMetaAfterTrade = Some(
            new AssetsMeta(Map("traded_status" -> "done", "tag" -> "V2").asJava)
          ),
          expectedSellersIouMetaAfterTrade = Some(
            new AssetsMeta(Map("traded_status" -> "done", "tag" -> "V2").asJava)
          ),
        )
      }
    }

    "one of the registry's participants only vetted assets V1" should {
      "succeed with the exercise choosing assets v1 so it can be accepted by the registry as well" in {
        implicit env =>
          /*
           Here we focus on the behavior of package selection in command execution in the following situation:
           - package selection only happens at the root command node, since it's specified by package-name
           - a child exercise of the root node that defines a static package bind to the package of the root node adds a new informee
             which is not considered in the package selection of the root node since it doesn't see it.
           Expectation: the package selection on the root node considers also the dependencies that it forces on new informees
           appearing in sub-nodes.

           As an example, we take the Accept choice of a DvP, which looks as follows (in parantheses are the informees of nodes)
           ---------------------------------
           Exe Accept (b, s)
             |
             |-- Fetch Share (b, s, r)
             |
             |-- Fetch IOU (b, s, r)
             |
             |-- Exe TransferShare (b, s, r)
             |   |
             |   `-- Share (b, s, r)
             |
             `-- Exe TransferIou (b, s, r)
                 |
                 `-- IOU (b, s, r)
            --------------------------------
            NOTE: The important aspect here is that the package selection on the command execution of "Accept" choice on the DvP offer
            yields a package id that does not force on the registry party an unvetted package-id (via static links) of the IOU and Share templates.
            The registry does not see the top-level action node of the transaction, and implicitly does not need to use the DvpOffer package.
           */
          tradeTest(
            vettingSetup = DvpAllVetted.updated(
              registryParticipant1,
              Set(
                // Only V1 versions vetted for the registry
                assetFactoryV1PkgId.withNoVettingBounds,
                ShareV1.PACKAGE_ID.toPackageId.withNoVettingBounds,
              ),
            )
          )(
            expectedShareTemplateId = ShareV1.TEMPLATE_ID_WITH_PACKAGE_ID,
            expectedIouTemplateId = IouV1.TEMPLATE_ID_WITH_PACKAGE_ID,
            expectedOfferTemplateId = DvpOfferV2.TEMPLATE_ID_WITH_PACKAGE_ID,
          )
      }
    }

    "one of the registry's participants did not vet anything" should {
      "fail" in { implicit env =>
        assertThrowsAndLogsCommandFailures(
          tradeTest(
            // Unvet all packages on registryParticipant2
            // Note: submission and package preferences requests are still attempted on registryParticipant1
            vettingSetup = DvpAllVetted.updated(registryParticipant2, Set.empty)
          )(
            expectedShareTemplateId = ShareV1.TEMPLATE_ID_WITH_PACKAGE_ID,
            expectedIouTemplateId = IouV1.TEMPLATE_ID_WITH_PACKAGE_ID,
            expectedOfferTemplateId = DvpOfferV2.TEMPLATE_ID_WITH_PACKAGE_ID,
          ),
          entry => {
            entry.shouldBeCantonErrorCode(NoPreferredPackagesFound)
            entry.message should include regex
              s"Failed to compute package preferences. Reason: No synchronizer satisfies the vetting requirements.*No package with package-name 'dvp-asset-factory' is consistently vetted by all hosting participants of party ${registry.show}"
          },
        )
      }
    }
  }

  private def tradeTest(
      vettingSetup: Map[ParticipantReference, Set[VettedPackage]]
  )(
      expectedShareTemplateId: data.Identifier,
      expectedIouTemplateId: data.Identifier,
      expectedOfferTemplateId: data.Identifier,
      expectedBuyersIouMeta: Option[AssetsMeta] = None,
      expectedSellersShareMeta: Option[AssetsMeta] = None,
      expectedBuyersShareMetaAfterTrade: Option[AssetsMeta] = None,
      expectedSellersIouMetaAfterTrade: Option[AssetsMeta] = None,
  )(implicit env: FixtureParam): Unit = {
    import env.*

    // Setup the vetting topology state
    SetupPackageVetting(darPaths = AllDars, targetTopology = Map(daId -> vettingSetup))

    val (shareCid, iouCid) = issueAssetsTest(
      expectedShareTemplateId,
      expectedIouTemplateId,
      expectedShareMeta = expectedSellersShareMeta,
      expectedIouMeta = expectedBuyersIouMeta,
    )

    val iouDisclosedContracts = ledger_api_utils
      .fetchContractsAsDisclosed(registryParticipant1, registry, IouV1.TEMPLATE_ID)
      .values

    val createOfferEvent = buyerParticipant.ledger_api.javaapi.commands
      .submit(
        Seq(buyer),
        new DvpOfferV1(
          buyer.toProtoPrimitive,
          seller.toProtoPrimitive,
          registry.toProtoPrimitive,
          "SO Inc.",
          new dvpoffer.v1.java.assets.Iou.ContractId(iouCid),
        ).create().commands().asScala.toList,
      )
      .getEvents
      .asScala
      .headOption
      .value

    // Assert Offer version
    Identifier.fromProto(
      createOfferEvent.toProtoEvent.getCreated.getTemplateId
    ) shouldBe expectedOfferTemplateId

    val dvpOfferPreference = sellerParticipant.ledger_api.interactive_submission
      .preferred_packages(
        Map(
          LfPackageName
            .assertFromString(DvpOfferV2.PACKAGE_NAME) -> Set(seller, buyer),
          LfPackageName.assertFromString(ShareV1.PACKAGE_NAME) -> Set(registry),
        )
      )

    val useDvpOfferMetaArgument =
      dvpOfferPreference.packageReferences
        .find(_.packageName == DvpOfferV2.PACKAGE_NAME)
        .value
        .packageId == DvpOfferV2.PACKAGE_ID

    val acceptTransaction = sellerParticipant.ledger_api.javaapi.commands
      .submit(
        Seq(seller),
        new DvpOfferV2.ContractId(createOfferEvent.getContractId)
          .exerciseAccept(
            new dvpoffer.v2.java.assets.Share.ContractId(shareCid),
            Option
              .when(useDvpOfferMetaArgument)(
                new dvpoffer.v2.java.assets.Meta(Map("traded_status" -> "done").asJava)
              )
              .toJava,
          )
          .commands()
          .asScala
          .toList,
        disclosedContracts = iouDisclosedContracts.toSeq,
        transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
      )

    val createdEvents = acceptTransaction.getEventsById.asScala.collect {
      case (_, createdEvent: data.CreatedEvent) => createdEvent
    }
    val createShareForBuyerEvent =
      createdEvents.find(_.getTemplateId.getEntityName == ShareV2.TEMPLATE_ID.getEntityName).value
    val createIouForSellerEvent =
      createdEvents.find(_.getTemplateId.getEntityName == IouV2.TEMPLATE_ID.getEntityName).value

    ShareV2
      .valueDecoder()
      .decode(createShareForBuyerEvent.getArguments)
      .meta
      .toScala shouldBe expectedBuyersShareMetaAfterTrade
    IouV2
      .valueDecoder()
      .decode(createIouForSellerEvent.getArguments)
      .meta
      .toScala shouldBe expectedSellersIouMetaAfterTrade
  }

  private def issueAssetsTest(
      expectedShareTemplateId: data.Identifier,
      expectedIouTemplateId: data.Identifier,
      overrideIssueAssetsV2: Boolean = false,
      expectedShareMeta: Option[AssetsMeta] = None,
      expectedIouMeta: Option[AssetsMeta] = None,
  )(implicit env: FixtureParam): (String /* share cId */, String /* IOU cId */ ) = {
    import env.*

    // TODO(#25523): Use Java codegen once module name conflicts can be resolved
    val dvpAssetFactoryPkgName = LfPackageName.assertFromString("dvp-asset-factory")
    val dvpAssetsPkgName = LfPackageName.assertFromString(ShareV1.PACKAGE_NAME)

    val assetFactoryV2Supported =
      registryParticipant1.ledger_api.interactive_submission
        .preferred_packages(Map(dvpAssetFactoryPkgName -> Set(registry)))
        .packageReferences
        .find(_.packageName == dvpAssetFactoryPkgName)
        .value
        .packageId == assetFactoryV2PkgId

    val createAssetFactory = ledger_api_utils.create(
      s"#$dvpAssetFactoryPkgName",
      "AssetFactory",
      "AssetFactory",
      Map("issuer" -> registry, "tag" -> (Option.when(assetFactoryV2Supported)("V2"): Any)),
    )

    val assetFactoryCreatedEvent = registryParticipant1.ledger_api.commands
      .submit(Seq(registry), Seq(createAssetFactory))
      .events
      .headOption
      .value
      .getCreated

    // Lazy value since the call fails if buyer and seller have disjoint Assets vetted packages
    lazy val issueAssetsSupportsV2Assets =
      registryParticipant1.ledger_api.interactive_submission
        .preferred_packages(
          Map(dvpAssetFactoryPkgName -> Set(registry), dvpAssetsPkgName -> Set(seller, buyer))
        )
        .packageReferences
        .find(_.packageName == dvpAssetFactoryPkgName)
        .value
        .packageId == assetFactoryV2PkgId

    val buyerSupportsV2Assets = buyerParticipant.ledger_api.interactive_submission
      .preferred_packages(Map(dvpAssetsPkgName -> Set(buyer)))
      .packageReferences
      .find(_.packageName == dvpAssetsPkgName)
      .value
      .packageId == IouV2.PACKAGE_ID

    val sellerSupportsV2Assets = sellerParticipant.ledger_api.interactive_submission
      .preferred_packages(Map(dvpAssetsPkgName -> Set(seller)))
      .packageReferences
      .find(_.packageName == dvpAssetsPkgName)
      .value
      .packageId == IouV2.PACKAGE_ID

    val issueAssetsCreates = registryParticipant1.ledger_api.commands
      .submit(
        Seq(registry),
        Seq(
          ledger_api_utils.exercise(
            "AssetFactory_IssueAssets",
            Map(
              "iouOwner" -> buyer,
              "iouAmount" -> 1337.0d,
              "iouVersion" -> (if (
                                 (overrideIssueAssetsV2 || issueAssetsSupportsV2Assets) && buyerSupportsV2Assets
                               ) 2
                               else 1),
              "shareOwner" -> seller,
              "shareCompany" -> "SO Inc.",
              "shareVersion" -> (if (
                                   (overrideIssueAssetsV2 || issueAssetsSupportsV2Assets) && sellerSupportsV2Assets
                                 ) 2
                                 else 1),
            ),
            assetFactoryCreatedEvent,
          )
        ),
      )
      .events
      .flatMap(_.event.created)

    val createdShareEvent = issueAssetsCreates
      .find(_.templateId.value.entityName == ShareV1.TEMPLATE_ID_WITH_PACKAGE_ID.getEntityName)
      .value
    val createdIouEvent = issueAssetsCreates
      .find(_.templateId.value.entityName == IouV1.TEMPLATE_ID_WITH_PACKAGE_ID.getEntityName)
      .value

    // Assert Share version
    Identifier.fromProto(
      toJavaProto(createdShareEvent.getTemplateId)
    ) shouldBe expectedShareTemplateId

    ShareV2
      .valueDecoder()
      .decode(data.CreatedEvent.fromProto(CreatedEvent.toJavaProto(createdShareEvent)).getArguments)
      .meta
      .toScala shouldBe expectedShareMeta

    // Assert IOU version
    Identifier.fromProto(
      toJavaProto(createdIouEvent.getTemplateId)
    ) shouldBe expectedIouTemplateId

    IouV2
      .valueDecoder()
      .decode(data.CreatedEvent.fromProto(CreatedEvent.toJavaProto(createdIouEvent)).getArguments)
      .meta
      .toScala shouldBe expectedIouMeta

    createdShareEvent.contractId -> createdIouEvent.contractId
  }
}
