// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.HashPurpose
import com.digitalasset.canton.data.LightTransactionViewTree.InvalidLightTransactionViewTree
import com.digitalasset.canton.data.MerkleTree.{BlindSubtree, RevealIfNeedBe, RevealSubtree}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.sequencing.protocol.{
  MemberRecipient,
  ParticipantsOfParty,
  Recipients,
  RecipientsTree,
}
import com.digitalasset.canton.topology.client.PartyTopologySnapshotClient
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{ParticipantId, PartyId}
import com.digitalasset.canton.{
  BaseTestWordSpec,
  HasExecutionContext,
  LfPartyId,
  ProtocolVersionChecksAnyWordSpec,
}
import monocle.PIso

import scala.annotation.nowarn
import scala.concurrent.Future

@nowarn("msg=match may not be exhaustive")
class GenTransactionTreeTest
    extends BaseTestWordSpec
    with HasExecutionContext
    with ProtocolVersionChecksAnyWordSpec {

  val factory: ExampleTransactionFactory = new ExampleTransactionFactory()()

  forEvery(factory.standardHappyCases) { example =>
    s"$example" can {
      val transactionTree = example.transactionTree

      "compute the correct sequence of transaction view trees" in {
        transactionTree.allTransactionViewTrees shouldEqual example.transactionViewTrees
      }

      forEvery(example.transactionViewTrees.zip(example.viewWithSubviews).zipWithIndex) {
        case ((expectedTransactionViewTree, (expectedView, _)), index) =>
          s"blind the transaction tree to the $index-th transaction view tree" in {
            transactionTree.transactionViewTree(
              expectedTransactionViewTree.viewHash.toRootHash
            ) shouldEqual expectedTransactionViewTree
          }

          s"yield the correct view for the $index-th transaction view tree" in {
            expectedTransactionViewTree.view shouldEqual expectedView
          }

          val topLevelExpected =
            example.rootTransactionViewTrees.contains(expectedTransactionViewTree)
          s"yield that the $index-th transaction view tree has isTopLevel=$topLevelExpected" in {
            expectedTransactionViewTree.isTopLevel shouldEqual topLevelExpected
          }
      }

      val fullInformeeTree = transactionTree.tryFullInformeeTree(testedProtocolVersion)

      val expectedInformeesAndThresholdByView = example.transactionViewTrees.map { viewTree =>
        val viewCommonData = viewTree.view.viewCommonData.tryUnwrap
        viewTree.viewPosition -> viewCommonData.viewConfirmationParameters
      }.toMap

      "compute the set of informees" in {
        example.fullInformeeTree.allInformees shouldEqual example.allInformees
      }

      "compute the full informee tree" in {
        fullInformeeTree should equal(example.fullInformeeTree)

        fullInformeeTree.informeesAndThresholdByViewPosition shouldEqual expectedInformeesAndThresholdByView
      }

      "be serialized and deserialized" in {
        val fullInformeeTree = example.fullInformeeTree
        FullInformeeTree.fromByteString(factory.cryptoOps, testedProtocolVersion)(
          fullInformeeTree.toByteString
        ) shouldEqual Right(fullInformeeTree)

        forAll(example.transactionTree.allLightTransactionViewTrees(testedProtocolVersion)) { lt =>
          LightTransactionViewTree.fromTrustedByteString(
            (example.cryptoOps, testedProtocolVersion)
          )(
            lt.toByteString
          ) shouldBe Right(lt)
        }
      }

      "correctly reconstruct the full transaction view trees from the lightweight ones" in {
        val allLightTrees =
          example.transactionTree.allLightTransactionViewTrees(testedProtocolVersion)
        val allTrees = example.transactionTree.allTransactionViewTrees
        LightTransactionViewTree
          .toFullViewTrees(PIso.id, testedProtocolVersion, factory.cryptoOps, topLevelOnly = false)(
            allLightTrees
          ) shouldBe (allTrees, Seq.empty, Seq.empty)
      }

      "correctly reconstruct the top-level transaction view trees from the lightweight ones" in {
        val allLightTrees =
          example.transactionTree.allLightTransactionViewTrees(testedProtocolVersion)
        val allTrees = example.transactionTree.allTransactionViewTrees.filter(_.isTopLevel)

        LightTransactionViewTree
          .toFullViewTrees(PIso.id, testedProtocolVersion, factory.cryptoOps, topLevelOnly = true)(
            allLightTrees
          ) shouldBe (allTrees, Seq.empty, Seq.empty)
      }

      "correctly reconstruct the top-level transaction view trees from the lightweight ones for each informee" in {
        val seedLength = example.cryptoOps.defaultHashAlgorithm.length
        val seed = example.cryptoOps.generateSecureRandomness(seedLength.toInt)
        val hkdfOps = ExampleTransactionFactory.hkdfOps

        val allLightTrees = example.transactionTree
          .allLightTransactionViewTreesWithWitnessesAndSeeds(
            seed,
            hkdfOps,
            testedProtocolVersion,
          )
          .valueOrFail("Cant get the light transaction trees")
        val allTrees = example.transactionTree.allTransactionViewTrees.toList
        val allInformees = allLightTrees.map(_._1.informees).fold(Set.empty)(_.union(_))

        forAll(allInformees) { inf =>
          val topLevelHashesForInf = allLightTrees
            .filter(lts =>
              lts._2.unwrap.headOption.value.contains(inf) && lts._2.unwrap
                .drop(1)
                .forall(!_.contains(inf))
            )
            .map(_._1.viewHash)
            .toSet
          val topLevelForInf = allTrees.filter(t => topLevelHashesForInf.contains(t.viewHash))
          val allLightWeightForInf =
            allLightTrees.filter(_._2.flatten.contains(inf)).map(_._1).toList
          LightTransactionViewTree
            .toFullViewTrees(
              PIso.id,
              testedProtocolVersion,
              factory.cryptoOps,
              topLevelOnly = true,
            )(
              allLightWeightForInf
            ) shouldBe (topLevelForInf, Seq.empty, Seq.empty)
        }
      }

      "correctly report missing subviews" in {
        val allLightTrees =
          example.transactionTree.allLightTransactionViewTrees(testedProtocolVersion)
        val removedLightTreeO = allLightTrees.find(_.viewPosition.position.sizeIs > 1)
        val inputLightTrees = allLightTrees.filterNot(removedLightTreeO.contains)
        val badLightTrees = inputLightTrees.filter(tree =>
          ViewPosition.isDescendant(
            removedLightTreeO.fold(ViewPosition.root)(_.viewPosition),
            tree.viewPosition,
          )
        )

        val allFullTrees = example.transactionTree.allTransactionViewTrees
        val expectedFullTrees = allFullTrees.filter(tree =>
          !ViewPosition.isDescendant(
            removedLightTreeO.fold(ViewPosition.root)(_.viewPosition),
            tree.viewPosition,
          )
        )

        LightTransactionViewTree
          .toFullViewTrees(PIso.id, testedProtocolVersion, factory.cryptoOps, topLevelOnly = false)(
            inputLightTrees
          ) shouldBe (expectedFullTrees, badLightTrees, Seq.empty)
      }

      "correctly process duplicate views" in {
        val allLightTrees =
          example.transactionTree.allLightTransactionViewTrees(testedProtocolVersion)
        val allFullTrees = example.transactionTree.allTransactionViewTrees

        val inputLightTrees1 = allLightTrees.flatMap(tree => Seq(tree, tree))
        LightTransactionViewTree
          .toFullViewTrees(PIso.id, testedProtocolVersion, factory.cryptoOps, topLevelOnly = false)(
            inputLightTrees1
          ) shouldBe (allFullTrees, Seq.empty, allLightTrees)

        val inputLightTrees2 = allLightTrees ++ allLightTrees
        LightTransactionViewTree
          .toFullViewTrees(PIso.id, testedProtocolVersion, factory.cryptoOps, topLevelOnly = false)(
            inputLightTrees2
          ) shouldBe (allFullTrees, Seq.empty, allLightTrees)
      }

      "correctly process views in an unusual order" in {
        val allLightTrees =
          example.transactionTree.allLightTransactionViewTrees(testedProtocolVersion)
        val inputLightTrees = allLightTrees.sortBy(_.viewPosition.position.size)
        val allFullTrees = example.transactionTree.allTransactionViewTrees
        LightTransactionViewTree
          .toFullViewTrees(PIso.id, testedProtocolVersion, factory.cryptoOps, topLevelOnly = false)(
            inputLightTrees
          ) shouldBe (allFullTrees, Seq.empty, Seq.empty)
      }
    }
  }

  "A transaction tree" when {

    val singleCreateView =
      factory.SingleCreate(ExampleTransactionFactory.lfHash(0)).rootViews.headOption.value

    // First check that the normal thing does not throw an exception.
    GenTransactionTree.tryCreate(factory.cryptoOps)(
      factory.submitterMetadata,
      factory.commonMetadata,
      factory.participantMetadata,
      MerkleSeq.fromSeq(factory.cryptoOps, testedProtocolVersion)(Seq(singleCreateView)),
    )

    "several root views have the same hash" must {
      "prevent creation" in {
        GenTransactionTree.create(factory.cryptoOps)(
          factory.submitterMetadata,
          factory.commonMetadata,
          factory.participantMetadata,
          MerkleSeq.fromSeq(factory.cryptoOps, testedProtocolVersion)(
            Seq(singleCreateView, singleCreateView)
          ),
        ) should matchPattern {
          case Left(message: String)
              if message.matches(
                "A transaction tree must contain a hash at most once\\. " +
                  "Found the hash .* twice\\."
              ) =>
        }
      }
    }

    "a view and a subview have the same hash" must {
      "prevent creation" in {
        val childViewCommonData =
          singleCreateView.viewCommonData.tryUnwrap.copy(salt = factory.commonDataSalt(1))
        val childView = singleCreateView.copy(viewCommonData = childViewCommonData)
        val subviews = TransactionSubviews(Seq(childView))(testedProtocolVersion, factory.cryptoOps)
        val parentView = singleCreateView.copy(subviews = subviews)

        GenTransactionTree.create(factory.cryptoOps)(
          factory.submitterMetadata,
          factory.commonMetadata,
          factory.participantMetadata,
          MerkleSeq.fromSeq(factory.cryptoOps, testedProtocolVersion)(Seq(parentView)),
        ) should matchPattern {
          case Left(message: String)
              if message.matches(
                "A transaction tree must contain a hash at most once\\. " +
                  "Found the hash .* twice\\."
              ) =>
        }
      }
    }
  }

  "A transaction view tree" when {

    val example = factory.MultipleRootsAndViewNestings

    val rootViewTree = example.rootTransactionViewTrees(1)
    val nonRootViewTree = example.transactionViewTrees(2)

    "everything is ok" must {
      "pass sanity tests" in {
        assert(rootViewTree.isTopLevel)
        assert(!nonRootViewTree.isTopLevel)
      }
    }

    "fully blinded" must {
      "reject creation" in {
        val fullyBlindedTree = example.transactionTree.blind {
          case _: GenTransactionTree => MerkleTree.RevealIfNeedBe
          case _: CommonMetadata => MerkleTree.RevealSubtree
          case _: ParticipantMetadata => MerkleTree.RevealSubtree
          case _ => MerkleTree.BlindSubtree
        }.tryUnwrap

        FullTransactionViewTree.create(fullyBlindedTree) shouldEqual Left(
          "A transaction view tree must contain an unblinded view."
        )
      }
    }

    "fully unblinded" must {
      "reject creation" in {
        FullTransactionViewTree.create(example.transactionTree).left.value should startWith(
          "A transaction view tree must not contain several unblinded views: "
        )
      }
    }

    "a subview of the represented view is blinded" must {
      "reject creation" in {
        val onlyView1Unblinded = rootViewTree.tree.blind {
          case _: GenTransactionTree => RevealIfNeedBe
          case v: TransactionView =>
            if (v == rootViewTree.view) MerkleTree.RevealIfNeedBe else MerkleTree.BlindSubtree
          case _: MerkleTreeLeaf[_] => MerkleTree.RevealSubtree
        }.tryUnwrap

        FullTransactionViewTree.create(onlyView1Unblinded).left.value should startWith(
          "A transaction view tree must contain a fully unblinded view:"
        )
      }
    }

    "the submitter metadata is blinded, although view is top level" must {
      "reject creation" in {
        val submitterMetadataBlinded = rootViewTree.tree.blind {
          case _: GenTransactionTree => RevealIfNeedBe
          case _: SubmitterMetadata => MerkleTree.BlindSubtree
          case _: TransactionView => MerkleTree.RevealSubtree
          case _: MerkleTreeLeaf[_] => MerkleTree.RevealSubtree
        }.tryUnwrap

        FullTransactionViewTree
          .create(submitterMetadataBlinded) shouldEqual Left(
          "The submitter metadata must be unblinded if and only if the represented view is top-level. " +
            "Submitter metadata: blinded, isTopLevel: true"
        )
      }
    }

    "the submitter metadata is unblinded, although view is not top level" must {
      "reject creation" in {
        val submitterMetadata = example.transactionTree.submitterMetadata

        val submitterMetadataUnblinded =
          nonRootViewTree.tree.copy(submitterMetadata = submitterMetadata)

        FullTransactionViewTree.create(submitterMetadataUnblinded) shouldEqual Left(
          "The submitter metadata must be unblinded if and only if the represented view is top-level. " +
            "Submitter metadata: unblinded, isTopLevel: false"
        )
      }
    }

    "the common metadata is blinded" must {
      "reject creation" in {
        val commonMetadataBlinded = rootViewTree.tree.blind {
          case _: GenTransactionTree => RevealIfNeedBe
          case _: CommonMetadata => MerkleTree.BlindSubtree
          case _ => MerkleTree.RevealSubtree
        }.tryUnwrap

        FullTransactionViewTree.create(commonMetadataBlinded) shouldEqual Left(
          "The common metadata of a transaction view tree must be unblinded."
        )
      }
    }

    "the participant metadata is blinded" must {
      "reject creation" in {
        val participantMetadataBlinded = rootViewTree.tree.blind {
          case _: GenTransactionTree => RevealIfNeedBe
          case _: ParticipantMetadata => MerkleTree.BlindSubtree
          case _ => MerkleTree.RevealSubtree
        }.tryUnwrap

        FullTransactionViewTree.create(participantMetadataBlinded) shouldEqual Left(
          "The participant metadata of a transaction view tree must be unblinded."
        )
      }
    }
  }

  // Before v3, the subview hashes do not need to be passed at construction
  "A light transaction view tree" when {
    val example = factory.ViewInterleavings

    forEvery(example.transactionViewTrees.zipWithIndex) { case (tvt, index) =>
      val viewWithBlindedSubviews = tvt.view.copy(subviews = tvt.view.subviews.blindFully)
      val genTransactionTree =
        tvt.tree.mapUnblindedRootViews(_.replace(tvt.viewHash, viewWithBlindedSubviews))

      val dummyViewHash = ViewHash(
        factory.cryptoOps.build(HashPurpose.MerkleTreeInnerNode).add("hummous").finish()
      )
      val mangledSubviewHashes =
        if (tvt.subviewHashes.isEmpty) Seq(dummyViewHash)
        else tvt.subviewHashes.updated(0, dummyViewHash)

      "given consistent subview hashes" must {
        s"pass sanity tests at creation (for the $index-th transaction view tree)" in {
          noException should be thrownBy LightTransactionViewTree
            .tryCreate(genTransactionTree, tvt.subviewHashes, testedProtocolVersion)
        }
      }

      "given inconsistent subview hashes" must {
        s"reject creation (for the $index-th transaction view tree)" in {
          an[InvalidLightTransactionViewTree] should be thrownBy LightTransactionViewTree
            .tryCreate(genTransactionTree, mangledSubviewHashes, testedProtocolVersion)

          if (tvt.subviewHashes.nonEmpty)
            an[InvalidLightTransactionViewTree] should be thrownBy LightTransactionViewTree
              .tryCreate(genTransactionTree, Seq.empty, testedProtocolVersion)
        }
      }
    }
  }

  "A full informee tree" when {

    val example = factory.MultipleRootsAndViewNestings

    "global metadata is incorrectly blinded" must {
      "reject creation" in {
        def corruptGlobalMetadataBlinding(informeeTree: GenTransactionTree): GenTransactionTree =
          informeeTree.copy(
            submitterMetadata = ExampleTransactionFactory.blinded(factory.submitterMetadata),
            commonMetadata = ExampleTransactionFactory.blinded(factory.commonMetadata),
            participantMetadata = factory.participantMetadata,
          )

        val corruptedGlobalMetadataMessage = Left(
          "The submitter metadata of a full informee tree must be unblinded. " +
            "The common metadata of an informee tree must be unblinded. " +
            "The participant metadata of an informee tree must be blinded."
        )

        val globalMetadataIncorrectlyBlinded1 =
          corruptGlobalMetadataBlinding(example.fullInformeeTree.tree)
        FullInformeeTree.create(
          globalMetadataIncorrectlyBlinded1,
          testedProtocolVersion,
        ) shouldEqual corruptedGlobalMetadataMessage

        val globalMetadataIncorrectlyBlinded2 =
          corruptGlobalMetadataBlinding(example.fullInformeeTree.tree)
        FullInformeeTree.create(
          globalMetadataIncorrectlyBlinded2,
          testedProtocolVersion,
        ) shouldEqual corruptedGlobalMetadataMessage
      }
    }

    "view metadata is incorrectly unblinded" must {
      "reject creation" in {
        val Seq(_, view1Unblinded) = example.transactionTree.rootViews.unblindedElements
        val informeeTree = example.fullInformeeTree.tree
        val Seq(_, view1) = informeeTree.rootViews.unblindedElements

        val view1WithParticipantDataUnblinded =
          view1.copy(viewParticipantData = view1Unblinded.viewParticipantData)
        val rootViews = MerkleSeq.fromSeq(factory.cryptoOps, testedProtocolVersion)(
          Seq(view1WithParticipantDataUnblinded)
        )

        val treeWithViewMetadataUnblinded =
          informeeTree.copy(rootViews = rootViews)

        val corruptedViewMetadataMessage = "(?s)" +
          "The view participant data in an informee tree must be blinded\\. Found .*\\."

        FullInformeeTree
          .create(treeWithViewMetadataUnblinded, testedProtocolVersion)
          .left
          .value should fullyMatch regex corruptedViewMetadataMessage

        FullInformeeTree
          .create(treeWithViewMetadataUnblinded, testedProtocolVersion)
          .left
          .value should fullyMatch regex corruptedViewMetadataMessage
      }
    }

    "a view is blinded" should {
      "reject creation" in {
        // Keep metadata of view0 and view1 unblinded, blind every other view
        val hashesOfUnblindedViews = Set(example.view0.viewHash, example.view1.viewHash)

        val partiallyBlindedTree =
          example.fullInformeeTree.tree.blind {
            {
              case _: GenTransactionTree => RevealIfNeedBe
              case _: CommonMetadata => RevealSubtree
              case _: SubmitterMetadata => RevealSubtree

              case v: TransactionView =>
                if (hashesOfUnblindedViews.contains(v.viewHash))
                  RevealIfNeedBe // Necessary to reveal view0 and view1
                else BlindSubtree // This will blind every other view
              case _: ViewCommonData => RevealSubtree // Necessary to reveal view0 and view1
            }
          }.tryUnwrap

        FullInformeeTree
          .create(partiallyBlindedTree, testedProtocolVersion)
          .left
          .value should fullyMatch regex "(?s)All views in a full informee tree must be unblinded\\. Found .*\\."
      }
    }

    "a view common data is blinded" should {
      "reject creation" in {
        val fullInformeeTree = example.fullInformeeTree.tree
        val rootViews = fullInformeeTree.rootViews.unblindedElements

        val rootViewsWithCommonDataBlinded =
          rootViews.map(view =>
            view.copy(viewCommonData = ExampleTransactionFactory.blinded(view.viewCommonData))
          )

        val viewCommonDataBlinded =
          fullInformeeTree.copy(rootViews =
            MerkleSeq.fromSeq(factory.cryptoOps, testedProtocolVersion)(
              rootViewsWithCommonDataBlinded
            )
          )

        FullInformeeTree
          .create(viewCommonDataBlinded, testedProtocolVersion)
          .left
          .value should fullyMatch regex "(?s)The view common data in a full informee tree must be unblinded\\. Found .*\\.\n" +
          "The view common data in a full informee tree must be unblinded\\. Found .*\\."
      }
    }
  }

  "Witnesses" must {
    import GenTransactionTreeTest.*

    "correctly compute recipients from witnesses" in {
      def mkWitnesses(setup: NonEmpty[Seq[Set[Int]]]): Witnesses =
        Witnesses(setup.map(_.map(informee)))

      // Maps parties to participants; parties have IDs that start at 1, participants have IDs that start at 11
      val topologyMap = Map(
        1 -> Set(11),
        2 -> Set(12),
        3 -> Set(13),
        4 -> Set(14),
        5 -> Set(11, 12, 13, 15),
        6 -> Set(16),
      ).map { case (partyId, participantIds) =>
        party(partyId) -> participantIds
          .map(id => participant(id) -> ParticipantPermission.Submission)
          .toMap
      }

      val topology = mock[PartyTopologySnapshotClient]
      when(topology.activeParticipantsOfParties(any[List[LfPartyId]])(anyTraceContext))
        .thenAnswer[Seq[LfPartyId]] { parties =>
          Future.successful(topologyMap.collect {
            case (party, map) if parties.contains(party) => (party, map.keySet)
          })
        }
      when(topology.partiesWithGroupAddressing(any[Seq[LfPartyId]])(anyTraceContext))
        // parties 3 and 6 will use group addressing
        .thenReturn(Future.successful(Set(party(3), party(6))))

      val witnesses = mkWitnesses(
        NonEmpty(Seq, Set(1, 2), Set(1, 3), Set(2, 4), Set(1, 2, 5), Set(6))
      )

      witnesses
        .toRecipients(topology)
        .valueOr(err => fail(err.message))
        .futureValue shouldBe Recipients(
        NonEmpty(
          Seq,
          RecipientsTree.ofRecipients(
            NonEmpty.mk(Set, ParticipantsOfParty(PartyId.tryFromLfParty(party(6)))),
            Seq(
              RecipientsTree.ofMembers(
                NonEmpty(Set, 11, 12, 13, 15).map(participant),
                Seq(
                  RecipientsTree.ofMembers(
                    NonEmpty.mk(Set, participant(12), participant(14)),
                    Seq(
                      RecipientsTree.ofRecipients(
                        NonEmpty.mk(
                          Set,
                          MemberRecipient(participant(11)),
                          ParticipantsOfParty(PartyId.tryFromLfParty(party(3))),
                        ),
                        Seq(
                          RecipientsTree.leaf(NonEmpty.mk(Set, participant(11), participant(12)))
                        ),
                      )
                    ),
                  )
                ),
              )
            ),
          ),
        )
      )
    }
  }
}

object GenTransactionTreeTest {
  private[data] def party(i: Int): LfPartyId = LfPartyId.assertFromString(s"party$i::1")

  private[data] def informee(i: Int): LfPartyId = party(i)

  private[data] def participant(i: Int): ParticipantId = ParticipantId(s"participant$i")
}
