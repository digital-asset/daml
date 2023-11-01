// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.SigningPublicKey
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.TestDomainParameters
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.DefaultTestIdentities.domainManager
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStoreX
import com.digitalasset.canton.topology.store.{
  TopologyStoreId,
  TopologyTransactionRejection,
  ValidatedTopologyTransactionX,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, HasExecutionContext, ProtocolVersionChecksAsyncWordSpec}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.ExecutionContext

class TopologyTransactionXTestFactory(loggerFactory: NamedLoggerFactory, initEc: ExecutionContext)
    extends TestingOwnerWithKeysX(domainManager, loggerFactory, initEc) {

  import SigningKeys.*

  def createNsX(ns: Namespace, key: SigningPublicKey, isRootDelegation: Boolean) =
    NamespaceDelegationX
      .create(ns, key, isRootDelegation)
      .fold(err => sys.error(s"Failed to create NamespaceDelegationX: $err"), identity)

  val ns1 = Namespace(key1.fingerprint)
  val ns2 = Namespace(key2.fingerprint)
  val ns3 = Namespace(key3.fingerprint)
  val ns4 = Namespace(key4.fingerprint)
  val ns6 = Namespace(key6.fingerprint)
  val ns7 = Namespace(key7.fingerprint)
  val ns8 = Namespace(key8.fingerprint)
  val ns9 = Namespace(key9.fingerprint)
  val domainId1 = DomainId(UniqueIdentifier(Identifier.tryCreate("domain"), ns1))
  val uid1a = UniqueIdentifier(Identifier.tryCreate("one"), ns1)
  val uid1b = UniqueIdentifier(Identifier.tryCreate("two"), ns1)
  val uid6 = UniqueIdentifier(Identifier.tryCreate("other"), ns6)
  val party1b = PartyId(uid1b)
  val party6 = PartyId(uid6)
  val participant1 = ParticipantId(uid1a)
  val participant6 = ParticipantId(uid6)
  val ns1k1_k1 = mkAdd(createNsX(ns1, key1, isRootDelegation = true), key1)
  val ns1k2_k1 = mkAdd(createNsX(ns1, key2, isRootDelegation = true), key1)
  val ns1k2_k1p = mkAdd(createNsX(ns1, key2, isRootDelegation = true), key1)
  val ns1k3_k2 = mkAdd(createNsX(ns1, key3, isRootDelegation = false), key2)
  val ns1k8_k3_fail = mkAdd(createNsX(ns1, key8, isRootDelegation = false), key3)
  val ns6k3_k6 = mkAdd(createNsX(ns6, key3, isRootDelegation = false), key6)
  val ns6k6_k6 = mkAdd(createNsX(ns6, key6, isRootDelegation = true), key6)
  val id1ak4_k2 = mkAdd(IdentifierDelegationX(uid1a, key4), key2)
  val id1ak4_k2p = mkAdd(IdentifierDelegationX(uid1a, key4), key2)
  val id1ak4_k1 = mkAdd(IdentifierDelegationX(uid1a, key4), key1)

  val id6k4_k1 = mkAdd(IdentifierDelegationX(uid6, key4), key1)

  val okm1ak5_k3 =
    mkAdd(OwnerToKeyMappingX(participant1, Some(domainId1), NonEmpty(Seq, key5)), key3)
  val okm1ak1E_k3 = mkAdd(
    OwnerToKeyMappingX(participant1, Some(domainId1), NonEmpty(Seq, EncryptionKeys.key1)),
    key3,
  )
  val okm1ak5_k2 =
    mkAdd(OwnerToKeyMappingX(participant1, Some(domainId1), NonEmpty(Seq, key5)), key2)
  val okm1bk5_k1 =
    mkAdd(OwnerToKeyMappingX(participant1, Some(domainId1), NonEmpty(Seq, key5)), key1)
  val okm1bk5_k4 =
    mkAdd(OwnerToKeyMappingX(participant1, Some(domainId1), NonEmpty(Seq, key5)), key4)

  val sequencer1 = SequencerId(UniqueIdentifier(Identifier.tryCreate("sequencer1"), ns1))
  val okmS1k7_k1 =
    mkAdd(OwnerToKeyMappingX(sequencer1, Some(domainId1), NonEmpty(Seq, key7)), key1)
  val okmS1k9_k1 =
    mkAdd(OwnerToKeyMappingX(sequencer1, Some(domainId1), NonEmpty(Seq, key9)), key1)
  val okmS1k7_k1_remove =
    mkTrans(okmS1k7_k1.transaction.reverse, NonEmpty(Set, key1), isProposal = false)

  val defaultDomainParameters = TestDomainParameters.defaultDynamic

  val p1p1B_k2 =
    mkAdd(
      PartyToParticipantX(
        party1b,
        None,
        threshold = PositiveInt.one,
        Seq(HostingParticipant(participant1, ParticipantPermissionX.Submission)),
        groupAddressing = false,
      ),
      key2,
    )
  val p1p6_k2 =
    mkAdd(
      PartyToParticipantX(
        party1b,
        None,
        threshold = PositiveInt.one,
        Seq(HostingParticipant(participant6, ParticipantPermissionX.Submission)),
        groupAddressing = false,
      ),
      key2,
      isProposal = true,
    )
  val p1p6_k6 =
    mkAdd(
      PartyToParticipantX(
        party1b,
        None,
        threshold = PositiveInt.one,
        Seq(HostingParticipant(participant6, ParticipantPermissionX.Submission)),
        groupAddressing = false,
      ),
      key6,
      isProposal = true,
    )
  val p1p6_k2k6 =
    mkAddMultiKey(
      PartyToParticipantX(
        party1b,
        None,
        threshold = PositiveInt.one,
        Seq(HostingParticipant(participant6, ParticipantPermissionX.Submission)),
        groupAddressing = false,
      ),
      NonEmpty(Set, key2, key6),
    )

  val p1p6B_k3 =
    mkAdd(
      PartyToParticipantX(
        party1b,
        Some(domainId1),
        threshold = PositiveInt.one,
        Seq(HostingParticipant(participant6, ParticipantPermissionX.Submission)),
        groupAddressing = false,
      ),
      key3,
    )

  val dmp1_k2 = mkAdd(
    DomainParametersStateX(DomainId(uid1a), defaultDomainParameters),
    key2,
  )

  val dmp1_k1 = mkAdd(
    DomainParametersStateX(
      DomainId(uid1a),
      defaultDomainParameters
        .tryUpdate(participantResponseTimeout = NonNegativeFiniteDuration.tryOfSeconds(1)),
    ),
    key1,
  )

  val dmp1_k1_bis = mkAdd(
    DomainParametersStateX(
      DomainId(uid1a),
      defaultDomainParameters
        .tryUpdate(participantResponseTimeout = NonNegativeFiniteDuration.tryOfSeconds(2)),
    ),
    key1,
  )

  val ns7k7_k7 = mkAdd(createNsX(ns7, key7, isRootDelegation = true), key7)
  val ns8k8_k8 = mkAdd(createNsX(ns8, key8, isRootDelegation = true), key8)
  val ns9k9_k9 = mkAdd(createNsX(ns9, key9, isRootDelegation = true), key9)

  val us1 = mkAddMultiKey(
    UnionspaceDefinitionX
      .create(ns7, PositiveInt.two, NonEmpty(Set, ns1, ns8, ns9))
      .fold(err => sys.error(s"Failed to create UnionspaceDefinitionX 1: $err"), identity),
    NonEmpty(Set, key1, key8, key9),
    serial = PositiveInt.one,
  )
  val us2 = mkAdd(
    UnionspaceDefinitionX
      .create(ns7, PositiveInt.one, NonEmpty(Set, ns1))
      .fold(err => sys.error(s"Failed to create UnionspaceDefinitionX 2: $err"), identity),
    key9,
    serial = PositiveInt.two,
    isProposal = true,
  )
  val us3 = mkAdd(
    UnionspaceDefinitionX
      .create(ns7, PositiveInt.one, NonEmpty(Set, ns1))
      .fold(err => sys.error(s"Failed to create UnionspaceDefinitionX 3: $err"), identity),
    key8,
    serial = PositiveInt.two,
    isProposal = true,
  )
  val unionspaceWithMultipleOwnerThreshold = List(ns1k1_k1, ns8k8_k8, ns9k9_k9, ns7k7_k7, us1)
}

class IncomingTopologyTransactionAuthorizationValidatorTestX
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with ProtocolVersionChecksAsyncWordSpec {

  "topology transaction authorization" onlyRunWithOrGreaterThan ProtocolVersion.CNTestNet when {

    object Factory extends TopologyTransactionXTestFactory(loggerFactory, parallelExecutionContext)

    def ts(seconds: Long) = CantonTimestamp.Epoch.plusSeconds(seconds)

    def mk(
        store: InMemoryTopologyStoreX[TopologyStoreId] =
          new InMemoryTopologyStoreX(DomainStore(Factory.domainId1), loggerFactory, timeouts)
    ) = {
      val validator =
        new IncomingTopologyTransactionAuthorizationValidatorX(
          Factory.cryptoApi.crypto.pureCrypto,
          store,
          Some(Factory.domainId1),
          loggerFactory,
        )
      validator
    }

    def check(
        validated: Seq[ValidatedTopologyTransactionX[TopologyChangeOpX, TopologyMappingX]],
        expectedOutcome: Seq[Option[TopologyTransactionRejection => Boolean]],
    ) = {
      validated should have length (expectedOutcome.size.toLong)
      validated.zipWithIndex.zip(expectedOutcome).foreach {
        case ((ValidatedTopologyTransactionX(_, Some(err), _), _), Some(expected)) =>
          assert(expected(err), (err, expected))
        case ((ValidatedTopologyTransactionX(transaction, rej, _), idx), expected) =>
          assertResult(expected, s"idx=$idx $transaction")(rej)
      }
      assert(true)
    }

    val unauthorized =
      Some((err: TopologyTransactionRejection) => err == TopologyTransactionRejection.NotAuthorized)

    "receiving transactions with signatures" should {
      "succeed to add if the signature is valid" in {
        val validator = mk()
        import Factory.*
        for {
          res <- validator.validateAndUpdateHeadAuthState(
            ts(0),
            List(ns1k1_k1, ns1k2_k1),
            Map.empty,
            expectFullAuthorization = true,
          )
        } yield {
          check(res._2, Seq(None, None))
        }
      }
      "fail to add if the signature is invalid" in {
        val validator = mk()
        import Factory.*
        val invalid = ns1k2_k1.copy(signatures = ns1k1_k1.signatures)
        for {
          (_, validatedTopologyTransactions) <- validator.validateAndUpdateHeadAuthState(
            ts(0),
            List(ns1k1_k1, invalid),
            Map.empty,
            expectFullAuthorization = true,
          )
        } yield {
          check(
            validatedTopologyTransactions,
            Seq(
              None,
              Some({
                case TopologyTransactionRejection.SignatureCheckFailed(_) => true
                case _ => false
              }),
            ),
          )
        }
      }
//       TODO(#12390) resuscitate
//      "reject if the transaction is for the wrong domain" in {
//        val validator = mk()
//        import Factory.*
//        val wrongDomain = DomainId(UniqueIdentifier.tryCreate("wrong", ns1.fingerprint.unwrap))
//        val pid = ParticipantId(UniqueIdentifier.tryCreate("correct", ns1.fingerprint.unwrap))
//        val wrong = mkAdd(
//          ParticipantState(
//            RequestSide.Both,
//            wrongDomain,
//            pid,
//            ParticipantPermission.Submission,
//            TrustLevel.Ordinary,
//          ),
//          Factory.SigningKeys.key1,
//        )
//        for {
//          res <- validator.validateAndUpdateHeadAuthState(ts(0), List(ns1k1_k1, wrong))
//        } yield {
//          check(
//            res._2,
//            Seq(
//              None,
//              Some({
//                case TopologyTransactionRejection.WrongDomain(_) => true
//                case _ => false
//              }),
//            ),
//          )
//        }
//      }
    }

    "observing namespace delegations" should {
      "succeed if transaction is properly authorized" in {
        val validator = mk()
        import Factory.*
        for {
          res <- validator.validateAndUpdateHeadAuthState(
            ts(0),
            List(ns1k1_k1, ns1k2_k1, ns1k3_k2),
            Map.empty,
            expectFullAuthorization = true,
          )
        } yield {
          check(res._2, Seq(None, None, None))
        }
      }
      "fail if transaction is not properly authorized" in {
        val validator = mk()
        import Factory.*
        for {
          res <- validator.validateAndUpdateHeadAuthState(
            ts(0),
            List(ns1k1_k1, ns6k3_k6, ns1k3_k2, ns1k2_k1, ns1k3_k2),
            Map.empty,
            expectFullAuthorization = true,
          )
        } yield {
          check(res._2, Seq(None, unauthorized, unauthorized, None, None))
        }
      }
      "succeed and use load existing delegations" in {
        val store =
          new InMemoryTopologyStoreX(TopologyStoreId.AuthorizedStore, loggerFactory, timeouts)
        val validator = mk(store)
        import Factory.*
        for {
          _ <- store.update(
            SequencedTime(ts(0)),
            EffectiveTime(ts(0)),
            removeMapping = Set.empty,
            removeTxs = Set.empty,
            additions = List(ns1k1_k1).map(ValidatedTopologyTransactionX(_)),
          )
          res <- validator.validateAndUpdateHeadAuthState(
            ts(1),
            List(ns1k2_k1, ns1k3_k2),
            Map.empty,
            expectFullAuthorization = true,
          )
        } yield {
          check(res._2, Seq(None, None))
        }
      }

      "fail on incremental non-authorized transactions" in {
        val validator = mk()
        import Factory.*
        for {
          res <- validator.validateAndUpdateHeadAuthState(
            ts(1),
            List(ns1k1_k1, ns1k3_k2, id1ak4_k2, ns1k2_k1, ns6k3_k6, id1ak4_k1),
            Map.empty,
            expectFullAuthorization = true,
          )

        } yield {
          check(res._2, Seq(None, unauthorized, unauthorized, None, unauthorized, None))
        }
      }

    }

    "observing identifier delegations" should {
      "succeed if transaction is properly authorized" in {
        val validator = mk()
        import Factory.*
        for {
          res <- validator.validateAndUpdateHeadAuthState(
            ts(0),
            List(ns1k1_k1, id1ak4_k1, ns1k2_k1, id1ak4_k2),
            Map.empty,
            expectFullAuthorization = true,
          )
        } yield {
          check(res._2, Seq(None, None, None, None))
        }
      }
      "fail if transaction is not properly authorized" in {
        val validator = mk()
        import Factory.*
        for {
          res <- validator.validateAndUpdateHeadAuthState(
            ts(0),
            List(id1ak4_k1, ns1k1_k1, id1ak4_k1, id6k4_k1),
            Map.empty,
            expectFullAuthorization = true,
          )
        } yield {
          check(res._2, Seq(unauthorized, None, None, unauthorized))
        }
      }
    }

    "observing normal delegations" should {

      "succeed if transaction is properly authorized" in {
        val validator = mk()
        import Factory.*
        for {
          res <- validator.validateAndUpdateHeadAuthState(
            ts(0),
            List(ns1k1_k1, ns1k2_k1, okm1ak5_k2, p1p1B_k2, id1ak4_k1, ns6k6_k6, p1p6_k2k6),
            Map.empty,
            expectFullAuthorization = true,
          )
        } yield {
          check(res._2, Seq(None, None, None, None, None, None, None))
        }
      }
      "fail if transaction is not properly authorized" in {
        val validator = mk()
        import Factory.*
        for {
          res <- validator.validateAndUpdateHeadAuthState(
            ts(0),
            List(ns1k1_k1, okm1ak5_k2, p1p1B_k2),
            Map.empty,
            expectFullAuthorization = true,
          )
        } yield {
          check(res._2, Seq(None, unauthorized, unauthorized))
        }
      }
      "succeed with loading existing identifier delegations" in {
        val store: InMemoryTopologyStoreX[TopologyStoreId.AuthorizedStore] =
          new InMemoryTopologyStoreX(TopologyStoreId.AuthorizedStore, loggerFactory, timeouts)
        val validator = mk(store)
        import Factory.*
        for {
          _ <- store.update(
            SequencedTime(ts(0)),
            EffectiveTime(ts(0)),
            removeMapping = Set.empty,
            removeTxs = Set.empty,
            additions = List(ns1k1_k1, ns6k6_k6, id1ak4_k1).map(ValidatedTopologyTransactionX(_)),
          )
          res <- validator.validateAndUpdateHeadAuthState(
            ts(1),
            List(ns1k2_k1, p1p6_k2k6, p1p1B_k2),
            Map.empty,
            expectFullAuthorization = true,
          )
        } yield {
          check(res._2, Seq(None, None, None))
        }
      }
    }

    "observing removals" should {
      "accept authorized removals" in {
        val validator = mk()
        import Factory.*
        val Rns1k2_k1 = mkTrans(ns1k2_k1.transaction.reverse)
        val Rid1ak4_k1 = mkTrans(id1ak4_k1.transaction.reverse)
        for {
          res <- validator.validateAndUpdateHeadAuthState(
            ts(0),
            List(ns1k1_k1, ns1k2_k1, id1ak4_k1, Rns1k2_k1, Rid1ak4_k1),
            Map.empty,
            expectFullAuthorization = true,
          )
        } yield {
          check(res._2, Seq(None, None, None, None, None))
        }
      }

      "reject un-authorized after removal" in {
        val validator = mk()
        import Factory.*
        val Rns1k2_k1 = mkTrans(ns1k2_k1.transaction.reverse)
        val Rid1ak4_k1 = mkTrans(id1ak4_k1.transaction.reverse)
        for {
          res <- validator.validateAndUpdateHeadAuthState(
            ts(0),
            List(ns1k1_k1, ns1k2_k1, id1ak4_k1, Rns1k2_k1, Rid1ak4_k1, okm1ak5_k2, p1p6_k2),
            Map.empty,
            expectFullAuthorization = true,
          )
        } yield {
          check(res._2, Seq(None, None, None, None, None, unauthorized, unauthorized))
        }
      }

    }

    "correctly determine cascading update for" should {
      "namespace additions" in {
        val store =
          new InMemoryTopologyStoreX(TopologyStoreId.AuthorizedStore, loggerFactory, timeouts)
        val validator = mk(store)
        import Factory.*
        for {
          _ <- store.update(
            SequencedTime(ts(0)),
            EffectiveTime(ts(0)),
            removeMapping = Set.empty,
            removeTxs = Set.empty,
            additions = List(ns6k6_k6).map(ValidatedTopologyTransactionX(_)),
          )
          res <- validator.validateAndUpdateHeadAuthState(
            ts(1),
            List(ns1k1_k1, okm1bk5_k1, p1p6_k6),
            Map.empty,
            expectFullAuthorization = true,
          )
        } yield {
          res._1.cascadingNamespaces shouldBe Set(ns1)
        }
      }

      "namespace removals" in {
        val store =
          new InMemoryTopologyStoreX(TopologyStoreId.AuthorizedStore, loggerFactory, timeouts)
        val validator = mk(store)
        import Factory.*
        val Rns1k1_k1 = mkTrans(ns1k1_k1.transaction.reverse)
        for {
          _ <- store.update(
            SequencedTime(ts(0)),
            EffectiveTime(ts(0)),
            removeMapping = Set.empty,
            removeTxs = Set.empty,
            additions = List(ns1k1_k1).map(ValidatedTopologyTransactionX(_)),
          )
          res <- validator.validateAndUpdateHeadAuthState(
            ts(1),
            List(Rns1k1_k1, okm1bk5_k1),
            Map(Rns1k1_k1.transaction.mapping.uniqueKey -> ns1k1_k1),
            expectFullAuthorization = true,
          )
        } yield {
          check(res._2, Seq(None, unauthorized))
          res._1.cascadingNamespaces shouldBe Set(ns1)
        }
      }

      "identifier additions and removals" in {
        val store =
          new InMemoryTopologyStoreX(TopologyStoreId.AuthorizedStore, loggerFactory, timeouts)
        val validator = mk(store)
        import Factory.*
        val Rid1ak4_k1 = mkTrans(id1ak4_k1.transaction.reverse)
        for {
          _ <- store.update(
            SequencedTime(ts(0)),
            EffectiveTime(ts(0)),
            removeMapping = Set.empty,
            removeTxs = Set.empty,
            additions = List(ns1k1_k1).map(ValidatedTopologyTransactionX(_)),
          )
          res <- validator.validateAndUpdateHeadAuthState(
            ts(1),
            List(id1ak4_k1),
            Map.empty,
            expectFullAuthorization = true,
          )
          res2 <- validator.validateAndUpdateHeadAuthState(
            ts(2),
            List(Rid1ak4_k1),
            Map.empty,
            expectFullAuthorization = true,
          )
        } yield {
          res._1.cascadingNamespaces shouldBe Set()
          res._1.cascadingUids shouldBe Set(uid1a)
          res2._1.cascadingUids shouldBe Set(uid1a)
        }
      }

      "cascading invalidation pre-existing identifier uids" in {
        val store =
          new InMemoryTopologyStoreX(TopologyStoreId.AuthorizedStore, loggerFactory, timeouts)
        val validator = mk(store)
        import Factory.*
        import Factory.SigningKeys.{ec as _, *}
        // scenario: we have id1ak4_k2 previously loaded. now we get a removal on k2. we need to ensure that
        // nothing can be added by k4
        val Rns1k2_k1 = mkTrans(ns1k2_k1.transaction.reverse)
        val id6ak7_k6 = mkAdd(IdentifierDelegationX(uid6, key7), key6)
        for {
          _ <- store.update(
            SequencedTime(ts(0)),
            EffectiveTime(ts(0)),
            removeMapping = Set.empty,
            removeTxs = Set.empty,
            additions =
              List(ns1k1_k1, ns1k2_k1, id1ak4_k2, ns6k6_k6).map(ValidatedTopologyTransactionX(_)),
          )
          res <- validator.validateAndUpdateHeadAuthState(
            ts(1),
            List(p1p6_k2k6, Rns1k2_k1, id6ak7_k6, p1p6_k2),
            Map(
              ns1k2_k1.transaction.mapping.uniqueKey -> ns1k2_k1
            ),
            expectFullAuthorization = true,
          )
        } yield {
          check(res._2, Seq(None, None, None, unauthorized))
          res._1.cascadingNamespaces shouldBe Set(ns1)
          res._1.filteredCascadingUids shouldBe Set(uid6)
        }
      }
    }

    "evolving unionspace definitions with threshold > 1" should {
      "succeed if proposing lower threshold and number of owners" in {
        val store =
          new InMemoryTopologyStoreX(TopologyStoreId.AuthorizedStore, loggerFactory, timeouts)
        val validator = mk(store)
        import Factory.*
        for {
          _ <- store.update(
            SequencedTime(ts(0)),
            EffectiveTime(ts(0)),
            removeMapping = Set.empty,
            removeTxs = Set.empty,
            additions = unionspaceWithMultipleOwnerThreshold.map(
              ValidatedTopologyTransactionX(_)
            ),
          )
          res <- validator.validateAndUpdateHeadAuthState(
            ts(1),
            List(us2),
            unionspaceWithMultipleOwnerThreshold
              .map(tx => tx.transaction.mapping.uniqueKey -> tx)
              .toMap,
            expectFullAuthorization = false,
          )
        } yield {
          check(res._2, Seq(None))
        }
      }

      "succeed in authorizing with quorum of owner signatures" in {
        val store =
          new InMemoryTopologyStoreX(TopologyStoreId.AuthorizedStore, loggerFactory, timeouts)
        val validator = mk(store)
        import Factory.*
        val proposeUnionspaceWithLowerThresholdAndOwnerNumber = List(us2)
        for {
          _ <- store.update(
            SequencedTime(ts(0)),
            EffectiveTime(ts(0)),
            removeMapping = Set.empty,
            removeTxs = Set.empty,
            additions = unionspaceWithMultipleOwnerThreshold.map(
              ValidatedTopologyTransactionX(_)
            ),
          )
          _ <- store.update(
            SequencedTime(ts(1)),
            EffectiveTime(ts(1)),
            removeMapping = Set.empty,
            removeTxs = Set.empty,
            additions = proposeUnionspaceWithLowerThresholdAndOwnerNumber.map(
              ValidatedTopologyTransactionX(_)
            ),
          )
          res <- validator.validateAndUpdateHeadAuthState(
            ts(2),
            // Analogously to how the TopologyStateProcessorX merges the signatures of proposals
            // with the same serial, combine the signature of the previous proposal to the current proposal.
            List(us3.addSignatures(us2.signatures.toSeq)),
            (unionspaceWithMultipleOwnerThreshold ++ proposeUnionspaceWithLowerThresholdAndOwnerNumber)
              .map(tx => tx.transaction.mapping.uniqueKey -> tx)
              .toMap,
            // Expect to be able to authorize now that we have two signatures as required by
            // unionspaceWithMultipleOwnerThreshold (us1).
            expectFullAuthorization = true,
          )
        } yield {
          check(res._2, Seq(None))
        }
      }
    }
  }

}
