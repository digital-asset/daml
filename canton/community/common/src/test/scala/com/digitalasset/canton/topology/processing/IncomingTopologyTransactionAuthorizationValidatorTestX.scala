// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import cats.Apply
import cats.instances.list.*
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
import com.digitalasset.canton.topology.store.TopologyTransactionRejection.{
  NoDelegationFoundForKeys,
  NotAuthorized,
}
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStoreX
import com.digitalasset.canton.topology.store.{
  TopologyStoreId,
  TopologyTransactionRejection,
  ValidatedTopologyTransactionX,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.{BaseTest, HasExecutionContext, ProtocolVersionChecksAsyncWordSpec}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.ExecutionContext

class TopologyTransactionXTestFactory(loggerFactory: NamedLoggerFactory, initEc: ExecutionContext)
    extends TestingOwnerWithKeysX(domainManager, loggerFactory, initEc) {

  import SigningKeys.*

  def createNsX(ns: Namespace, key: SigningPublicKey, isRootDelegation: Boolean) =
    NamespaceDelegationX.tryCreate(ns, key, isRootDelegation)

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

  val dns1 = mkAddMultiKey(
    DecentralizedNamespaceDefinitionX
      .create(ns7, PositiveInt.two, NonEmpty(Set, ns1, ns8, ns9))
      .fold(
        err => sys.error(s"Failed to create DecentralizedNamespaceDefinitionX 1: $err"),
        identity,
      ),
    NonEmpty(Set, key1, key8, key9),
    serial = PositiveInt.one,
  )
  val dns2 = mkAdd(
    DecentralizedNamespaceDefinitionX
      .create(ns7, PositiveInt.one, NonEmpty(Set, ns1))
      .fold(
        err => sys.error(s"Failed to create DecentralizedNamespaceDefinitionX 2: $err"),
        identity,
      ),
    key9,
    serial = PositiveInt.two,
    isProposal = true,
  )
  val dns3 = mkAdd(
    DecentralizedNamespaceDefinitionX
      .create(ns7, PositiveInt.one, NonEmpty(Set, ns1))
      .fold(
        err => sys.error(s"Failed to create DecentralizedNamespaceDefinitionX 3: $err"),
        identity,
      ),
    key8,
    serial = PositiveInt.two,
    isProposal = true,
  )
  val decentralizedNamespaceWithMultipleOwnerThreshold =
    List(ns1k1_k1, ns8k8_k8, ns9k9_k9, ns7k7_k7, dns1)
}

class IncomingTopologyTransactionAuthorizationValidatorTestX
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with ProtocolVersionChecksAsyncWordSpec {

  "topology transaction authorization" when {

    object Factory extends TopologyTransactionXTestFactory(loggerFactory, parallelExecutionContext)

    def ts(seconds: Long) = CantonTimestamp.Epoch.plusSeconds(seconds)

    def mk(
        store: InMemoryTopologyStoreX[TopologyStoreId] =
          new InMemoryTopologyStoreX(DomainStore(Factory.domainId1), loggerFactory, timeouts),
        validationIsFinal: Boolean = true,
    ) = {
      val validator =
        new IncomingTopologyTransactionAuthorizationValidatorX(
          Factory.cryptoApi.crypto.pureCrypto,
          store,
          Some(Factory.domainId1),
          validationIsFinal = validationIsFinal,
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
          check(
            res._2,
            Seq(
              None,
              Some(_ == NoDelegationFoundForKeys(Set(SigningKeys.key6.fingerprint))),
              Some(_ == NoDelegationFoundForKeys(Set(SigningKeys.key2.fingerprint))),
              None,
              None,
            ),
          )
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
          check(
            res._2,
            Seq(
              None,
              Some(_ == NoDelegationFoundForKeys(Set(SigningKeys.key2.fingerprint))),
              Some(_ == NoDelegationFoundForKeys(Set(SigningKeys.key2.fingerprint))),
              None,
              Some(_ == NoDelegationFoundForKeys(Set(SigningKeys.key6.fingerprint))),
              None,
            ),
          )
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
          check(
            res._2,
            Seq(
              Some(_ == NoDelegationFoundForKeys(Set(SigningKeys.key1.fingerprint))),
              None,
              None,
              Some(_ == NoDelegationFoundForKeys(Set(SigningKeys.key1.fingerprint))),
            ),
          )
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
          check(
            res._2,
            Seq(
              None,
              Some(_ == NoDelegationFoundForKeys(Set(SigningKeys.key2.fingerprint))),
              Some(_ == NoDelegationFoundForKeys(Set(SigningKeys.key2.fingerprint))),
            ),
          )
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
          check(
            res._2,
            Seq(
              None,
              None,
              None,
              None,
              None,
              Some(_ == NoDelegationFoundForKeys(Set(SigningKeys.key2.fingerprint))),
              Some(_ == NoDelegationFoundForKeys(Set(SigningKeys.key2.fingerprint))),
            ),
          )
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
          check(
            res._2,
            Seq(None, Some(_ == NoDelegationFoundForKeys(Set(SigningKeys.key1.fingerprint)))),
          )
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
          check(
            res._2,
            Seq(
              None,
              None,
              None,
              Some(_ == NoDelegationFoundForKeys(Set(SigningKeys.key2.fingerprint))),
            ),
          )
          res._1.cascadingNamespaces shouldBe Set(ns1)
          res._1.filteredCascadingUids shouldBe Set(uid6)
        }
      }
    }

    "evolving decentralized namespace definitions with threshold > 1" should {
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
            additions = decentralizedNamespaceWithMultipleOwnerThreshold.map(
              ValidatedTopologyTransactionX(_)
            ),
          )
          res <- validator.validateAndUpdateHeadAuthState(
            ts(1),
            List(dns2),
            decentralizedNamespaceWithMultipleOwnerThreshold
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
        val proposeDecentralizedNamespaceWithLowerThresholdAndOwnerNumber = List(dns2)
        for {
          _ <- store.update(
            SequencedTime(ts(0)),
            EffectiveTime(ts(0)),
            removeMapping = Set.empty,
            removeTxs = Set.empty,
            additions = decentralizedNamespaceWithMultipleOwnerThreshold.map(
              ValidatedTopologyTransactionX(_)
            ),
          )
          _ <- store.update(
            SequencedTime(ts(1)),
            EffectiveTime(ts(1)),
            removeMapping = Set.empty,
            removeTxs = Set.empty,
            additions = proposeDecentralizedNamespaceWithLowerThresholdAndOwnerNumber.map(
              ValidatedTopologyTransactionX(_)
            ),
          )
          res <- validator.validateAndUpdateHeadAuthState(
            ts(2),
            // Analogously to how the TopologyStateProcessorX merges the signatures of proposals
            // with the same serial, combine the signature of the previous proposal to the current proposal.
            List(dns3.addSignatures(dns2.signatures.toSeq)),
            (decentralizedNamespaceWithMultipleOwnerThreshold ++ proposeDecentralizedNamespaceWithLowerThresholdAndOwnerNumber)
              .map(tx => tx.transaction.mapping.uniqueKey -> tx)
              .toMap,
            // Expect to be able to authorize now that we have two signatures as required by
            // decentralizedNamespaceWithMultipleOwnerThreshold (dns1).
            expectFullAuthorization = true,
          )
        } yield {
          check(res._2, Seq(None))
        }
      }
    }

    def checkProposalFlatAfterValidation(validationIsFinal: Boolean, expectProposal: Boolean) = {
      val store =
        new InMemoryTopologyStoreX(TopologyStoreId.AuthorizedStore, loggerFactory, timeouts)
      val validator = mk(store, validationIsFinal)
      import Factory.*
      import SigningKeys.{ec as _, *}

      val dns_id = DecentralizedNamespaceDefinitionX.computeNamespace(Set(ns1, ns8))
      val dns_2_owners = mkAddMultiKey(
        DecentralizedNamespaceDefinitionX
          .create(dns_id, PositiveInt.two, NonEmpty(Set, ns1, ns8))
          .value,
        NonEmpty(Set, key1, key8),
        serial = PositiveInt.one,
      )
      val decentralizedNamespaceWithThreeOwners = List(ns1k1_k1, ns8k8_k8, dns_2_owners)

      for {
        _ <- store.update(
          SequencedTime(ts(0)),
          EffectiveTime(ts(0)),
          removeMapping = Set.empty,
          removeTxs = Set.empty,
          additions = decentralizedNamespaceWithThreeOwners.map(
            ValidatedTopologyTransactionX(_)
          ),
        )

        pkgTx = TopologyTransactionX(
          TopologyChangeOpX.Replace,
          serial = PositiveInt.one,
          VettedPackagesX(
            ParticipantId(Identifier.tryCreate("consortium-participiant"), dns_id),
            None,
            Seq.empty,
          ),
          BaseTest.testedProtocolVersion,
        )
        result_packageVetting <- validator
          .validateAndUpdateHeadAuthState(
            ts(1),
            transactionsToValidate = List(
              // Setting isProposal=true despite having enough keys.
              // This simulates processing a proposal with the signature of a node,
              // that got merged with another proposal already in the store.
              mkTrans(pkgTx, signingKeys = NonEmpty(Set, key1, key8), isProposal = true)
            ),
            transactionsInStore = Map.empty,
            expectFullAuthorization = false,
          )

      } yield {
        val validatedPkgTx = result_packageVetting._2.loneElement

        validatedPkgTx.rejectionReason shouldBe None
        withClue("package transaction is proposal")(
          validatedPkgTx.transaction.isProposal shouldBe expectProposal
        )
      }
    }

    "change the proposal status when the validation is final" in {
      checkProposalFlatAfterValidation(validationIsFinal = true, expectProposal = false)
    }

    "not change the proposal status when the validation is not final" in {
      checkProposalFlatAfterValidation(validationIsFinal = false, expectProposal = true)
    }

    "respect the threshold of decentralized namespaces" in {
      val store =
        new InMemoryTopologyStoreX(TopologyStoreId.AuthorizedStore, loggerFactory, timeouts)
      val validator = mk(store)
      import Factory.*
      import SigningKeys.{ec as _, *}

      val dns_id = DecentralizedNamespaceDefinitionX.computeNamespace(Set(ns1, ns8, ns9))
      val dns = mkAddMultiKey(
        DecentralizedNamespaceDefinitionX
          .create(dns_id, PositiveInt.tryCreate(3), NonEmpty(Set, ns1, ns8, ns9))
          .value,
        NonEmpty(Set, key1, key8, key9),
        serial = PositiveInt.one,
      )

      val decentralizedNamespaceWithThreeOwners = List(ns1k1_k1, ns8k8_k8, ns9k9_k9, dns)

      val pkgMapping = VettedPackagesX(
        ParticipantId(Identifier.tryCreate("consortium-participiant"), dns_id),
        None,
        Seq.empty,
      )
      val pkgTx = TopologyTransactionX(
        TopologyChangeOpX.Replace,
        serial = PositiveInt.one,
        pkgMapping,
        BaseTest.testedProtocolVersion,
      )

      def validateTx(
          isProposal: Boolean,
          expectFullAuthorization: Boolean,
          signingKeys: SigningPublicKey*
      ) = TraceContext.withNewTraceContext { freshTraceContext =>
        validator
          .validateAndUpdateHeadAuthState(
            ts(1),
            transactionsToValidate = List(
              mkTrans(
                pkgTx,
                isProposal = isProposal,
                signingKeys = NonEmpty.from(signingKeys.toSet).value,
              )
            ),
            transactionsInStore = Map.empty,
            expectFullAuthorization = expectFullAuthorization,
          )(freshTraceContext)
          .map(_._2.loneElement)
      }

      for {
        _ <- store.update(
          SequencedTime(ts(0)),
          EffectiveTime(ts(0)),
          removeMapping = Set.empty,
          removeTxs = Set.empty,
          additions = decentralizedNamespaceWithThreeOwners.map(
            ValidatedTopologyTransactionX(_)
          ),
        )

        combinationsThatAreNotAuthorized = Seq(
          ( /* isProposal*/ true, /* expectFullAuthorization*/ true),
          ( /* isProposal*/ false, /* expectFullAuthorization*/ true),
          // doesn't make much sense. a non-proposal by definition must be fully authorized
          ( /* isProposal*/ false, /* expectFullAuthorization*/ false),
        )

        // try with 1/3 signatures
        _ <- MonadUtil.sequentialTraverse(combinationsThatAreNotAuthorized) {
          case (isProposal, expectFullAuthorization) =>
            clueF(
              s"key1: isProposal=$isProposal, expectFullAuthorization=$expectFullAuthorization"
            )(
              validateTx(isProposal, expectFullAuthorization, key1).map(
                _.rejectionReason shouldBe Some(NotAuthorized)
              )
            )
        }

        // authorizing as proposal should succeed
        _ <- clueF(s"key1: isProposal=true, expectFullAuthorization=false")(
          validateTx(isProposal = true, expectFullAuthorization = false, key1).map(
            _.rejectionReason shouldBe None
          )
        )

        // try with 2/3 signatures
        key1_key8_notAuthorized <- MonadUtil.sequentialTraverse(combinationsThatAreNotAuthorized) {
          case (isProposal, expectFullAuthorization) =>
            clueF(
              s"key1, key8: isProposal=$isProposal, expectFullAuthorization=$expectFullAuthorization"
            )(
              validateTx(isProposal, expectFullAuthorization, key1, key8).map(
                _.rejectionReason shouldBe Some(NotAuthorized)
              )
            )
        }

        _ <- clueF(
          s"key1, key8: isProposal=true, expectFullAuthorization=false"
        )(
          validateTx(
            isProposal = true,
            expectFullAuthorization = false,
            key1,
            key8,
          ).map(
            _.rejectionReason shouldBe None
          )
        )

        // when there are enough signatures, the transaction should become fully authorized
        // regardless of the `isProposal` and `expectFullAuthorization` flags
        allCombinations = Apply[List].product(List(true, false), List(true, false))
        _ <- MonadUtil.sequentialTraverse(allCombinations) {
          case (isProposal, expectFullAuthorization) =>
            clueF(
              s"key1, key8, key9: isProposal=$isProposal, expectFullAuthorization=$expectFullAuthorization"
            )(
              validateTx(isProposal, expectFullAuthorization, key1, key8, key9).map(
                _.rejectionReason shouldBe None
              )
            )
        }

      } yield {
        succeed
      }
    }
  }

}
