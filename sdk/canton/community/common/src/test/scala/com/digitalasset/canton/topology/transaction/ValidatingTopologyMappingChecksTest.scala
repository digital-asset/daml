// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.{Fingerprint, SigningPublicKey}
import com.digitalasset.canton.protocol.{DynamicDomainParameters, OnboardingRestriction}
import com.digitalasset.canton.topology.DefaultTestIdentities.{mediatorId, sequencerId}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.TopologyStoreId.AuthorizedStore
import com.digitalasset.canton.topology.store.TopologyTransactionRejection.{
  InvalidTopologyMapping,
  PartyExceedsHostingLimit,
}
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransaction,
  StoredTopologyTransactions,
  TopologyStore,
  TopologyTransactionRejection,
}
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{
  Confirmation,
  Observation,
  Submission,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Replace
import com.digitalasset.canton.topology.transaction.TopologyMapping.Code
import com.digitalasset.canton.{BaseTest, HasExecutionContext, ProtocolVersionChecksAnyWordSpec}
import org.scalatest.wordspec.AnyWordSpec

import scala.annotation.nowarn
import scala.language.implicitConversions

@nowarn("msg=match may not be exhaustive")
class ValidatingTopologyMappingChecksTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with ProtocolVersionChecksAnyWordSpec {

  private lazy val factory = new TestingOwnerWithKeys(
    DefaultTestIdentities.mediatorId,
    loggerFactory,
    initEc = parallelExecutionContext,
  )

  def mk() = {
    val store = new InMemoryTopologyStore(AuthorizedStore, loggerFactory, timeouts)
    val check = new ValidatingTopologyMappingChecks(store, loggerFactory)
    (check, store)
  }

  "TopologyMappingChecks" when {
    import DefaultTestIdentities.{
      domainId,
      participant1,
      participant2,
      participant3,
      party1,
      party2,
      party3,
    }
    import factory.TestingTransactions.*

    def checkTransaction(
        checks: TopologyMappingChecks,
        toValidate: GenericSignedTopologyTransaction,
        inStore: Option[GenericSignedTopologyTransaction] = None,
    ): Either[TopologyTransactionRejection, Unit] =
      checks.checkTransaction(EffectiveTime.MaxValue, toValidate, inStore).value.futureValue

    implicit def toHostingParticipant(
        participantToPermission: (ParticipantId, ParticipantPermission)
    ): HostingParticipant =
      HostingParticipant(participantToPermission._1, participantToPermission._2)

    "validating any Mapping" should {
      "reject removal of non-existent mappings" in {
        import factory.SigningKeys.key1
        val (checks, _) = mk()

        val removeNsdSerial1 = factory.mkRemove(
          NamespaceDelegation.tryCreate(Namespace(key1.fingerprint), key1, isRootDelegation = true),
          serial = PositiveInt.one,
        )
        // also check that for serial > 1
        val removeNsdSerial3 = factory.mkRemove(
          NamespaceDelegation.tryCreate(Namespace(key1.fingerprint), key1, isRootDelegation = true),
          serial = PositiveInt.three,
        )
        checkTransaction(checks, removeNsdSerial1) shouldBe Left(
          TopologyTransactionRejection.NoCorrespondingActiveTxToRevoke(removeNsdSerial1.mapping)
        )
        checkTransaction(checks, removeNsdSerial3) shouldBe Left(
          TopologyTransactionRejection.NoCorrespondingActiveTxToRevoke(removeNsdSerial3.mapping)
        )
      }
    }

    "validating DecentralizedNamespaceDefinition" should {
      "reject namespaces not derived from their owners' namespaces" in {
        val (checks, store) = mk()
        val (keys, namespaces, rootCerts) = setUpRootCerts(
          factory.SigningKeys.key1,
          factory.SigningKeys.key2,
          factory.SigningKeys.key3,
        )

        addToStore(store, rootCerts*)

        val dns = factory.mkAddMultiKey(
          DecentralizedNamespaceDefinition
            .create(
              Namespace(Fingerprint.tryCreate("bogusNamespace")),
              PositiveInt.one,
              NonEmpty.from(namespaces).value.toSet,
            )
            .value,
          signingKeys = keys,
          // using serial=2 here to test that we don't special case serial=1
          serial = PositiveInt.two,
        )

        checkTransaction(checks, dns, None) should matchPattern {
          case Left(TopologyTransactionRejection.InvalidTopologyMapping(err))
              if err.contains("not derived from the owners") =>
        }
      }

      "reject if a namespace delegation with the same namespace already exists" in {
        val (checks, store) = mk()
        val (keys, namespaces, rootCerts) = setUpRootCerts(
          factory.SigningKeys.key1,
          factory.SigningKeys.key2,
          factory.SigningKeys.key3,
        )

        val dnd_namespace = DecentralizedNamespaceDefinition.computeNamespace(namespaces.toSet)

        // we are creating namespace delegation with the same namespace as the decentralized namespace.
        // this nsd however is not actually fully authorized, but for the purpose of this test, we want to see
        // that the decentralized namespace definition gets rejected.
        val conflicting_nsd = factory.mkAdd(
          NamespaceDelegation
            .tryCreate(dnd_namespace, factory.SigningKeys.key8, isRootDelegation = false),
          factory.SigningKeys.key8,
        )
        addToStore(store, (rootCerts :+ conflicting_nsd)*)

        val dnd = factory.mkAddMultiKey(
          DecentralizedNamespaceDefinition
            .create(
              dnd_namespace,
              PositiveInt.one,
              NonEmpty.from(namespaces).value.toSet,
            )
            .value,
          signingKeys = keys,
          serial = PositiveInt.one,
        )

        checkTransaction(checks, dnd, None) shouldBe Left(
          TopologyTransactionRejection.NamespaceAlreadyInUse(`dnd_namespace`)
        )
      }
    }

    "validating NamespaceDelegation" should {
      "reject a namespace delegation if a decentralized namespace with the same namespace already exists" in {
        val (checks, store) = mk()
        val (rootKeys, namespaces, rootCerts) = setUpRootCerts(
          factory.SigningKeys.key1,
          factory.SigningKeys.key2,
          factory.SigningKeys.key3,
        )

        val dnd_namespace = DecentralizedNamespaceDefinition.computeNamespace(namespaces.toSet)

        val dnd = factory.mkAddMultiKey(
          DecentralizedNamespaceDefinition
            .create(
              dnd_namespace,
              PositiveInt.one,
              NonEmpty.from(namespaces).value.toSet,
            )
            .value,
          signingKeys = rootKeys,
          serial = PositiveInt.one,
        )

        addToStore(store, (rootCerts :+ dnd)*)

        // we are creating namespace delegation with the same namespace as the decentralized namespace.
        // even if it is signed by enough owners of the decentralized namespace, we don't allow namespace delegations
        // for a decentralized namespace, because
        // 1. it goes against the very purpose of a decentralized namespace
        // 2. the authorization machinery is actually not prepared to deal with it
        // A similar effect can be achieved by setting the threshold of the DND to 1
        val conflicting_nsd = factory.mkAddMultiKey(
          NamespaceDelegation
            .tryCreate(dnd_namespace, factory.SigningKeys.key8, isRootDelegation = false),
          rootKeys,
        )

        checkTransaction(checks, conflicting_nsd, None) shouldBe Left(
          TopologyTransactionRejection.NamespaceAlreadyInUse(`dnd_namespace`)
        )
      }
    }

    "validating PartyToParticipant" should {

      "reject when participants don't have a DTC" in {
        val (checks, store) = mk()
        addToStore(store, p2_dtc)

        val failureCases = Seq(Seq(participant1), Seq(participant1, participant2))

        failureCases.foreach { participants =>
          val ptp = factory.mkAdd(
            PartyToParticipant.tryCreate(
              party1,
              None,
              PositiveInt.one,
              participants.map[HostingParticipant](_ -> Submission),
              groupAddressing = false,
            )
          )
          checkTransaction(checks, ptp) shouldBe Left(
            TopologyTransactionRejection.UnknownMembers(Seq(participant1))
          )
        }
      }

      "reject when participants don't have a valid encryption or signing key" in {
        val (checks, store) = mk()
        val p2MissingEncKey = factory.mkAdd(
          OwnerToKeyMapping(participant2, None, NonEmpty(Seq, factory.SigningKeys.key1))
        )
        val p3MissingSigningKey = factory.mkAdd(
          OwnerToKeyMapping(participant3, None, NonEmpty(Seq, factory.EncryptionKeys.key1))
        )

        addToStore(store, p1_dtc, p2_dtc, p3_dtc, p2MissingEncKey, p3MissingSigningKey)

        val missingKeyCases = Seq(participant1, participant2, participant3)

        missingKeyCases.foreach { participant =>
          val ptp = factory.mkAdd(
            PartyToParticipant.tryCreate(
              party1,
              None,
              PositiveInt.one,
              Seq(participant -> Submission),
              groupAddressing = false,
            )
          )
          checkTransaction(checks, ptp) shouldBe Left(
            TopologyTransactionRejection.InsufficientKeys(Seq(participant))
          )
        }
      }

      "reject when the party exceeds the explicitly issued PartyHostingLimits" in {
        def mkPTP(numParticipants: Int) = {
          val hostingParticipants = Seq[HostingParticipant](
            participant1 -> Observation,
            participant2 -> Submission,
            participant3 -> Submission,
          )
          factory.mkAdd(
            PartyToParticipant.tryCreate(
              partyId = party1,
              domainId = None,
              threshold = PositiveInt.one,
              participants = hostingParticipants.take(numParticipants),
              groupAddressing = false,
            )
          )
        }

        val (checks, store) = mk()
        val limits = factory.mkAdd(PartyHostingLimits(domainId, party1, 2))
        addToStore(store, p1_otk, p1_dtc, p2_otk, p2_dtc, p3_otk, p3_dtc, limits)

        // 2 participants are at the limit
        val twoParticipants = mkPTP(numParticipants = 2)
        checkTransaction(checks, twoParticipants) shouldBe Right(())

        // 3 participants exceed the limit imposed by the domain
        val threeParticipants = mkPTP(numParticipants = 3)
        checkTransaction(checks, threeParticipants) shouldBe Left(
          PartyExceedsHostingLimit(party1, 2, 3)
        )

      }

      "report no errors for valid mappings" in {
        val (checks, store) = mk()
        addToStore(store, p1_otk, p1_dtc, p2_otk, p2_dtc, p3_otk, p3_dtc)

        val validCases = Seq[(PositiveInt, Seq[HostingParticipant])](
          PositiveInt.one -> Seq(participant1 -> Confirmation),
          PositiveInt.one -> Seq(participant1 -> Submission),
          PositiveInt.one -> Seq(participant1 -> Observation, participant2 -> Confirmation),
          PositiveInt.two -> Seq(participant1 -> Confirmation, participant2 -> Submission),
          PositiveInt.two -> Seq(
            participant1 -> Observation,
            participant2 -> Submission,
            participant3 -> Submission,
          ),
        )

        validCases.foreach { case (threshold, participants) =>
          val ptp = factory.mkAdd(
            PartyToParticipant.tryCreate(
              party1,
              None,
              threshold,
              participants,
              groupAddressing = false,
            )
          )
          checkTransaction(checks, ptp) shouldBe Right(())
        }
      }

    }

    "validating DomainTrustCertificate" should {
      "reject a removal when the participant still hosts a party" in {
        val (checks, store) = mk()
        val ptp = factory.mkAdd(
          PartyToParticipant.tryCreate(
            party1,
            None,
            PositiveInt.one,
            Seq(participant1 -> Submission),
            groupAddressing = false,
          )
        )
        addToStore(
          store,
          ptp,
        )
        val prior = factory.mkAdd(DomainTrustCertificate(participant1, domainId, false, Seq.empty))

        val dtc =
          factory.mkRemove(DomainTrustCertificate(participant1, domainId, false, Seq.empty))

        checkTransaction(checks, dtc, Some(prior)) shouldBe Left(
          TopologyTransactionRejection.ParticipantStillHostsParties(participant1, Seq(party1))
        )

      }

      "reject the addition if the domain is locked" in {
        Seq(OnboardingRestriction.RestrictedLocked, OnboardingRestriction.UnrestrictedLocked)
          .foreach { restriction =>
            val (checks, store) = mk()
            val ptp = factory.mkAdd(
              DomainParametersState(
                domainId,
                DynamicDomainParameters
                  .defaultValues(testedProtocolVersion)
                  .tryUpdate(onboardingRestriction = restriction),
              )
            )
            addToStore(store, ptp)

            val dtc =
              factory.mkAdd(DomainTrustCertificate(participant1, domainId, false, Seq.empty))

            checkTransaction(checks, dtc) shouldBe Left(
              TopologyTransactionRejection.OnboardingRestrictionInPlace(
                participant1,
                restriction,
                None,
              )
            )
          }
      }

      "reject the addition if the domain is restricted" in {
        val (checks, store) = mk()
        val ptp = factory.mkAdd(
          DomainParametersState(
            domainId,
            DynamicDomainParameters
              .defaultValues(testedProtocolVersion)
              .tryUpdate(onboardingRestriction = OnboardingRestriction.RestrictedOpen),
          )
        )
        addToStore(
          store,
          ptp,
          factory.mkAdd(
            ParticipantDomainPermission(
              domainId,
              participant1,
              ParticipantPermission.Submission,
              None,
              None,
            )
          ),
        )

        // participant2 does not have permission from the domain to join
        checkTransaction(
          checks,
          factory.mkAdd(DomainTrustCertificate(participant2, domainId, false, Seq.empty)),
        ) shouldBe Left(
          TopologyTransactionRejection.OnboardingRestrictionInPlace(
            participant2,
            OnboardingRestriction.RestrictedOpen,
            None,
          )
        )

        // participant1 has been permissioned by the domain
        checkTransaction(
          checks,
          factory.mkAdd(DomainTrustCertificate(participant1, domainId, false, Seq.empty)),
          None,
        ) shouldBe Right(())
      }
    }

    "validating MediatorDomainState" should {
      "report no errors for valid mappings" in {
        val (checks, store) = mk()
        val (Seq(med1, med2), transactions) = generateMemberIdentities(2, MediatorId(_))
        addToStore(store, transactions*)

        val mds1 = factory.mkAdd(
          MediatorDomainState
            .create(
              domainId,
              NonNegativeInt.zero,
              PositiveInt.one,
              active = Seq(med1),
              Seq.empty,
            )
            .value,
          // the signing key is not relevant for the test
          factory.SigningKeys.key1,
        )

        val mds2 = factory.mkAdd(
          MediatorDomainState
            .create(
              domainId,
              NonNegativeInt.zero,
              PositiveInt.one,
              active = Seq(med1, med2),
              Seq.empty,
            )
            .value,
          // the signing key is not relevant for the test
          factory.SigningKeys.key1,
        )

        checkTransaction(checks, mds1) shouldBe Right(())
        checkTransaction(checks, mds2, Some(mds1)) shouldBe Right(())
      }

      "report MissingMappings for mediators with partial or missing identity transactions" in {
        val (checks, store) = mk()
        val (Seq(med1, med2, med3, med4), transactions) = generateMemberIdentities(4, MediatorId(_))

        val incompleteIdentities = transactions.filter { transaction =>
          (transaction.mapping.code, transaction.mapping.namespace) match {
            case (Code.OwnerToKeyMapping, namespace) =>
              Set(med1, med3).map(_.namespace).contains(namespace)
            case (Code.NamespaceDelegation, namespace) =>
              Set(med1, med2).map(_.namespace).contains(namespace)
            case otherwise => fail(s"unexpected mapping: $otherwise")
          }
        }
        addToStore(store, incompleteIdentities*)

        val mds1 = factory.mkAdd(
          MediatorDomainState
            .create(
              domainId,
              NonNegativeInt.zero,
              PositiveInt.one,
              active = Seq(med1, med2, med3, med4),
              Seq.empty,
            )
            .value,
          // the signing key is not relevant for the test
          factory.SigningKeys.key1,
        )

        checkTransaction(checks, mds1, None) shouldBe Left(
          TopologyTransactionRejection.MissingMappings(
            Map(
              med2 -> Seq(Code.OwnerToKeyMapping),
              med3 -> Seq(Code.NamespaceDelegation),
              med4 -> Seq(Code.NamespaceDelegation, Code.OwnerToKeyMapping),
            )
          )
        )
      }
    }

    "validating SequencerDomainState" should {
      "report no errors for valid mappings" in {
        val (checks, store) = mk()
        val (Seq(seq1, seq2), transactions) = generateMemberIdentities(2, SequencerId(_))
        addToStore(store, transactions*)

        val sds1 = factory.mkAdd(
          SequencerDomainState
            .create(
              domainId,
              PositiveInt.one,
              active = Seq(seq1),
              Seq.empty,
            )
            .value,
          // the signing key is not relevant for the test
          factory.SigningKeys.key1,
        )

        val sds2 = factory.mkAdd(
          SequencerDomainState
            .create(
              domainId,
              PositiveInt.one,
              active = Seq(seq1, seq2),
              Seq.empty,
            )
            .value,
          // the signing key is not relevant for the test
          factory.SigningKeys.key1,
        )

        checkTransaction(checks, sds1) shouldBe Right(())
        checkTransaction(checks, sds2, Some(sds1)) shouldBe Right(())
      }

      "report MissingMappings for sequencers with partial or missing identity transactions" in {
        val (checks, store) = mk()
        val (Seq(seq1, seq2, seq3, seq4), transactions) =
          generateMemberIdentities(4, SequencerId(_))

        val incompleteIdentities = transactions.filter { transaction =>
          (transaction.mapping.code, transaction.mapping.namespace) match {
            case (Code.OwnerToKeyMapping, namespace) =>
              Set(seq1, seq3).map(_.namespace).contains(namespace)
            case (Code.NamespaceDelegation, namespace) =>
              Set(seq1, seq2).map(_.namespace).contains(namespace)
            case otherwise => fail(s"unexpected mapping: $otherwise")
          }
        }
        addToStore(store, incompleteIdentities*)

        val sds1 = factory.mkAdd(
          SequencerDomainState
            .create(
              domainId,
              PositiveInt.one,
              active = Seq(seq1, seq2, seq3, seq4),
              Seq.empty,
            )
            .value,
          // the signing key is not relevant for the test
          factory.SigningKeys.key1,
        )

        checkTransaction(checks, sds1, None) shouldBe Left(
          TopologyTransactionRejection.MissingMappings(
            Map(
              seq2 -> Seq(Code.OwnerToKeyMapping),
              seq3 -> Seq(Code.NamespaceDelegation),
              seq4 -> Seq(Code.NamespaceDelegation, Code.OwnerToKeyMapping),
            )
          )
        )
      }
    }

    "validating OwnerToKeyMapping" should {
      "report no errors for valid mappings" in {
        val (checks, _) = mk()
        val okm_sequencer = factory.mkAddMultiKey(
          OwnerToKeyMapping(sequencerId, None, NonEmpty(Seq, factory.SigningKeys.key1)),
          NonEmpty(Set, factory.SigningKeys.key1),
        )
        val okm_mediator = factory.mkAddMultiKey(
          OwnerToKeyMapping(mediatorId, None, NonEmpty(Seq, factory.SigningKeys.key1)),
          NonEmpty(Set, factory.SigningKeys.key1),
        )
        val okm_participant = factory.mkAddMultiKey(
          OwnerToKeyMapping(
            participant1,
            None,
            NonEmpty(Seq, factory.EncryptionKeys.key1, factory.SigningKeys.key1),
          ),
          NonEmpty(Set, factory.SigningKeys.key1),
        )

        checkTransaction(checks, okm_sequencer) shouldBe Right(())
        checkTransaction(checks, okm_mediator) shouldBe Right(())
        checkTransaction(checks, okm_participant) shouldBe Right(())
      }
      "reject minimum key violations" in {
        val (checks, _) = mk()
        val okm_sequencerNoSigningKey = factory.mkAddMultiKey(
          OwnerToKeyMapping(sequencerId, None, NonEmpty(Seq, factory.EncryptionKeys.key1)),
          NonEmpty(Set, factory.SigningKeys.key1),
        )
        val okm_mediatorNoSigningKey = factory.mkAddMultiKey(
          OwnerToKeyMapping(mediatorId, None, NonEmpty(Seq, factory.EncryptionKeys.key1)),
          NonEmpty(Set, factory.SigningKeys.key1),
        )
        val okm_participantNoSigningKey = factory.mkAddMultiKey(
          OwnerToKeyMapping(participant1, None, NonEmpty(Seq, factory.EncryptionKeys.key1)),
          NonEmpty(Set, factory.SigningKeys.key1),
        )
        val okm_participantNoEncryptionKey = factory.mkAddMultiKey(
          OwnerToKeyMapping(participant1, None, NonEmpty(Seq, factory.SigningKeys.key1)),
          NonEmpty(Set, factory.SigningKeys.key1),
        )

        Seq(okm_sequencerNoSigningKey, okm_mediatorNoSigningKey, okm_participantNoSigningKey)
          .foreach(tx =>
            checkTransaction(checks, tx) shouldBe Left(
              InvalidTopologyMapping(
                "OwnerToKeyMapping must contain at least 1 signing key."
              )
            )
          )
        checkTransaction(checks, okm_participantNoEncryptionKey) shouldBe Left(
          InvalidTopologyMapping(
            "OwnerToKeyMapping for participants must contain at least 1 encryption key."
          )
        )
      }
    }

    "validating AuthorityOf" should {
      val ptps @ Seq(p1_ptp, p2_ptp, p3_ptp) = Seq(party1, party2, party3).map { party =>
        factory.mkAdd(
          PartyToParticipant.tryCreate(
            party,
            None,
            PositiveInt.one,
            Seq(HostingParticipant(participant1, ParticipantPermission.Confirmation)),
            groupAddressing = false,
          )
        )
      }
      "report no errors for valid mappings" in {
        val (checks, store) = mk()
        addToStore(store, ptps*)

        val authorityOf =
          factory.mkAdd(
            AuthorityOf.create(party1, None, PositiveInt.two, Seq(party2, party3)).value
          )
        checkTransaction(checks, authorityOf) shouldBe Right(())
      }

      "report UnknownParties for missing PTPs for referenced parties" in {
        val (checks, store) = mk()
        addToStore(store, p1_ptp)

        val missingAuthorizingParty =
          factory.mkAdd(AuthorityOf.create(party2, None, PositiveInt.one, Seq(party1)).value)
        checkTransaction(checks, missingAuthorizingParty) shouldBe Left(
          TopologyTransactionRejection.UnknownParties(Seq(party2))
        )

        val missingAuthorizedParty =
          factory.mkAdd(AuthorityOf.create(party1, None, PositiveInt.one, Seq(party2)).value)
        checkTransaction(checks, missingAuthorizedParty) shouldBe Left(
          TopologyTransactionRejection.UnknownParties(Seq(party2))
        )

        val missingAllParties =
          factory.mkAdd(AuthorityOf.create(party2, None, PositiveInt.one, Seq(party3)).value)
        checkTransaction(checks, missingAllParties) shouldBe Left(
          TopologyTransactionRejection.UnknownParties(Seq(party2, party3))
        )

        val missingMixedParties =
          factory.mkAdd(
            AuthorityOf.create(party2, None, PositiveInt.one, Seq(party1, party3)).value
          )
        checkTransaction(checks, missingMixedParties) shouldBe Left(
          TopologyTransactionRejection.UnknownParties(Seq(party2, party3))
        )
      }
    }
  }

  private def generateMemberIdentities[M <: Member](
      numMembers: Int,
      uidToMember: UniqueIdentifier => M,
  ): (Seq[M], Seq[GenericSignedTopologyTransaction]) = {
    val allKeys = {
      import factory.SigningKeys.*
      Seq(key1, key2, key3, key4, key5, key6)
    }
    val (memberIds, identityTransactions) = (1 to numMembers).map { idx =>
      val key = allKeys(idx)
      val member =
        uidToMember(UniqueIdentifier.tryCreate(s"member$idx", Namespace(key.fingerprint)))
      member -> List(
        factory.mkAdd(
          NamespaceDelegation.tryCreate(member.namespace, key, isRootDelegation = true),
          key,
        ),
        factory.mkAdd(OwnerToKeyMapping(member, None, NonEmpty(Seq, key)), key),
      )
    }.unzip

    memberIds -> identityTransactions.flatten
  }

  private def addToStore(
      store: TopologyStore[AuthorizedStore],
      transactions: GenericSignedTopologyTransaction*
  ): Unit = {
    store
      .bootstrap(
        StoredTopologyTransactions(
          transactions.map(tx =>
            StoredTopologyTransaction(SequencedTime.MinValue, EffectiveTime.MinValue, None, tx)
          )
        )
      )
      .futureValue
  }

  private def setUpRootCerts(keys: SigningPublicKey*): (
      NonEmpty[Set[SigningPublicKey]],
      Seq[Namespace],
      Seq[SignedTopologyTransaction[Replace, NamespaceDelegation]],
  ) = {
    val (namespaces, rootCerts) =
      keys.map { key =>
        val namespace = Namespace(key.fingerprint)
        namespace -> factory.mkAdd(
          NamespaceDelegation.tryCreate(
            namespace,
            key,
            isRootDelegation = true,
          ),
          signingKey = key,
        )
      }.unzip
    val keysNE = NonEmpty.from(keys).value.toSet
    (keysNE, namespaces, rootCerts)
  }

}
