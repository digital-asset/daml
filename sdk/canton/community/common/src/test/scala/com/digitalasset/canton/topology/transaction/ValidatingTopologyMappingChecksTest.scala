// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import cats.instances.order.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.{Fingerprint, SigningPublicKey}
import com.digitalasset.canton.protocol.{DynamicDomainParameters, OnboardingRestriction}
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.DefaultTestIdentities.{mediatorId, sequencerId}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.TopologyStoreId.AuthorizedStore
import com.digitalasset.canton.topology.store.TopologyTransactionRejection.InvalidTopologyMapping
import com.digitalasset.canton.topology.store.*
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{
  Confirmation,
  Observation,
  Submission,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Replace
import com.digitalasset.canton.topology.transaction.TopologyMapping.Code
import com.digitalasset.canton.topology.transaction.TopologyMappingChecks.PendingChangesLookup
import com.digitalasset.canton.{BaseTest, HasExecutionContext, ProtocolVersionChecksAnyWordSpec}
import org.scalatest.wordspec.AnyWordSpec

import java.time
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
    import DefaultTestIdentities.{domainId, participant1, participant2, participant3, party1}
    import factory.TestingTransactions.*

    def checkTransaction(
        checks: TopologyMappingChecks,
        toValidate: GenericSignedTopologyTransaction,
        inStore: Option[GenericSignedTopologyTransaction] = None,
        pendingChangesLookup: PendingChangesLookup = Map.empty,
    ): Either[TopologyTransactionRejection, Unit] =
      checks
        .checkTransaction(EffectiveTime.MaxValue, toValidate, inStore, pendingChangesLookup)
        .value
        .futureValue

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

      "reject if removal also changes the content" in {
        import factory.SigningKeys.{key1, key2}
        val (checks, _) = mk()

        val removeNs1k2 = factory.mkRemove(
          NamespaceDelegation
            .tryCreate(
              Namespace(key1.fingerprint),
              key2,
              // changing the mapping compared to ns1k2 by setting isRootDelegation = true
              isRootDelegation = true,
            ),
          serial = PositiveInt.two,
        )
        checkTransaction(checks, removeNs1k2, Some(ns1k2)) shouldBe Left(
          TopologyTransactionRejection.RemoveMustNotChangeMapping(
            removeNs1k2.mapping,
            ns1k2.mapping,
          )
        )
      }

      "respect pending changes when loading additional data for validations" in {
        import factory.SigningKeys.{key1, key2, key3}
        val (checks, store) = mk()
        val ns1 = Namespace(key1.fingerprint)
        val ns2 = Namespace(key2.fingerprint)
        val ns3 = Namespace(key3.fingerprint)

        val nsd1Replace_1 =
          factory.mkAdd(NamespaceDelegation.tryCreate(ns1, key1, isRootDelegation = true))
        val nsd1Remove_2 = factory.mkRemove(
          NamespaceDelegation.tryCreate(ns1, key1, isRootDelegation = true),
          serial = PositiveInt.two,
        )
        val nsd1ReplaceProposal_3 = factory.mkAdd(
          NamespaceDelegation.tryCreate(ns1, key1, isRootDelegation = true),
          serial = PositiveInt.three,
          isProposal = true,
        )

        val nsd2Replace_1 =
          factory.mkAdd(NamespaceDelegation.tryCreate(ns2, key2, isRootDelegation = true))
        val nsd2Remove_2 = factory.mkRemove(
          NamespaceDelegation.tryCreate(ns2, key2, isRootDelegation = true),
          serial = PositiveInt.two,
        )

        val nsd3Replace_1 =
          factory.mkAdd(NamespaceDelegation.tryCreate(ns3, key3, isRootDelegation = true))

        store
          .update(
            SequencedTime(ts),
            EffectiveTime(ts),
            removeMapping = Map.empty,
            removeTxs = Set.empty,
            additions = Seq(nsd1Replace_1, nsd2Replace_1).map(ValidatedTopologyTransaction(_)),
          )
          .futureValue

        store
          .update(
            SequencedTime(ts + seconds(1)),
            EffectiveTime(ts + seconds(1)),
            removeMapping = Map(nsd1Remove_2.mapping.uniqueKey -> nsd1Remove_2.serial),
            removeTxs = Set.empty,
            additions = Seq(ValidatedTopologyTransaction(nsd1Remove_2)),
          )
          .futureValue

        store
          .update(
            SequencedTime(ts + seconds(2)),
            EffectiveTime(ts + seconds(2)),
            removeMapping = Map.empty,
            removeTxs = Set.empty,
            additions = Seq(ValidatedTopologyTransaction(nsd1ReplaceProposal_3)),
          )
          .futureValue

        /*
         * The store contains the following transactions:
         * TS0: Replace NSD1, Replace NSD2
         * TS1: Remove NSD1
         * TS2: Replace Proposal NSD1
         */

        // TS0: load without pending changes
        checks
          .loadFromStore(
            EffectiveTime(ts.immediateSuccessor),
            codes = Set(Code.NamespaceDelegation),
            pendingChangesLookup = Map.empty,
          )
          .futureValue should contain theSameElementsAs Seq(nsd1Replace_1, nsd2Replace_1)

        // TS0: load with Removal NS2 as pending change
        checks
          .loadFromStore(
            EffectiveTime(ts.immediateSuccessor),
            codes = Set(Code.NamespaceDelegation),
            Map(nsd2Remove_2.mapping.uniqueKey -> nsd2Remove_2),
          )
          .futureValue shouldBe Seq(nsd1Replace_1)

        // TS0: load with Replace NS3 as pending change without prior transactions in the store
        checks
          .loadFromStore(
            EffectiveTime(ts.immediateSuccessor),
            codes = Set(Code.NamespaceDelegation),
            pendingChangesLookup = Map(nsd3Replace_1.mapping.uniqueKey -> nsd3Replace_1),
          )
          .futureValue should contain theSameElementsAs Seq(
          nsd1Replace_1,
          nsd2Replace_1,
          nsd3Replace_1,
        )

        // TS0: load with Replace NS3 as pending change without prior transactions in the store and also matching a
        // namespace filter
        checks
          .loadFromStore(
            EffectiveTime(ts.immediateSuccessor),
            codes = Set(Code.NamespaceDelegation),
            pendingChangesLookup = Map(nsd3Replace_1.mapping.uniqueKey -> nsd3Replace_1),
            filterNamespace = Some(Seq(ns2, ns3)),
          )
          .futureValue should contain theSameElementsAs Seq(nsd2Replace_1, nsd3Replace_1)

        // TS1: don't load Remove NS1 from the store
        checks
          .loadFromStore(
            EffectiveTime(ts.immediateSuccessor + seconds(1)),
            codes = Set(Code.NamespaceDelegation),
            Map.empty,
          )
          .futureValue shouldBe Seq(nsd2Replace_1)

        // TS1: don't load Remove NS1 from the store mixed with Remove NS2 as pending change
        checks
          .loadFromStore(
            EffectiveTime(ts.immediateSuccessor + seconds(1)),
            codes = Set(Code.NamespaceDelegation),
            Map(nsd2Remove_2.mapping.uniqueKey -> nsd2Remove_2),
          )
          .futureValue shouldBe Seq.empty

        // TS2: don't load proposals
        checks
          .loadFromStore(
            EffectiveTime(ts.immediateSuccessor + seconds(2)),
            codes = Set(Code.NamespaceDelegation),
            Map.empty,
          )
          .futureValue shouldBe Seq(nsd2Replace_1)

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
          signingKeys = keys.toSet,
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
          signingKeys = keys.toSet,
          serial = PositiveInt.one,
        )

        checkTransaction(checks, dnd, None) shouldBe Left(
          TopologyTransactionRejection.NamespaceAlreadyInUse(`dnd_namespace`)
        )
      }

      "reject if an owning namespace does not have a root certificate" in {
        val (checks, store) = mk()
        val (keys, namespaces, rootCerts) = setUpRootCerts(
          factory.SigningKeys.key1,
          factory.SigningKeys.key2,
          factory.SigningKeys.key3,
        )

        def createDND(owners: Seq[Namespace], keys: Seq[SigningPublicKey]) =
          factory.mkAddMultiKey(
            DecentralizedNamespaceDefinition
              .create(
                DecentralizedNamespaceDefinition.computeNamespace(owners.toSet),
                PositiveInt.one,
                NonEmpty.from(owners).value.toSet,
              )
              .value,
            signingKeys = NonEmpty.from(keys).value.toSet,
            serial = PositiveInt.one,
          )

        val dnd_k1k2 = createDND(namespaces.take(2), keys.take(2))

        addToStore(store, (rootCerts :+ dnd_k1k2)*)

        val ns4 = Namespace(factory.SigningKeys.key4.fingerprint)

        val dnd_invalid = createDND(
          namespaces.takeRight(2) ++ Seq(ns4, dnd_k1k2.mapping.namespace),
          // we don't have to provide all keys for this transaction to be fully authorized,
          // because the test doesn't check authorization, just semantic validity.
          keys.takeRight(2),
        )
        checkTransaction(checks, dnd_invalid, None) should matchPattern {
          case Left(TopologyTransactionRejection.InvalidTopologyMapping(err))
              if err.contains(
                s"No root certificate found for ${Seq(ns4, dnd_k1k2.mapping.namespace).sorted.mkString(", ")}"
              ) =>
        }
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
          signingKeys = rootKeys.toSet,
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
          rootKeys.toSet,
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
          OwnerToKeyMapping(participant2, NonEmpty(Seq, factory.SigningKeys.key1))
        )
        val p3MissingSigningKey = factory.mkAdd(
          OwnerToKeyMapping(participant3, NonEmpty(Seq, factory.EncryptionKeys.key1))
        )

        addToStore(store, p1_dtc, p2_dtc, p3_dtc, p2MissingEncKey, p3MissingSigningKey)

        val missingKeyCases = Seq(participant1, participant2, participant3)

        missingKeyCases.foreach { participant =>
          val ptp = factory.mkAdd(
            PartyToParticipant.tryCreate(
              party1,
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

      "handle conflicts between partyId and existing admin parties from domain trust certificates" in {
        // the defaults below are a valid explicit admin party allocation for participant1.adminParty
        def mkPTP(
            partyId: PartyId = participant1.adminParty,
            participants: Seq[HostingParticipant] =
              Seq(HostingParticipant(participant1, Submission)),
            groupdAddressing: Boolean = false,
        ) = factory.mkAdd(
          PartyToParticipant
            .create(
              partyId = partyId,
              threshold = PositiveInt.one,
              participants = participants,
              groupAddressing = groupdAddressing,
            )
            .value
        )

        val (checks, store) = mk()
        addToStore(store, p1_otk, p1_dtc, p2_otk, p2_dtc)

        // handle the happy case
        checkTransaction(checks, mkPTP()) shouldBe Right(())

        // unhappy scenarios
        val invalidParticipantPermission = Seq(
          mkPTP(participants = Seq(HostingParticipant(participant1, Confirmation))),
          mkPTP(participants = Seq(HostingParticipant(participant1, Observation))),
        )

        val invalidNumberOfHostingParticipants = mkPTP(participants =
          Seq(
            HostingParticipant(participant1, Submission),
            HostingParticipant(participant2, Submission),
          )
        )

        val foreignParticipant =
          mkPTP(participants = Seq(HostingParticipant(participant2, Submission)))

        val invalidGroupAddressing = mkPTP(groupdAddressing = true)

        // we don't need to explicitly check threshold > 1, because we already reject the PTP if participants.size > 1
        // and the threshold can never be higher than the number of participants

        val unhappyCases = invalidParticipantPermission ++ Seq(
          foreignParticipant,
          invalidNumberOfHostingParticipants,
          invalidGroupAddressing,
        )

        forAll(unhappyCases)(ptp =>
          checkTransaction(checks, ptp) shouldBe Left(
            TopologyTransactionRejection.PartyIdConflictWithAdminParty(ptp.mapping.partyId)
          )
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
            PositiveInt.one,
            Seq(participant1 -> Submission),
            groupAddressing = false,
          )
        )
        addToStore(
          store,
          ptp,
        )
        val prior = factory.mkAdd(DomainTrustCertificate(participant1, domainId))

        val dtc =
          factory.mkRemove(DomainTrustCertificate(participant1, domainId))

        checkTransaction(checks, dtc, Some(prior)) shouldBe Left(
          TopologyTransactionRejection.ParticipantStillHostsParties(participant1, Seq(party1))
        )
      }

      "handle conflicts with existing party allocations" in {
        val explicitAdminPartyParticipant1 = factory.mkAdd(
          PartyToParticipant
            .create(
              partyId = participant1.adminParty,
              threshold = PositiveInt.one,
              participants = Seq(HostingParticipant(participant1, Submission)),
              groupAddressing = false,
            )
            .value
        )

        // we allocate a party with participant2's UID on participant1.
        // this is not an explicit admin party allocation, the party just so happens to use the same UID as participant2.
        val partyWithParticipant2Uid = factory.mkAdd(
          PartyToParticipant
            .create(
              partyId = participant2.adminParty,
              threshold = PositiveInt.one,
              participants = Seq(HostingParticipant(participant1, Submission)),
              groupAddressing = false,
            )
            .value
        )

        val dop = factory.mkAdd(
          DomainParametersState(
            domainId,
            DynamicDomainParameters.defaultValues(testedProtocolVersion),
          )
        )

        val (checks, store) = mk()

        // normally it's not possible to have a valid PTP without an already existing DTC of the hosting participants.
        // but let's pretend for this check.
        addToStore(store, dop, explicitAdminPartyParticipant1, partyWithParticipant2Uid)

        // happy case: we allow the DTC (either a creation or modifying an existing one)
        // if there is a valid explicit admin party allocation
        checkTransaction(checks, p1_dtc, None) shouldBe Right(())

        // unhappy case: there already exists a normal party allocation with the same UID
        checkTransaction(checks, p2_dtc, None) shouldBe Left(
          TopologyTransactionRejection.ParticipantIdConflictWithPartyId(
            participant2,
            partyWithParticipant2Uid.mapping.partyId,
          )
        )
      }

      "reject the addition if the domain is locked" in {
        Seq(OnboardingRestriction.RestrictedLocked, OnboardingRestriction.UnrestrictedLocked)
          .foreach { restriction =>
            val (checks, store) = mk()
            val dop = factory.mkAdd(
              DomainParametersState(
                domainId,
                DynamicDomainParameters
                  .defaultValues(testedProtocolVersion)
                  .tryUpdate(onboardingRestriction = restriction),
              )
            )
            addToStore(store, dop)

            val dtc =
              factory.mkAdd(DomainTrustCertificate(participant1, domainId))

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
        val dop = factory.mkAdd(
          DomainParametersState(
            domainId,
            DynamicDomainParameters
              .defaultValues(testedProtocolVersion)
              .tryUpdate(onboardingRestriction = OnboardingRestriction.RestrictedOpen),
          )
        )
        addToStore(
          store,
          dop,
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
          factory.mkAdd(DomainTrustCertificate(participant2, domainId)),
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
          factory.mkAdd(DomainTrustCertificate(participant1, domainId)),
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

      "report MediatorsAlreadyAssignedToGroups for duplicate mediator assignments" in {
        val (checks, store) = mk()
        val (Seq(med1, med2, med3), transactions) = generateMemberIdentities(3, MediatorId(_))

        val Seq(group0, group1, group2) = Seq(
          NonNegativeInt.zero -> Seq(med1),
          NonNegativeInt.one -> Seq(med2),
          NonNegativeInt.two -> Seq(med1, med2, med3),
        ).map { case (group, mediators) =>
          factory.mkAdd(
            MediatorDomainState
              .create(
                domainId,
                group,
                PositiveInt.one,
                active = mediators,
                Seq.empty,
              )
              .value,
            // the signing key is not relevant for the test
            factory.SigningKeys.key1,
          )
        }

        addToStore(store, (transactions :+ group0 :+ group1)*)

        checkTransaction(checks, group2, None) shouldBe Left(
          TopologyTransactionRejection.MediatorsAlreadyInOtherGroups(
            NonNegativeInt.two,
            Map(med1 -> NonNegativeInt.zero, med2 -> NonNegativeInt.one),
          )
        )
      }

      "report mediators defined both as active and observers" in {
        val (Seq(med1, med2), _transactions) = generateMemberIdentities(2, MediatorId(_))

        MediatorDomainState
          .create(
            domainId,
            NonNegativeInt.zero,
            PositiveInt.one,
            active = Seq(med1, med2),
            observers = Seq(med1),
          ) shouldBe Left(
          s"the following mediators were defined both as active and observer: $med1"
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
      "report sequencers defined both as active and observers" in {
        val (Seq(seq1, seq2), _transactions) = generateMemberIdentities(2, SequencerId(_))

        SequencerDomainState
          .create(
            domainId,
            PositiveInt.one,
            active = Seq(seq1, seq2),
            observers = Seq(seq1),
          ) shouldBe Left(
          s"the following sequencers were defined both as active and observer: $seq1"
        )
      }

    }

    "validating OwnerToKeyMapping" should {
      "report no errors for valid mappings" in {
        val (checks, _) = mk()
        val okm_sequencer = factory.mkAddMultiKey(
          OwnerToKeyMapping(sequencerId, NonEmpty(Seq, factory.SigningKeys.key1)),
          NonEmpty(Set, factory.SigningKeys.key1),
        )
        val okm_mediator = factory.mkAddMultiKey(
          OwnerToKeyMapping(mediatorId, NonEmpty(Seq, factory.SigningKeys.key1)),
          NonEmpty(Set, factory.SigningKeys.key1),
        )
        val okm_participant = factory.mkAddMultiKey(
          OwnerToKeyMapping(
            participant1,
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
          OwnerToKeyMapping(sequencerId, NonEmpty(Seq, factory.EncryptionKeys.key1)),
          NonEmpty(Set, factory.SigningKeys.key1),
        )
        val okm_mediatorNoSigningKey = factory.mkAddMultiKey(
          OwnerToKeyMapping(mediatorId, NonEmpty(Seq, factory.EncryptionKeys.key1)),
          NonEmpty(Set, factory.SigningKeys.key1),
        )
        val okm_participantNoSigningKey = factory.mkAddMultiKey(
          OwnerToKeyMapping(participant1, NonEmpty(Seq, factory.EncryptionKeys.key1)),
          NonEmpty(Set, factory.SigningKeys.key1),
        )
        val okm_participantNoEncryptionKey = factory.mkAddMultiKey(
          OwnerToKeyMapping(participant1, NonEmpty(Seq, factory.SigningKeys.key1)),
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
        factory.mkAdd(OwnerToKeyMapping(member, NonEmpty(Seq, key)), key),
      )
    }.unzip

    memberIds -> identityTransactions.flatten
  }

  private def addToStore(
      store: TopologyStore[AuthorizedStore],
      transactions: GenericSignedTopologyTransaction*
  ): Unit =
    store
      .bootstrap(
        StoredTopologyTransactions(
          transactions.map(tx =>
            StoredTopologyTransaction(SequencedTime.MinValue, EffectiveTime.MinValue, None, tx)
          )
        )
      )
      .futureValue

  private def setUpRootCerts(keys: SigningPublicKey*): (
      NonEmpty[Seq[SigningPublicKey]],
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
    val keysNE = NonEmpty.from(keys).value
    (keysNE, namespaces, rootCerts)
  }

  private def seconds(s: Int) = PositiveSeconds.tryCreate(time.Duration.ofSeconds(s.toLong))

}
