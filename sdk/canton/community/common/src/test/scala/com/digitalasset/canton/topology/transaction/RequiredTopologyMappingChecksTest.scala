// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import cats.implicits.catsSyntaxOptionId
import cats.instances.order.*
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.{Fingerprint, SigningKeysWithThreshold, SigningPublicKey}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.{DynamicSynchronizerParameters, OnboardingRestriction}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.DefaultTestIdentities.{
  mediatorId,
  sequencerId,
  synchronizerId,
}
import com.digitalasset.canton.topology.cache.TopologyStateLookup
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.*
import com.digitalasset.canton.topology.store.StoredTopologyTransaction.GenericStoredTopologyTransaction
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.transaction.DelegationRestriction.{
  CanSignAllButNamespaceDelegations,
  CanSignAllMappings,
}
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{
  Confirmation,
  Observation,
  Submission,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Replace
import com.digitalasset.canton.topology.transaction.TopologyMapping.Code
import com.digitalasset.canton.topology.transaction.checks.{
  RequiredTopologyMappingChecks,
  TopologyMappingChecks,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{
  BaseTest,
  FailOnShutdown,
  HasExecutionContext,
  ProtocolVersionChecksAnyWordSpec,
}
import org.scalatest.wordspec.AnyWordSpec

import scala.annotation.nowarn
import scala.concurrent.ExecutionContext
import scala.language.implicitConversions

private[transaction] abstract class BaseTopologyMappingChecksTest[T <: TopologyMappingChecks]
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with ProtocolVersionChecksAnyWordSpec
    with FailOnShutdown {

  protected def mkChecks(store: TopologyStore[SynchronizerStore]): T

  protected lazy val factory = new TestingOwnerWithKeys(
    DefaultTestIdentities.mediatorId,
    loggerFactory,
    initEc = parallelExecutionContext,
  )

  protected def mk() = {
    val store =
      new InMemoryTopologyStore(
        SynchronizerStore(DefaultTestIdentities.physicalSynchronizerId),
        testedProtocolVersion,
        loggerFactory,
        timeouts,
      )
    val check = mkChecks(store)
    (check, store)
  }
  protected def addToStore(
      store: TopologyStore[SynchronizerStore],
      transactions: GenericSignedTopologyTransaction*
  ): Unit =
    store
      .update(
        sequenced = SequencedTime.MinValue,
        effective = EffectiveTime.MinValue,
        removals = Map.empty,
        additions = transactions.map(ValidatedTopologyTransaction(_)),
      )
      .futureValueUS

  protected def generateMemberIdentities[M <: Member](
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
          NamespaceDelegation.tryCreate(member.namespace, key, CanSignAllMappings),
          key,
        ),
        factory.mkAdd(OwnerToKeyMapping.tryCreate(member, NonEmpty(Seq, key)), key),
      )
    }.unzip

    memberIds -> identityTransactions.flatten
  }

  protected def mkMediatorGroups(
      serial: PositiveInt,
      groupSetup: (NonNegativeInt, Seq[MediatorId])*
  ): Seq[SignedTopologyTransaction[TopologyChangeOp.Replace, MediatorSynchronizerState]] =
    groupSetup.map { case (group, mediators) =>
      factory.mkAdd(
        MediatorSynchronizerState
          .create(
            synchronizerId,
            group,
            PositiveInt.one,
            active = mediators,
            Seq.empty,
          )
          .value,
        // the signing key is not relevant for the test
        signingKey = factory.SigningKeys.key1,
        serial = serial,
      )
    }

  protected def makeSynchronizerState(
      serial: PositiveInt,
      sequencers: SequencerId*
  ): SignedTopologyTransaction[TopologyChangeOp.Replace, SequencerSynchronizerState] =
    factory.mkAdd(
      SequencerSynchronizerState
        .create(
          synchronizerId,
          PositiveInt.one,
          active = sequencers,
          Seq.empty,
        )
        .value,
      // the signing key is not relevant for the test
      signingKey = factory.SigningKeys.key1,
      serial = serial,
    )
  def checkTransaction(
      checks: TopologyMappingChecks,
      toValidate: GenericSignedTopologyTransaction,
      inStore: Option[GenericSignedTopologyTransaction] = None,
      relaxChecksForBackwardsCompatibility: Boolean = false,
  ): Either[TopologyTransactionRejection, Unit] =
    checks
      .checkTransaction(
        EffectiveTime.MaxValue,
        toValidate,
        inStore,
        relaxChecksForBackwardsCompatibility,
      )
      .value
      .futureValueUS

  implicit def toHostingParticipant(
      participantToPermission: (ParticipantId, ParticipantPermission)
  ): HostingParticipant =
    HostingParticipant(participantToPermission._1, participantToPermission._2)

}

@nowarn("msg=match may not be exhaustive")
class RequiredTopologyMappingChecksTest
    extends BaseTopologyMappingChecksTest[RequiredTopologyMappingChecks] {

  override protected def mkChecks(
      store: TopologyStore[SynchronizerStore]
  ): RequiredTopologyMappingChecks =
    RequiredTopologyMappingChecks(
      Some(defaultStaticSynchronizerParameters),
      new TopologyStateLookup {
        override def lookupHistoryForUid(
            asOf: EffectiveTime,
            asOfInclusive: Boolean,
            uid: UniqueIdentifier,
            transactionType: Code,
            op: TopologyChangeOp,
        )(implicit
            traceContext: TraceContext
        ): FutureUnlessShutdown[Seq[GenericStoredTopologyTransaction]] = ???

        override def synchronizerId: Option[PhysicalSynchronizerId] = store.storeId.forSynchronizer

        override def lookupForUid(
            asOf: EffectiveTime,
            asOfInclusive: Boolean,
            uid: UniqueIdentifier,
            transactionTypes: Set[Code],
            op: TopologyChangeOp,
        )(implicit
            traceContext: TraceContext
        ): FutureUnlessShutdown[Seq[GenericStoredTopologyTransaction]] = op match {
          case TopologyChangeOp.Replace =>
            store
              .findPositiveTransactions(
                asOf = asOf.value,
                asOfInclusive = asOfInclusive,
                isProposal = false,
                types = transactionTypes.toSeq,
                filterUid = NonEmpty.mk(Seq, uid).some,
                filterNamespace = None,
              )
              .map(_.result)
          case TopologyChangeOp.Remove => ???
        }

        override def lookupForNamespace(
            asOf: EffectiveTime,
            asOfInclusive: Boolean,
            ns: Namespace,
            transactionTypes: Set[Code],
            op: TopologyChangeOp,
        )(implicit
            traceContext: TraceContext
        ): FutureUnlessShutdown[Seq[GenericStoredTopologyTransaction]] =
          lookupForNamespaces(asOf, asOfInclusive, NonEmpty.mk(Seq, ns), transactionTypes, op)
            .map(_.toSeq.flatMap(_._2))

        override def lookupForNamespaces(
            asOf: EffectiveTime,
            asOfInclusive: Boolean,
            ns: NonEmpty[Seq[Namespace]],
            transactionTypes: Set[Code],
            op: TopologyChangeOp,
        )(implicit
            traceContext: TraceContext,
            executionContext: ExecutionContext,
        ): FutureUnlessShutdown[Map[Namespace, Seq[GenericStoredTopologyTransaction]]] = op match {
          case TopologyChangeOp.Replace =>
            store
              .findPositiveTransactions(
                asOf = asOf.value,
                asOfInclusive = asOfInclusive,
                isProposal = false,
                types = transactionTypes.toSeq,
                filterUid = None,
                filterNamespace = ns.some,
              )
              .map(_.result.groupBy(_.mapping.namespace))(executionContext)
          case TopologyChangeOp.Remove =>
            store
              .findNegativeTransactions(
                asOf = asOf.value,
                asOfInclusive = asOfInclusive,
                isProposal = false,
                types = transactionTypes.toSeq,
                filterUid = None,
                filterNamespace = ns.some,
              )
              .map(_.result.groupBy(_.mapping.namespace))(executionContext)

        }

      },
      loggerFactory,
    )

  "RequiredTopologyMappingChecks" when {
    import DefaultTestIdentities.{synchronizerId, participant1, participant2, participant3, party1}
    import factory.TestingTransactions.*

    def checkTransaction(
        checks: TopologyMappingChecks,
        toValidate: GenericSignedTopologyTransaction,
        inStore: Option[GenericSignedTopologyTransaction] = None,
        relaxChecksForBackwardsCompatibility: Boolean = false,
    ): Either[TopologyTransactionRejection, Unit] =
      checks
        .checkTransaction(
          EffectiveTime.MaxValue,
          toValidate,
          inStore,
          relaxChecksForBackwardsCompatibility,
        )
        .value
        .futureValueUS

    implicit def toHostingParticipant(
        participantToPermission: (ParticipantId, ParticipantPermission)
    ): HostingParticipant =
      HostingParticipant(participantToPermission._1, participantToPermission._2)

    "validating any Mapping" should {
      "reject removal of non-existent mappings" in {
        import factory.SigningKeys.key1
        val (checks, _) = mk()

        val mapping = PartyToParticipant.tryCreate(
          PartyId.tryCreate("Alice", Namespace(key1.fingerprint)),
          PositiveInt.one,
          Seq(HostingParticipant(participant1, ParticipantPermission.Submission)),
        )

        val removePtpSerial1 = factory.mkRemove(
          mapping,
          serial = PositiveInt.one,
        )
        // also check that for serial > 1
        val removePtpSerial3 = factory.mkRemove(
          mapping,
          serial = PositiveInt.three,
        )
        checkTransaction(checks, removePtpSerial1) shouldBe Left(
          TopologyTransactionRejection.RequiredMapping.NoCorrespondingActiveTxToRevoke(
            removePtpSerial1.mapping
          )
        )
        checkTransaction(checks, removePtpSerial3) shouldBe Left(
          TopologyTransactionRejection.RequiredMapping.NoCorrespondingActiveTxToRevoke(
            removePtpSerial3.mapping
          )
        )
      }

      "reject only REPLACE transactions with the highest possible serial" in {
        import factory.SigningKeys.key1
        val (checks, _) = mk()

        val maxSerialReplace = factory.mkAdd(
          NamespaceDelegation.tryCreate(Namespace(key1.fingerprint), key1, CanSignAllMappings),
          serial = PositiveInt.MaxValue,
        )
        checkTransaction(checks, maxSerialReplace) shouldBe Left(
          TopologyTransactionRejection.RequiredMapping.InvalidTopologyMapping(
            s"The serial for a REPLACE must be less than ${PositiveInt.MaxValue}."
          )
        )

        val maxSerialMinsOneReplace = factory.mkAdd(
          NamespaceDelegation.tryCreate(Namespace(key1.fingerprint), key1, CanSignAllMappings),
          serial = PositiveInt.tryCreate(PositiveInt.MaxValue.value - 1),
        )
        val maxSerialRemove = factory.mkRemove(
          NamespaceDelegation.tryCreate(Namespace(key1.fingerprint), key1, CanSignAllMappings),
          serial = PositiveInt.MaxValue,
        )

        checkTransaction(checks, toValidate = maxSerialMinsOneReplace) shouldBe Right(())
        checkTransaction(
          checks,
          toValidate = maxSerialRemove,
          inStore = Some(maxSerialMinsOneReplace),
        ) shouldBe Right(())
      }

      "reject if removal also changes the content" in {
        import factory.SigningKeys.{key1, key2}
        val (checks, _) = mk()

        val removeNs1k2 = factory.mkRemove(
          NamespaceDelegation
            .tryCreate(
              Namespace(key1.fingerprint),
              key2,
              // changing the mapping compared to ns1k2 by setting CanSignAllMappings
              CanSignAllMappings,
            ),
          serial = PositiveInt.two,
        )
        checkTransaction(checks, removeNs1k2, Some(ns1k2)) shouldBe Left(
          TopologyTransactionRejection.RequiredMapping.RemoveMustNotChangeMapping(
            removeNs1k2.mapping,
            ns1k2.mapping,
          )
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
              Namespace(Fingerprint.tryFromString("bogusNamespace")),
              PositiveInt.one,
              NonEmpty.from(namespaces).value.toSet,
            )
            .value,
          signingKeys = keys.toSet,
          // using serial=2 here to test that we don't special case serial=1
          serial = PositiveInt.two,
        )

        checkTransaction(checks, dns, None) should matchPattern {
          case Left(TopologyTransactionRejection.RequiredMapping.InvalidTopologyMapping(err))
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
            .tryCreate(dnd_namespace, factory.SigningKeys.key8, CanSignAllButNamespaceDelegations),
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
          TopologyTransactionRejection.RequiredMapping.NamespaceAlreadyInUse(`dnd_namespace`)
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
          case Left(TopologyTransactionRejection.RequiredMapping.InvalidTopologyMapping(err))
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
            .tryCreate(dnd_namespace, factory.SigningKeys.key8, CanSignAllButNamespaceDelegations),
          rootKeys.toSet,
        )

        checkTransaction(checks, conflicting_nsd, None) shouldBe Left(
          TopologyTransactionRejection.RequiredMapping.NamespaceAlreadyInUse(`dnd_namespace`)
        )
      }

      "allow revoking a root NSD without prior existing positive one" in {
        val (checks, store) = mk()

        val revoke = factory.mkRemove(
          ns1k1.mapping,
          NonEmpty.mk(Set, ns1k1.mapping.target),
          serial = PositiveInt.one,
        )

        checkTransaction(checks, revoke, None) shouldBe Right(())
      }

      "reject re-creation if it has already been revoked" in {
        val (checks, store) = mk()

        val revoke = factory.mkRemove(
          ns1k1.mapping,
          NonEmpty.mk(Set, ns1k1.mapping.target),
          serial = PositiveInt.one,
        )
        addToStore(store, revoke)

        val recreate = factory.mkAdd(
          ns1k1.mapping,
          ns1k1.mapping.target,
          serial = PositiveInt.two,
        )

        checkTransaction(checks, recreate, Some(revoke)) shouldBe Left(
          TopologyTransactionRejection.RequiredMapping.NamespaceHasBeenRevoked(
            ns1k1.mapping.namespace
          )
        )
      }

      "reject re-creation of an intermediate NSD if it has already been revoked" in {
        val (checks, store) = mk()

        val revokeIntermediate = factory.mkRemove(
          ns1k2.mapping,
          NonEmpty.mk(Set, factory.SigningKeys.key1, factory.SigningKeys.key2),
          serial = PositiveInt.one,
        )

        val recreate = factory.mkAdd(
          ns1k1.mapping,
          ns1k2.mapping.target,
          serial = PositiveInt.two,
        )

        checkTransaction(checks, recreate, Some(revokeIntermediate)) shouldBe Left(
          TopologyTransactionRejection.RequiredMapping.NamespaceHasBeenRevoked(
            ns1k2.mapping.namespace
          )
        )
      }

    }

    "validating PartyToParticipant" should {

      "reject self-signing with a key that has a stored revoked NSD with the same key" in {
        val (checks, store) = mk()

        val revokedNsd = factory.mkRemove(
          ns1k1.mapping,
          NonEmpty.mk(Set, ns1k1.mapping.target),
        )

        addToStore(store, p2_otk, p2_dtc, revokedNsd)

        val ptp = factory.mkAdd(
          PartyToParticipant.tryCreate(
            PartyId.tryCreate("alice", ns1k1.mapping.namespace),
            PositiveInt.one,
            Seq(HostingParticipant(participant2, Submission)),
            partySigningKeysWithThreshold = Some(
              SigningKeysWithThreshold.tryCreate(
                NonEmpty.mk(Seq, ns1k1.mapping.target),
                PositiveInt.one,
              )
            ),
          )
        )

        checkTransaction(checks, ptp) shouldBe Left(
          TopologyTransactionRejection.RequiredMapping.NamespaceHasBeenRevoked(
            ns1k1.mapping.namespace
          )
        )
      }

      "reject self-signing with a key that has a revocation pending for a NSD with the same key" in {
        val (checks, store) = mk()

        val revokedNd = factory.mkRemove(
          ns1k1.mapping,
          NonEmpty.mk(Set, ns1k1.mapping.target),
        )

        addToStore(store, revokedNd, p2_otk, p2_dtc)

        val ptp = factory.mkAdd(
          PartyToParticipant.tryCreate(
            PartyId.tryCreate("alice", ns1k1.mapping.namespace),
            PositiveInt.one,
            Seq(HostingParticipant(participant2, Submission)),
            partySigningKeysWithThreshold = Some(
              SigningKeysWithThreshold.tryCreate(
                NonEmpty.mk(Seq, ns1k1.mapping.target),
                PositiveInt.one,
              )
            ),
          )
        )

        checkTransaction(
          checks,
          ptp,
        ) shouldBe Left(
          TopologyTransactionRejection.RequiredMapping.NamespaceHasBeenRevoked(
            ns1k1.mapping.namespace
          )
        )
      }

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
            )
          )
          checkTransaction(checks, ptp) shouldBe Left(
            TopologyTransactionRejection.RequiredMapping.UnknownMembers(Seq(participant1))
          )
        }
      }

      "reject when participants don't have an OTK" in {
        val (checks, store) = mk()

        addToStore(store, p1_dtc, p2_dtc, p3_dtc)

        val missingKeyCases = Seq(participant1)

        missingKeyCases.foreach { participant =>
          val ptp = factory.mkAdd(
            PartyToParticipant.tryCreate(
              party1,
              PositiveInt.one,
              Seq(participant -> Submission),
            )
          )
          checkTransaction(checks, ptp) shouldBe Left(
            TopologyTransactionRejection.RequiredMapping.InsufficientKeys(Seq(participant))
          )
        }
      }

      "handle conflicts between partyId and existing admin parties from synchronizer trust certificates" in {
        // the defaults below are a valid explicit admin party allocation for participant1.adminParty
        def mkPTP(
            partyId: PartyId = participant1.adminParty,
            participants: Seq[HostingParticipant] =
              Seq(HostingParticipant(participant1, Submission)),
        ) = factory.mkAdd(
          PartyToParticipant
            .create(
              partyId = partyId,
              threshold = PositiveInt.one,
              participants = participants,
              partySigningKeysWithThreshold = None,
            )
            .value
        )

        val (checks, store) = mk()
        addToStore(store, p1_otk, p1_dtc, p2_otk, p2_dtc)

        // handle the happy case
        checkTransaction(checks, mkPTP()) shouldBe Either.unit

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

        // we don't need to explicitly check threshold > 1, because we already reject the PTP if participants.size > 1
        // and the threshold can never be higher than the number of participants

        val unhappyCases = invalidParticipantPermission ++ Seq(
          foreignParticipant,
          invalidNumberOfHostingParticipants,
        )

        forAll(unhappyCases)(ptp =>
          checkTransaction(checks, ptp) shouldBe Left(
            TopologyTransactionRejection.RequiredMapping.PartyIdConflictWithAdminParty(
              ptp.mapping.partyId
            )
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
            )
          )
          checkTransaction(checks, ptp) shouldBe Either.unit
        }
      }

    }

    "validating SynchronizerTrustCertificate" should {

      "reject addition without valid otk" in {
        val (checks, store) = mk()
        addToStore(store, dpc1)
        checkTransaction(checks, p1_dtc, None) shouldBe Left(
          TopologyTransactionRejection.RequiredMapping.InsufficientKeys(Seq(participant1))
        )
      }

      "handle conflicts with existing party allocations" in {

        val explicitAdminPartyParticipant1 = factory.mkAdd(
          PartyToParticipant
            .create(
              partyId = participant1.adminParty,
              threshold = PositiveInt.one,
              participants = Seq(HostingParticipant(participant1, Submission)),
              partySigningKeysWithThreshold = None,
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
              partySigningKeysWithThreshold = None,
            )
            .value
        )

        val dop = factory.mkAdd(
          SynchronizerParametersState(
            synchronizerId,
            DynamicSynchronizerParameters.defaultValues(testedProtocolVersion),
          )
        )

        val (checks, store) = mk()

        // normally it's not possible to have a valid PTP without an already existing DTC of the hosting participants.
        // but let's pretend for this check.
        addToStore(store, dop, p1_otk, explicitAdminPartyParticipant1, partyWithParticipant2Uid)

        // happy case: we allow the DTC (either a creation or modifying an existing one)
        // if there is a valid explicit admin party allocation
        checkTransaction(checks, p1_dtc, None) shouldBe Either.unit

        // unhappy case: there already exists a normal party allocation with the same UID
        checkTransaction(checks, p2_dtc, None) shouldBe Left(
          TopologyTransactionRejection.RequiredMapping.ParticipantIdConflictWithPartyId(
            participant2,
            partyWithParticipant2Uid.mapping.partyId,
          )
        )
      }

      "reject the addition if the synchronizer is locked" in {
        Seq(OnboardingRestriction.RestrictedLocked, OnboardingRestriction.UnrestrictedLocked)
          .foreach { restriction =>
            val (checks, store) = mk()
            val dop = factory.mkAdd(
              SynchronizerParametersState(
                synchronizerId,
                DynamicSynchronizerParameters
                  .defaultValues(testedProtocolVersion)
                  .tryUpdate(onboardingRestriction = restriction),
              )
            )
            addToStore(store, dop)

            val dtc =
              factory.mkAdd(SynchronizerTrustCertificate(participant1, synchronizerId))

            checkTransaction(checks, dtc) shouldBe Left(
              TopologyTransactionRejection.RequiredMapping.OnboardingRestrictionInPlace(
                participant1,
                restriction,
                None,
              )
            )
          }
      }

      "reject the addition if the synchronizer is restricted" in {
        val (checks, store) = mk()

        val dop = factory.mkAdd(
          SynchronizerParametersState(
            synchronizerId,
            DynamicSynchronizerParameters
              .defaultValues(testedProtocolVersion)
              .tryUpdate(onboardingRestriction = OnboardingRestriction.RestrictedOpen),
          )
        )

        addToStore(
          store,
          dop,
          p1_otk,
          factory.mkAdd(
            ParticipantSynchronizerPermission(
              synchronizerId,
              participant1,
              ParticipantPermission.Submission,
              None,
              None,
            )
          ),
        )

        // participant2 does not have permission from the synchronizer to join
        checkTransaction(
          checks,
          factory.mkAdd(SynchronizerTrustCertificate(participant2, synchronizerId)),
        ) shouldBe Left(
          TopologyTransactionRejection.RequiredMapping.OnboardingRestrictionInPlace(
            participant2,
            OnboardingRestriction.RestrictedOpen,
            None,
          )
        )

        // participant1 has been permissioned by the synchronizer
        checkTransaction(
          checks,
          factory.mkAdd(SynchronizerTrustCertificate(participant1, synchronizerId)),
          None,
        ) shouldBe Either.unit
      }

      "reject a rejoining participant" in {
        val (checks, store) = mk()
        val dtcRemoval = factory.mkRemove(
          SynchronizerTrustCertificate(
            participant1,
            synchronizerId,
          )
        )
        addToStore(
          store,
          dtcRemoval,
        )
        val rejoin =
          factory.mkAdd(
            SynchronizerTrustCertificate(participant1, synchronizerId),
            serial = PositiveInt.two,
          )

        checkTransaction(checks, rejoin, Some(dtcRemoval)) shouldBe Left(
          TopologyTransactionRejection.RequiredMapping.ParticipantCannotRejoinSynchronizer(
            participant1
          )
        )
      }

    }

    "validating MediatorSynchronizerState" should {

      "report no errors for valid mappings" in {
        val (checks, store) = mk()
        val (Seq(med1, med2), transactions) = generateMemberIdentities(2, MediatorId(_))
        addToStore(store, transactions*)

        val Seq(mds1) = mkMediatorGroups(PositiveInt.one, (NonNegativeInt.zero -> Seq(med1)))
        val Seq(mds2) = mkMediatorGroups(PositiveInt.two, (NonNegativeInt.zero -> Seq(med1, med2)))

        checkTransaction(checks, mds1) shouldBe Either.unit
        checkTransaction(checks, mds2, Some(mds1)) shouldBe Either.unit
      }

      "bail if mediator has no keys" in {
        import DefaultTestIdentities.mediatorId
        val (checks, store) = mk()
        val mds1 = factory.mkAdd(
          MediatorSynchronizerState
            .create(
              synchronizerId,
              NonNegativeInt.zero,
              PositiveInt.one,
              active = Seq(mediatorId),
              Seq.empty,
            )
            .value,
          // the signing key is not relevant for the test
          signingKey = factory.SigningKeys.key1,
          serial = PositiveInt.one,
        )
        checkTransaction(checks, mds1) shouldBe Left(
          TopologyTransactionRejection.RequiredMapping.InsufficientKeys(
            Seq(mediatorId)
          )
        )

      }

      "report MediatorsAlreadyAssignedToGroups for duplicate mediator assignments" in {
        val (checks, store) = mk()
        val (Seq(med1, med2, med3), transactions) = generateMemberIdentities(3, MediatorId(_))

        val Seq(group0, group1, group2) = mkMediatorGroups(
          PositiveInt.one,
          NonNegativeInt.zero -> Seq(med1),
          NonNegativeInt.one -> Seq(med2),
          NonNegativeInt.two -> Seq(med1, med2, med3),
        )

        addToStore(store, (transactions :+ group0 :+ group1)*)

        checkTransaction(checks, group2, None) shouldBe Left(
          TopologyTransactionRejection.RequiredMapping.MediatorsAlreadyInOtherGroups(
            NonNegativeInt.two,
            Map(med1 -> NonNegativeInt.zero, med2 -> NonNegativeInt.one),
          )
        )
      }

      "report mediators defined both as active and observers" in {
        val (Seq(med1, med2), _transactions) = generateMemberIdentities(2, MediatorId(_))

        MediatorSynchronizerState
          .create(
            synchronizerId,
            NonNegativeInt.zero,
            PositiveInt.one,
            active = Seq(med1, med2),
            observers = Seq(med1),
          ) shouldBe Left(
          s"the following mediators were defined both as active and observer: $med1"
        )
      }

      "handle validation of proposal with a concurrent update of the store" in {
        val (checks, store) = mk()
        val (Seq(med1, med2), transactions) = generateMemberIdentities(2, MediatorId(_))

        val Seq(group0_add_med1) =
          mkMediatorGroups(PositiveInt.one, NonNegativeInt.zero -> Seq(med1))

        addToStore(store, (transactions :+ group0_add_med1)*)

        val Seq(group0_add_med2) = mkMediatorGroups(
          PositiveInt.two,
          NonNegativeInt.zero -> Seq(med1, med2),
        )

        // let's pretend that group0_add_med2 was broadcast by other synchronizerOwners
        // and became fully authorized (not necessarily effective yet though) and stored
        // between determining the previous effective transaction (group0_add_med1) at the start of
        // the processing of group0_add_med2 and validating group0_add_med2
        store
          .update(
            SequencedTime(ts1),
            EffectiveTime(ts1),
            removals = Map(
              group0_add_med1.mapping.uniqueKey -> (Some(PositiveInt.one), Set.empty)
            ),
            additions = Seq(
              ValidatedTopologyTransaction(group0_add_med2)
            ),
          )
          .futureValueUS
        checkTransaction(checks, group0_add_med2, Some(group0_add_med1)) shouldBe Right(())
      }
    }

    "validating SequencerSynchronizerState" should {

      "report no errors for valid mappings" in {
        val (checks, store) = mk()
        val (Seq(seq1, seq2), transactions) = generateMemberIdentities(2, SequencerId(_))
        addToStore(store, transactions*)

        val sds1 = makeSynchronizerState(PositiveInt.one, seq1)
        val sds2 = makeSynchronizerState(PositiveInt.two, seq1, seq2)

        checkTransaction(checks, sds1) shouldBe Either.unit
        checkTransaction(checks, sds2, Some(sds1)) shouldBe Either.unit
      }

      "bail if sequencer has no keys" in {
        val (checks, store) = mk()
        val sss = factory.mkAdd(
          SequencerSynchronizerState
            .create(
              synchronizerId,
              PositiveInt.one,
              active = Seq(sequencerId),
              Seq.empty,
            )
            .value,
          // the signing key is not relevant for the test
          signingKey = factory.SigningKeys.key1,
          serial = PositiveInt.one,
        )
        checkTransaction(checks, sss) shouldBe Left(
          TopologyTransactionRejection.RequiredMapping.InsufficientKeys(
            Seq(sequencerId)
          )
        )

      }

      "report sequencers defined both as active and observers" in {
        val (Seq(seq1, seq2), _transactions) = generateMemberIdentities(2, SequencerId(_))

        SequencerSynchronizerState
          .create(
            synchronizerId,
            PositiveInt.one,
            active = Seq(seq1, seq2),
            observers = Seq(seq1),
          ) shouldBe Left(
          s"the following sequencers were defined both as active and observer: $seq1"
        )
      }

      "handle validation of a proposal with a concurrent update in the store" in {
        val (checks, store) = mk()
        val (Seq(seq1, seq2), transactions) = generateMemberIdentities(2, SequencerId(_))

        val sds_add_seq1 = makeSynchronizerState(PositiveInt.one, seq1)

        addToStore(store, (transactions :+ sds_add_seq1)*)

        val sds_add_seq2 = makeSynchronizerState(PositiveInt.two, seq1, seq2)

        // let's pretend that sds_add_seq2 was broadcast by other synchronizerOwners
        // and became fully authorized (not necessarily effective yet though) and stored
        // between determining the previous effective transaction (sds_add_seq1) at the start of
        // the processing of sds_add_seq2 and validating sds_add_seq2.
        store
          .update(
            SequencedTime(ts1),
            EffectiveTime(ts1),
            removals = Map(
              sds_add_seq1.mapping.uniqueKey -> (Some(PositiveInt.one), Set.empty)
            ),
            additions = Seq(
              ValidatedTopologyTransaction(sds_add_seq2)
            ),
          )
          .futureValueUS
        checkTransaction(checks, sds_add_seq2, Some(sds_add_seq1)) shouldBe Right(())
      }

    }

    "validating OwnerToKeyMapping" should {
      "report no errors for valid mappings" in {
        val (checks, _) = mk()
        val okm_sequencer = factory.mkAddMultiKey(
          OwnerToKeyMapping.tryCreate(sequencerId, NonEmpty(Seq, factory.SigningKeys.key1)),
          NonEmpty(Set, factory.SigningKeys.key1),
        )
        val okm_mediator = factory.mkAddMultiKey(
          OwnerToKeyMapping.tryCreate(mediatorId, NonEmpty(Seq, factory.SigningKeys.key1)),
          NonEmpty(Set, factory.SigningKeys.key1),
        )
        val okm_participant = factory.mkAddMultiKey(
          OwnerToKeyMapping.tryCreate(
            participant1,
            NonEmpty(Seq, factory.EncryptionKeys.key1, factory.SigningKeys.key1),
          ),
          NonEmpty(Set, factory.SigningKeys.key1),
        )

        checkTransaction(checks, okm_sequencer) shouldBe Either.unit
        checkTransaction(checks, okm_mediator) shouldBe Either.unit
        checkTransaction(checks, okm_participant) shouldBe Either.unit
      }
      "reject minimum key violations" in {
        val (checks, _) = mk()
        val okm_sequencerNoSigningKey = factory.mkAddMultiKey(
          OwnerToKeyMapping.tryCreate(sequencerId, NonEmpty(Seq, factory.EncryptionKeys.key1)),
          NonEmpty(Set, factory.SigningKeys.key1),
        )
        val okm_mediatorNoSigningKey = factory.mkAddMultiKey(
          OwnerToKeyMapping.tryCreate(mediatorId, NonEmpty(Seq, factory.EncryptionKeys.key1)),
          NonEmpty(Set, factory.SigningKeys.key1),
        )
        val okm_participantNoSigningKey = factory.mkAddMultiKey(
          OwnerToKeyMapping.tryCreate(participant1, NonEmpty(Seq, factory.EncryptionKeys.key1)),
          NonEmpty(Set, factory.SigningKeys.key1),
        )
        val okm_participantNoEncryptionKey = factory.mkAddMultiKey(
          OwnerToKeyMapping.tryCreate(participant1, NonEmpty(Seq, factory.SigningKeys.key1)),
          NonEmpty(Set, factory.SigningKeys.key1),
        )

        Seq(okm_sequencerNoSigningKey, okm_mediatorNoSigningKey, okm_participantNoSigningKey)
          .foreach(tx =>
            checkTransaction(checks, tx) shouldBe Left(
              TopologyTransactionRejection.RequiredMapping.InvalidOwnerToKeyMapping(
                tx.mapping.member,
                "signing",
                Seq.empty,
                Seq("EC-Curve25519"),
              )
            )
          )
        checkTransaction(checks, okm_participantNoEncryptionKey) shouldBe Left(
          TopologyTransactionRejection.RequiredMapping.InvalidOwnerToKeyMapping(
            participant1,
            "encryption",
            Seq.empty,
            Seq("EC-P256"),
          )
        )
      }
      "reject re-adding of keys" in {
        val (checks, _) = mk()

        def mkAdd(serial: PositiveInt) = factory.mkAddMultiKey(
          OwnerToKeyMapping.tryCreate(sequencerId, NonEmpty(Seq, factory.SigningKeys.key1)),
          NonEmpty(Set, factory.SigningKeys.key1),
          serial = serial,
        )

        val okm1 = mkAdd(PositiveInt.one)
        val okm2 = factory.mkRemoveTx(okm1)
        val okm3 = mkAdd(PositiveInt.three)
        checkTransaction(checks, okm3, inStore = Some(okm2)) shouldBe Left(
          TopologyTransactionRejection.RequiredMapping.CannotReregisterKeys(sequencerId)
        )
      }
      "removals" should {
        "fail if participant is active" in {
          val (checks, store) = mk()

          addToStore(store, p1_otk, p1_dtc)

          checkTransaction(
            checks,
            factory.mkRemoveTx(p1_otk),
            inStore = Some(p1_otk),
          ) shouldBe Left(
            TopologyTransactionRejection.RequiredMapping.InvalidOwnerToKeyMappingRemoval(
              participant1,
              p1_dtc.transaction,
            )
          )

        }

        "fail if mediator is active" in {
          val (checks, store) = mk()

          val mg =
            mkMediatorGroups(PositiveInt.one, (NonNegativeInt.one, Seq(mediatorId))).loneElement
          addToStore(store, med_okm_k3, mg)

          checkTransaction(
            checks,
            factory.mkRemoveTx(med_okm_k3),
            inStore = Some(med_okm_k3),
          ) shouldBe Left(
            TopologyTransactionRejection.RequiredMapping.InvalidOwnerToKeyMappingRemoval(
              mediatorId,
              mg.transaction,
            )
          )
        }

        "fail if sequencer is active" in {
          val (checks, store) = mk()

          val grp = makeSynchronizerState(PositiveInt.one, sequencerId)
          addToStore(store, seq_okm_k2, grp)

          checkTransaction(
            checks,
            factory.mkRemoveTx(seq_okm_k2),
            inStore = Some(seq_okm_k2),
          ) shouldBe Left(
            TopologyTransactionRejection.RequiredMapping.InvalidOwnerToKeyMappingRemoval(
              sequencerId,
              grp.transaction,
            )
          )
        }

      }
    }
  }

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
            CanSignAllMappings,
          ),
          signingKey = key,
        )
      }.unzip
    val keysNE = NonEmpty.from(keys).value
    (keysNE, namespaces, rootCerts)
  }

}
