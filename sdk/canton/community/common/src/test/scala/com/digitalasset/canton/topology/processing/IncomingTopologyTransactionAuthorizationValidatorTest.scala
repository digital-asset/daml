// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import cats.Apply
import cats.instances.list.*
import cats.syntax.foldable.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.SignatureCheckError.InvalidSignature
import com.digitalasset.canton.crypto.{Signature, SigningPublicKey}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.TopologyTransactionRejection.{
  NoDelegationFoundForKeys,
  NotAuthorized,
}
import com.digitalasset.canton.topology.store.*
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyMapping.MappingHash
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.{BaseTest, HasExecutionContext, ProtocolVersionChecksAsyncWordSpec}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

class IncomingTopologyTransactionAuthorizationValidatorTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with ProtocolVersionChecksAsyncWordSpec {

  "topology transaction authorization" when {

    object Factory extends TopologyTransactionTestFactory(loggerFactory, parallelExecutionContext)

    def ts(seconds: Long) = CantonTimestamp.Epoch.plusSeconds(seconds)

    def mk(
        store: InMemoryTopologyStore[TopologyStoreId] =
          new InMemoryTopologyStore(DomainStore(Factory.domainId1), loggerFactory, timeouts),
        validationIsFinal: Boolean = true,
    ) = {
      val validator =
        new IncomingTopologyTransactionAuthorizationValidator(
          Factory.cryptoApi.crypto.pureCrypto,
          store,
          Some(Factory.domainId1),
          validationIsFinal = validationIsFinal,
          loggerFactory,
        )
      validator
    }

    def check(
        validated: Seq[ValidatedTopologyTransaction[TopologyChangeOp, TopologyMapping]],
        expectedOutcome: Seq[Option[TopologyTransactionRejection => Boolean]],
    ) = {
      validated should have length (expectedOutcome.size.toLong)
      validated.zipWithIndex.zip(expectedOutcome).foreach {
        case ((ValidatedTopologyTransaction(tx, Some(err), _), _), Some(expected)) =>
          assert(expected(err), s"Error $err was not expected for transaction: $tx")
        case ((ValidatedTopologyTransaction(transaction, rej, _), idx), expected) =>
          assertResult(expected, s"idx=$idx $transaction")(rej)
      }
      succeed
    }

    def validate(
        validator: IncomingTopologyTransactionAuthorizationValidator,
        timestamp: CantonTimestamp,
        toValidate: Seq[GenericSignedTopologyTransaction],
        inStore: Map[MappingHash, GenericSignedTopologyTransaction],
        expectFullAuthorization: Boolean,
    )(implicit traceContext: TraceContext) = {
      MonadUtil
        .sequentialTraverse(toValidate)(tx =>
          validator.validateAndUpdateHeadAuthState(
            timestamp,
            tx,
            inStore.get(tx.mapping.uniqueKey),
            expectFullAuthorization,
          )
        )
        .map { results =>
          val (aggregations, transactions) = results.unzip
          (aggregations.combineAll, transactions)
        }
    }

    "receiving transactions with signatures" should {
      "succeed to add if the signature is valid" in {
        val validator = mk()
        import Factory.*
        for {
          res <- validate(
            validator,
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
          (_, validatedTopologyTransactions) <- validate(
            validator,
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

      "fail to add if the OwnerToKeyMapping misses the signature for newly added signing keys" in {
        val validator = mk()
        import Factory.*

        val okmS1k7_k1_missing_k7 =
          okmS1k7_k1.removeSignatures(Set(SigningKeys.key7.fingerprint)).value
        for {
          (_, validatedTopologyTransactions) <- validate(
            validator,
            ts(0),
            List(ns1k1_k1, okmS1k7_k1_missing_k7),
            Map.empty,
            expectFullAuthorization = true,
          )
        } yield {
          check(
            validatedTopologyTransactions,
            Seq(
              None,
              Some(_ == NotAuthorized),
            ),
          )
        }
      }

      "reject if the transaction is for the wrong domain" in {
        val validator = mk()
        import Factory.*
        val wrongDomain = DomainId(UniqueIdentifier.tryCreate("wrong", ns1.fingerprint.unwrap))
        val pid = ParticipantId(UniqueIdentifier.tryCreate("correct", ns1.fingerprint.unwrap))
        val wrong = mkAdd(
          DomainTrustCertificate(
            pid,
            wrongDomain,
            false,
            Seq.empty,
          ),
          Factory.SigningKeys.key1,
        )
        for {
          res <- validate(
            validator,
            ts(0),
            List(ns1k1_k1, wrong),
            Map.empty,
            expectFullAuthorization = false,
          )
        } yield {
          check(
            res._2,
            Seq(
              None,
              Some({
                case TopologyTransactionRejection.WrongDomain(_) => true
                case _ => false
              }),
            ),
          )
        }
      }

      // testing an inconsistent topology store with multiple DNDs effective at the same time
      "be able to handle multiple decentralized namespace transactions for the same namespace being erroneously effective" in {
        import Factory.*
        import SigningKeys.{ec as _, *}
        val store =
          new InMemoryTopologyStore(TopologyStoreId.AuthorizedStore, loggerFactory, timeouts)
        val validator = mk(store)

        val namespace = DecentralizedNamespaceDefinition.computeNamespace(Set(ns1))

        val dnd1 = mkAddMultiKey(
          DecentralizedNamespaceDefinition
            .create(
              namespace,
              PositiveInt.one,
              owners = NonEmpty(Set, ns1),
            )
            .value,
          serial = PositiveInt.one,
          signingKeys = NonEmpty(Set, key1),
        )

        val dnd2 = mkAddMultiKey(
          DecentralizedNamespaceDefinition
            .create(
              namespace,
              PositiveInt.one,
              owners = NonEmpty(Set, ns1, ns2),
            )
            .value,
          serial = PositiveInt.one,
          signingKeys = NonEmpty(Set, key1, key2),
        )

        // we intentionally bootstrap with 2 transactions for the same mapping unique key being effective at the same time,
        // so that we can test that authorization validator can handle such faulty situations and not just break
        val bootstrapTransactions = Seq(ns1k1_k1, ns2k2_k2, dnd1, dnd2).map(
          StoredTopologyTransaction(SequencedTime.MinValue, EffectiveTime.MinValue, None, _)
        )

        val dnd3 = mkAddMultiKey(
          DecentralizedNamespaceDefinition
            .create(
              namespace,
              PositiveInt.two,
              owners = NonEmpty(Set, ns1, ns2),
            )
            .value,
          serial = PositiveInt.one,
          signingKeys = NonEmpty(Set, key1, key2),
        )

        for {
          _ <- store.bootstrap(StoredTopologyTransactions(bootstrapTransactions))
          result <- validate(
            validator,
            ts(1),
            Seq(dnd3),
            Map(dnd2.mapping.uniqueKey -> dnd2),
            expectFullAuthorization = false,
          )
        } yield {
          result._2.loneElement.rejectionReason shouldBe None
        }
      }
    }

    "observing namespace delegations" should {
      "succeed if transaction is properly authorized" in {
        val validator = mk()
        import Factory.*
        for {
          res <- validate(
            validator,
            ts(0),
            List(ns1k1_k1, ns1k2_k1, ns1k3_k2),
            Map.empty,
            expectFullAuthorization = true,
          )
        } yield {
          check(res._2, Seq(None, None, None))
        }
      }
      "fail if the signature of a root certificate is not valid" in {
        val validator = mk()
        import Factory.*

        val sig_k1_emptySignature = Signature
          .fromProtoV30(ns1k1_k1.signatures.head1.toProtoV30.copy(signature = ByteString.empty()))
          .value
        val ns1k1_k1WithEmptySignature =
          ns1k1_k1.copy(signatures = NonEmpty(Set, sig_k1_emptySignature))

        for {
          res <- validate(
            validator,
            ts(0),
            List(ns1k1_k1WithEmptySignature, ns1k2_k1),
            Map.empty,
            expectFullAuthorization = true,
          )
        } yield {
          check(
            res._2,
            Seq(
              Some({
                case TopologyTransactionRejection.SignatureCheckFailed(
                      InvalidSignature(`sig_k1_emptySignature`, _, _)
                    ) =>
                  true
                case _ => false
              }),
              Some(_ == NoDelegationFoundForKeys(Set(SigningKeys.key1.fingerprint))),
            ),
          )
        }
      }
      "fail if transaction is not properly authorized" in {
        val validator = mk()
        import Factory.*
        for {
          res <- validate(
            validator,
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
          new InMemoryTopologyStore(TopologyStoreId.AuthorizedStore, loggerFactory, timeouts)
        val validator = mk(store)
        import Factory.*
        for {
          _ <- store.update(
            SequencedTime(ts(0)),
            EffectiveTime(ts(0)),
            removeMapping = Map.empty,
            removeTxs = Set.empty,
            additions = List(ns1k1_k1).map(ValidatedTopologyTransaction(_)),
          )
          res <- validate(
            validator,
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
          res <- validate(
            validator,
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
          res <- validate(
            validator,
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
          res <- validate(
            validator,
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
          res <- validate(
            validator,
            ts(0),
            List(ns1k1_k1, ns1k2_k1, okm1ak5k1E_k2, p1p1B_k2, id1ak4_k1, ns6k6_k6, p1p6_k2k6),
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
          resultExpectFullAuthorization <- validate(
            validator,
            ts(0),
            List(ns1k1_k1, okm1ak5k1E_k2, p1p1B_k2),
            Map.empty,
            expectFullAuthorization = true,
          )
          // also check that insufficiently authorized non-proposals get rejected with expectFullAuthorization
          resultDontExpectFullAuthorization <- validate(
            validator,
            ts(0),
            List(ns1k1_k1, okm1ak5k1E_k2, p1p1B_k2),
            Map.empty,
            expectFullAuthorization = false,
          )

        } yield {
          check(
            resultExpectFullAuthorization._2,
            Seq(
              None,
              Some(_ == NotAuthorized),
              Some(_ == NoDelegationFoundForKeys(Set(SigningKeys.key2.fingerprint))),
            ),
          )

          check(
            resultDontExpectFullAuthorization._2,
            Seq(
              None,
              Some(_ == NotAuthorized),
              Some(_ == NoDelegationFoundForKeys(Set(SigningKeys.key2.fingerprint))),
            ),
          )

        }
      }
      "succeed with loading existing identifier delegations" in {
        val store: InMemoryTopologyStore[TopologyStoreId.AuthorizedStore] =
          new InMemoryTopologyStore(TopologyStoreId.AuthorizedStore, loggerFactory, timeouts)
        val validator = mk(store)
        import Factory.*
        for {
          _ <- store.update(
            SequencedTime(ts(0)),
            EffectiveTime(ts(0)),
            removeMapping = Map.empty,
            removeTxs = Set.empty,
            additions = List(ns1k1_k1, ns6k6_k6, id1ak4_k1).map(ValidatedTopologyTransaction(_)),
          )
          res <- validate(
            validator,
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
          res <- validate(
            validator,
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
          res <- validate(
            validator,
            ts(0),
            List(ns1k1_k1, ns1k2_k1, id1ak4_k1, Rns1k2_k1, Rid1ak4_k1, okm1ak5k1E_k2, p1p6_k2),
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
              Some(_ == NotAuthorized),
              Some(_ == NoDelegationFoundForKeys(Set(SigningKeys.key2.fingerprint))),
            ),
          )
        }
      }

    }

    "correctly determine cascading update for" should {
      "namespace additions" in {
        val store =
          new InMemoryTopologyStore(TopologyStoreId.AuthorizedStore, loggerFactory, timeouts)
        val validator = mk(store)
        import Factory.*
        for {
          _ <- store.update(
            SequencedTime(ts(0)),
            EffectiveTime(ts(0)),
            removeMapping = Map.empty,
            removeTxs = Set.empty,
            additions = List(ns6k6_k6).map(ValidatedTopologyTransaction(_)),
          )
          res <- validate(
            validator,
            ts(1),
            List(ns1k1_k1, okm1bk5k1E_k1, p1p6_k6),
            Map.empty,
            expectFullAuthorization = true,
          )
        } yield {
          res._1.cascadingNamespaces shouldBe Set(ns1)
        }
      }

      "namespace removals" in {
        val store =
          new InMemoryTopologyStore(TopologyStoreId.AuthorizedStore, loggerFactory, timeouts)
        val validator = mk(store)
        import Factory.*
        val Rns1k1_k1 = mkTrans(ns1k1_k1.transaction.reverse)
        for {
          _ <- store.update(
            SequencedTime(ts(0)),
            EffectiveTime(ts(0)),
            removeMapping = Map.empty,
            removeTxs = Set.empty,
            additions = List(ns1k1_k1).map(ValidatedTopologyTransaction(_)),
          )
          res <- validate(
            validator,
            ts(1),
            List(Rns1k1_k1, okm1bk5k1E_k1),
            Map(Rns1k1_k1.mapping.uniqueKey -> ns1k1_k1),
            expectFullAuthorization = true,
          )
        } yield {
          check(
            res._2,
            Seq(None, Some(_ == NotAuthorized)),
          )
          res._1.cascadingNamespaces shouldBe Set(ns1)
        }
      }

      "identifier additions and removals" in {
        val store =
          new InMemoryTopologyStore(TopologyStoreId.AuthorizedStore, loggerFactory, timeouts)
        val validator = mk(store)
        import Factory.*
        val Rid1ak4_k1 = mkTrans(id1ak4_k1.transaction.reverse)
        for {
          _ <- store.update(
            SequencedTime(ts(0)),
            EffectiveTime(ts(0)),
            removeMapping = Map.empty,
            removeTxs = Set.empty,
            additions = List(ns1k1_k1).map(ValidatedTopologyTransaction(_)),
          )
          res <- validate(
            validator,
            ts(1),
            List(id1ak4_k1),
            Map.empty,
            expectFullAuthorization = true,
          )
          res2 <- validate(
            validator,
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
          new InMemoryTopologyStore(TopologyStoreId.AuthorizedStore, loggerFactory, timeouts)
        val validator = mk(store)
        import Factory.*
        import Factory.SigningKeys.{ec as _, *}
        // scenario: we have id1ak4_k2 previously loaded. now we get a removal on k2. we need to ensure that
        // nothing can be added by k4
        val Rns1k2_k1 = mkTrans(ns1k2_k1.transaction.reverse)
        val id6ak7_k6 = mkAdd(IdentifierDelegation(uid6, key7), key6)
        for {
          _ <- store.update(
            SequencedTime(ts(0)),
            EffectiveTime(ts(0)),
            removeMapping = Map.empty,
            removeTxs = Set.empty,
            additions =
              List(ns1k1_k1, ns1k2_k1, id1ak4_k2, ns6k6_k6).map(ValidatedTopologyTransaction(_)),
          )
          res <- validate(
            validator,
            ts(1),
            List(p1p6_k2k6, Rns1k2_k1, id6ak7_k6, p1p6_k2),
            Map(
              ns1k2_k1.mapping.uniqueKey -> ns1k2_k1
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

    "observing PartyToParticipant mappings" should {
      "allow participants to unilaterally disassociate themselves from parties" in {
        val store =
          new InMemoryTopologyStore(TopologyStoreId.AuthorizedStore, loggerFactory, timeouts)
        val validator = mk(store)
        import Factory.*

        val pid2 = ParticipantId(UniqueIdentifier.tryCreate("participant2", ns2))
        val participants_1_2_6_HostParty1 = mkAddMultiKey(
          PartyToParticipant.tryCreate(
            party1b, // lives in the namespace of p1, corresponding to `SigningKeys.key1`
            None,
            threshold = PositiveInt.two,
            Seq(
              HostingParticipant(participant1, ParticipantPermission.Submission),
              HostingParticipant(pid2, ParticipantPermission.Submission),
              HostingParticipant(participant6, ParticipantPermission.Submission),
            ),
            groupAddressing = false,
          ),
          // both the party's owner and the participant sign
          NonEmpty(Set, SigningKeys.key1, SigningKeys.key2, SigningKeys.key6),
          serial = PositiveInt.one,
        )

        val unhostingMapping = PartyToParticipant.tryCreate(
          party1b,
          None,
          threshold = PositiveInt.two,
          Seq(
            HostingParticipant(participant1, ParticipantPermission.Submission),
            HostingParticipant(participant6, ParticipantPermission.Submission),
          ),
          groupAddressing = false,
        )
        val unhostingMappingAndThresholdChange = PartyToParticipant.tryCreate(
          party1b,
          None,
          threshold = PositiveInt.one,
          Seq(
            HostingParticipant(participant1, ParticipantPermission.Submission),
            HostingParticipant(participant6, ParticipantPermission.Submission),
          ),
          groupAddressing = false,
        )

        val participant2RemovesItselfUnilaterally = mkAdd(
          unhostingMapping,
          // only the unhosting participant signs
          SigningKeys.key2,
          serial = PositiveInt.two,
        )

        val participant2RemovedFullyAuthorized = mkAddMultiKey(
          unhostingMapping,
          // both the unhosting participant as well as the party's owner signs
          NonEmpty(Set, SigningKeys.key1, SigningKeys.key2),
          serial = PositiveInt.two,
        )

        val ptpMappingHash = participants_1_2_6_HostParty1.mapping.uniqueKey
        for {
          _ <- store.update(
            SequencedTime(ts(0)),
            EffectiveTime(ts(0)),
            removeMapping = Map.empty,
            removeTxs = Set.empty,
            additions = List(ns1k1_k1, ns2k2_k2, ns6k6_k6).map(
              ValidatedTopologyTransaction(_)
            ),
          )
          hostingResult <- validate(
            validator,
            ts(1),
            List(participants_1_2_6_HostParty1),
            inStore = Map.empty,
            expectFullAuthorization = false,
          )

          // unilateral unhosting by participant2 only signed by the participant
          unhostingResult <- validate(
            validator,
            ts(2),
            List(participant2RemovesItselfUnilaterally),
            inStore = Map(ptpMappingHash -> participants_1_2_6_HostParty1),
            expectFullAuthorization = false,
          )

          // it is still allowed to have a mix of signatures for unhosting
          unhostingMixedResult <- validate(
            validator,
            ts(2),
            List(participant2RemovedFullyAuthorized),
            inStore = Map(ptpMappingHash -> participants_1_2_6_HostParty1),
            expectFullAuthorization = false,
          )

          // the participant being removed may not sign if anything else changes
          unhostingAndThresholdChangeResult <- validate(
            validator,
            ts(2),
            List(
              mkAddMultiKey(
                unhostingMappingAndThresholdChange,
                NonEmpty(Set, SigningKeys.key2),
              )
            ),
            inStore = Map(ptpMappingHash -> participants_1_2_6_HostParty1),
            expectFullAuthorization = false,
          )
        } yield {
          check(hostingResult._2, Seq(None))
          check(unhostingResult._2, Seq(None))
          check(unhostingMixedResult._2, Seq(None))
          check(
            unhostingAndThresholdChangeResult._2,
            Seq(Some(_ == NoDelegationFoundForKeys(Set(SigningKeys.key2.fingerprint)))),
          )
        }
      }
    }

    "evolving decentralized namespace definitions with threshold > 1" should {
      "succeed if proposing lower threshold and number of owners" in {
        val store =
          new InMemoryTopologyStore(TopologyStoreId.AuthorizedStore, loggerFactory, timeouts)
        val validator = mk(store)
        import Factory.*
        for {
          _ <- store.update(
            SequencedTime(ts(0)),
            EffectiveTime(ts(0)),
            removeMapping = Map.empty,
            removeTxs = Set.empty,
            additions = decentralizedNamespaceWithMultipleOwnerThreshold.map(
              ValidatedTopologyTransaction(_)
            ),
          )
          res <- validate(
            validator,
            ts(1),
            List(dns2),
            decentralizedNamespaceWithMultipleOwnerThreshold
              .map(tx => tx.mapping.uniqueKey -> tx)
              .toMap,
            expectFullAuthorization = false,
          )
        } yield {
          check(res._2, Seq(None))
        }
      }

      "succeed in authorizing with quorum of owner signatures" in {
        val store =
          new InMemoryTopologyStore(TopologyStoreId.AuthorizedStore, loggerFactory, timeouts)
        val validator = mk(store)
        import Factory.*
        val proposeDecentralizedNamespaceWithLowerThresholdAndOwnerNumber = List(dns2)
        for {
          _ <- store.update(
            SequencedTime(ts(0)),
            EffectiveTime(ts(0)),
            removeMapping = Map.empty,
            removeTxs = Set.empty,
            additions = decentralizedNamespaceWithMultipleOwnerThreshold.map(
              ValidatedTopologyTransaction(_)
            ),
          )
          _ <- store.update(
            SequencedTime(ts(1)),
            EffectiveTime(ts(1)),
            removeMapping = Map.empty,
            removeTxs = Set.empty,
            additions = proposeDecentralizedNamespaceWithLowerThresholdAndOwnerNumber.map(
              ValidatedTopologyTransaction(_)
            ),
          )
          res <- validate(
            validator,
            ts(2),
            // Analogously to how the TopologyStateProcessor merges the signatures of proposals
            // with the same serial, combine the signature of the previous proposal to the current proposal.
            List(dns3.addSignatures(dns2.signatures.toSeq)),
            (decentralizedNamespaceWithMultipleOwnerThreshold ++ proposeDecentralizedNamespaceWithLowerThresholdAndOwnerNumber)
              .map(tx => tx.mapping.uniqueKey -> tx)
              .toMap,
            // Expect to be able to authorize now that we have two signatures as required by
            // decentralizedNamespaceWithMultipleOwnerThreshold (dns1).
            expectFullAuthorization = true,
          )
        } yield {
          check(res._2, Seq(None))
        }
      }

      "remove from cache for TopologyChangeOp.REMOVAL" in {
        val store =
          new InMemoryTopologyStore(TopologyStoreId.AuthorizedStore, loggerFactory, timeouts)
        val validator = mk(store)
        import Factory.*
        for {
          // 1. validate and store the decentralized namespace owners root certificates
          resultAddOwners <- validate(
            validator,
            ts(0),
            decentralizedNamespaceOwners,
            Map.empty,
            expectFullAuthorization = true,
          )
          _ = resultAddOwners._2.foreach(_.rejectionReason shouldBe None)
          _ <- store.update(
            SequencedTime(ts(0)),
            EffectiveTime(ts(0)),
            removeMapping = Map.empty,
            removeTxs = Set.empty,
            additions = resultAddOwners._2,
          )

          // 2. validate and store the decentralized namespace definition
          // this puts the DND authorization graph into the cache
          resultAddDND <- validate(
            validator,
            ts(1),
            List(dns1),
            Map.empty,
            expectFullAuthorization = true,
          )
          _ = resultAddDND._2.foreach(_.rejectionReason shouldBe None)
          _ <- store.update(
            SequencedTime(ts(1)),
            EffectiveTime(ts(1)),
            removeMapping = Map.empty,
            removeTxs = Set.empty,
            additions = resultAddDND._2,
          )

          // 3. now process the removal of the decentralized namespace definition
          // this should remove the DND authorization graph from the cache
          resRemoveDND <- validate(
            validator,
            ts(2),
            List(dns1Removal),
            Map(dns1.mapping.uniqueKey -> dns1),
            expectFullAuthorization = true,
          )
          _ = resRemoveDND._2.foreach(_.rejectionReason shouldBe None)
          _ <- store.update(
            SequencedTime(ts(2)),
            EffectiveTime(ts(2)),
            removeMapping = Map(dns1Removal.mapping.uniqueKey -> dns1Removal.serial),
            removeTxs = Set.empty,
            additions = resRemoveDND._2,
          )

          // 4. Now to the actual test: try to authorize something for the decentralized namespace.
          // this should be rejected because the namespace is not valid anymore, and the
          // authorization cache has been properly cleaned up.
          resultUnauthorizedIDD <- validate(
            validator,
            ts(3),
            List(dns1Idd),
            Map.empty,
            expectFullAuthorization = true,
          )
        } yield {
          check(
            resultUnauthorizedIDD._2,
            Seq(
              Some(
                _ == NoDelegationFoundForKeys(
                  Set(SigningKeys.key1, SigningKeys.key8, SigningKeys.key9).map(_.fingerprint)
                )
              )
            ),
          )
        }

      }
    }

    def checkProposalFlagAfterValidation(validationIsFinal: Boolean, expectProposal: Boolean) = {
      val store =
        new InMemoryTopologyStore(TopologyStoreId.AuthorizedStore, loggerFactory, timeouts)
      val validator = mk(store, validationIsFinal)
      import Factory.*
      import SigningKeys.{ec as _, *}

      val dns_id = DecentralizedNamespaceDefinition.computeNamespace(Set(ns1, ns8))
      val dns_2_owners = mkAddMultiKey(
        DecentralizedNamespaceDefinition
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
          removeMapping = Map.empty,
          removeTxs = Set.empty,
          additions = decentralizedNamespaceWithThreeOwners.map(
            ValidatedTopologyTransaction(_)
          ),
        )

        pkgTx = TopologyTransaction(
          TopologyChangeOp.Replace,
          serial = PositiveInt.one,
          VettedPackages(
            ParticipantId(UniqueIdentifier.tryCreate("consortium-participiant", dns_id)),
            None,
            Seq.empty,
          ),
          BaseTest.testedProtocolVersion,
        )
        result_packageVetting <- validate(
          validator,
          ts(1),
          toValidate = List(
            // Setting isProposal=true despite having enough keys.
            // This simulates processing a proposal with the signature of a node,
            // that got merged with another proposal already in the store.
            mkTrans(pkgTx, signingKeys = NonEmpty(Set, key1, key8), isProposal = true)
          ),
          inStore = Map.empty,
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
      checkProposalFlagAfterValidation(validationIsFinal = true, expectProposal = false)
    }

    "not change the proposal status when the validation is not final" in {
      checkProposalFlagAfterValidation(validationIsFinal = false, expectProposal = true)
    }

    "remove superfluous signatures" in {
      val store =
        new InMemoryTopologyStore(TopologyStoreId.AuthorizedStore, loggerFactory, timeouts)
      val validator = mk(store)
      import Factory.*
      import SigningKeys.{ec as _, *}

      val dns_id = DecentralizedNamespaceDefinition.computeNamespace(Set(ns1, ns8))
      val dnsTwoOwners = mkAddMultiKey(
        DecentralizedNamespaceDefinition
          .create(dns_id, PositiveInt.two, NonEmpty(Set, ns1, ns8))
          .value,
        NonEmpty(Set, key1, key8),
        serial = PositiveInt.one,
      )
      val decentralizedNamespaceWithTwoOwners = List(ns1k1_k1, ns8k8_k8, dnsTwoOwners)

      for {
        _ <- store.update(
          SequencedTime(ts(0)),
          EffectiveTime(ts(0)),
          removeMapping = Map.empty,
          removeTxs = Set.empty,
          additions = decentralizedNamespaceWithTwoOwners.map(
            ValidatedTopologyTransaction(_)
          ),
        )

        pkgTx = TopologyTransaction(
          TopologyChangeOp.Replace,
          serial = PositiveInt.one,
          VettedPackages(
            ParticipantId(UniqueIdentifier.tryCreate("consortium-participiant", dns_id)),
            None,
            Seq.empty,
          ),
          BaseTest.testedProtocolVersion,
        )
        resultPackageVetting <- validate(
          validator,
          ts(1),
          toValidate = List(
            // Signing this transaction also with key9 simulates that ns9 was part of the
            // decentralized namespace before and was eligible for signing the transaction.
            // After this validation, we expect the signature of key9 to be removed
            mkTrans(pkgTx, signingKeys = NonEmpty(Set, key9, key1, key8), isProposal = true)
          ),
          inStore = Map.empty,
          expectFullAuthorization = false,
        )

        // if there are only superfluous signatures, reject the transaction
        resultOnlySuperfluousSignatures <- validate(
          validator,
          ts(2),
          toValidate = List(
            mkTrans(pkgTx, signingKeys = NonEmpty(Set, key3, key5), isProposal = true)
          ),
          inStore = Map.empty,
          expectFullAuthorization = false,
        )

      } yield {
        val validatedPkgTx = resultPackageVetting._2.loneElement
        val signatures = validatedPkgTx.transaction.signatures

        validatedPkgTx.rejectionReason shouldBe None
        signatures.map(_.signedBy).forgetNE should contain theSameElementsAs (Set(key1, key8).map(
          _.id
        ))

        resultOnlySuperfluousSignatures._2.loneElement.rejectionReason shouldBe Some(
          TopologyTransactionRejection.NoDelegationFoundForKeys(Set(key3.id, key5.id))
        )
      }
    }

    "respect the threshold of decentralized namespaces" in {
      val store =
        new InMemoryTopologyStore(TopologyStoreId.AuthorizedStore, loggerFactory, timeouts)
      val validator = mk(store)
      import Factory.*
      import SigningKeys.{ec as _, *}

      val dns_id = DecentralizedNamespaceDefinition.computeNamespace(Set(ns1, ns8, ns9))
      val dns = mkAddMultiKey(
        DecentralizedNamespaceDefinition
          .create(dns_id, PositiveInt.tryCreate(3), NonEmpty(Set, ns1, ns8, ns9))
          .value,
        NonEmpty(Set, key1, key8, key9),
        serial = PositiveInt.one,
      )

      val decentralizedNamespaceWithThreeOwners = List(ns1k1_k1, ns8k8_k8, ns9k9_k9, dns)

      val pkgMapping = VettedPackages(
        ParticipantId(UniqueIdentifier.tryCreate("consortium-participiant", dns_id)),
        None,
        Seq.empty,
      )
      val pkgTx = TopologyTransaction(
        TopologyChangeOp.Replace,
        serial = PositiveInt.one,
        pkgMapping,
        BaseTest.testedProtocolVersion,
      )

      def validateTx(
          isProposal: Boolean,
          expectFullAuthorization: Boolean,
          signingKeys: SigningPublicKey*
      ) = TraceContext.withNewTraceContext { freshTraceContext =>
        validate(
          validator,
          ts(1),
          toValidate = List(
            mkTrans(
              pkgTx,
              isProposal = isProposal,
              signingKeys = NonEmpty.from(signingKeys.toSet).value,
            )
          ),
          inStore = Map.empty,
          expectFullAuthorization = expectFullAuthorization,
        )(freshTraceContext)
          .map(_._2.loneElement)
      }

      for {
        _ <- store.update(
          SequencedTime(ts(0)),
          EffectiveTime(ts(0)),
          removeMapping = Map.empty,
          removeTxs = Set.empty,
          additions = decentralizedNamespaceWithThreeOwners.map(
            ValidatedTopologyTransaction(_)
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
