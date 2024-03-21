// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.store.TopologyTransactionRejection.Duplicate
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.store.{
  TopologyStoreId,
  TopologyTransactionRejection,
  ValidatedTopologyTransaction,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  DomainId,
  ParticipantId,
  UniqueIdentifier,
}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AsyncWordSpec

class IncomingTopologyTransactionAuthorizationValidatorTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext {

  "topology transaction authorization" when {

    object Factory extends TopologyTransactionTestFactory(loggerFactory, parallelExecutionContext)

    def ts(seconds: Long) = CantonTimestamp.Epoch.plusSeconds(seconds)

    def mkStore(): InMemoryTopologyStore[TopologyStoreId] =
      new InMemoryTopologyStore(
        TopologyStoreId.AuthorizedStore,
        loggerFactory,
        timeouts,
        futureSupervisor,
      )

    def mk(store: InMemoryTopologyStore[TopologyStoreId] = mkStore()) = {
      val validator =
        new IncomingTopologyTransactionAuthorizationValidator(
          Factory.cryptoApi.pureCrypto,
          store,
          Some(DefaultTestIdentities.domainId),
          loggerFactory,
        )
      validator
    }

    def check(
        validated: Seq[ValidatedTopologyTransaction],
        outcome: Seq[Option[TopologyTransactionRejection => Boolean]],
    ) = {
      validated should have length (outcome.size.toLong)
      validated.zipWithIndex.zip(outcome).foreach {
        case ((ValidatedTopologyTransaction(transaction, Some(err)), idx), Some(expected)) =>
          assert(expected(err), (err, expected))
        case ((ValidatedTopologyTransaction(transaction, rej), idx), expected) =>
          assertResult(expected, s"idx=$idx $transaction")(rej)
      }
      assert(true)
    }

    val unauthorized =
      Some((err: TopologyTransactionRejection) => err == TopologyTransactionRejection.NotAuthorized)

    implicit val toValidated
        : SignedTopologyTransaction[TopologyChangeOp] => ValidatedTopologyTransaction =
      x => ValidatedTopologyTransaction(x, None)

    "receiving transactions with signatures" should {
      "succeed to add if the signature is valid" in {
        val validator = mk()
        import Factory.*
        for {
          res <- validator.validateAndUpdateHeadAuthState(ts(0), List(ns1k1_k1, ns1k2_k1))
        } yield {
          check(res._2, Seq(None, None))
        }
      }
      "fail to add if the signature is invalid" in {
        val validator = mk()
        import Factory.*
        val invalid = ns1k2_k1.update(signature = ns1k1_k1.signature)
        for {
          (_, validatedTopologyTransactions) <- validator.validateAndUpdateHeadAuthState(
            ts(0),
            List(ns1k1_k1, invalid),
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
      "reject if the transaction is for the wrong domain" in {
        val validator = mk()
        import Factory.*
        val wrongDomain = DomainId(UniqueIdentifier.tryCreate("wrong", ns1.fingerprint.unwrap))
        val pid = ParticipantId(UniqueIdentifier.tryCreate("correct", ns1.fingerprint.unwrap))
        val wrong = mkAdd(
          ParticipantState(
            RequestSide.Both,
            wrongDomain,
            pid,
            ParticipantPermission.Submission,
            TrustLevel.Ordinary,
          ),
          Factory.SigningKeys.key1,
        )
        for {
          res <- validator.validateAndUpdateHeadAuthState(ts(0), List(ns1k1_k1, wrong))
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
      "fail if the transaction has already been stored" in {
        val store = mkStore()
        val validator = mk(store)
        import Factory.*
        val tsN = ts(-5)
        for {
          // prefill store with existing transaction
          _ <- store.append(SequencedTime(tsN), EffectiveTime(tsN), List(ns1k1_k1))
          res <- validator.validateAndUpdateHeadAuthState(ts(0), List(ns1k1_k1, ns1k2_k1))
        } yield {
          check(
            res._2,
            Seq(
              // first one should be detected as duplicate
              Some {
                case Duplicate(_) =>
                  true
                case _ => false
              },
              None,
            ),
          )
        }
      }
    }

    "observing namespace delegations" should {
      "succeed if transaction is properly authorized" in {
        val validator = mk()
        import Factory.*
        for {
          res <- validator.validateAndUpdateHeadAuthState(ts(0), List(ns1k1_k1, ns1k2_k1, ns1k3_k2))
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
          )
        } yield {
          check(res._2, Seq(None, unauthorized, unauthorized, None, None))
        }
      }
      "succeed and use load existing delegations" in {
        val store = mkStore()
        val validator = mk(store)
        import Factory.*
        for {
          _ <- store.append(SequencedTime(ts(0)), EffectiveTime(ts(0)), List(ns1k1_k1))
          res <- validator.validateAndUpdateHeadAuthState(ts(1), List(ns1k2_k1, ns1k3_k2))
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
            List(ns1k1_k1, ns1k2_k1, okm1ak5_k2, p1p1B_k2, id1ak4_k1, ns6k6_k6, p1p2F_k2, p1p2T_k6),
          )
        } yield {
          check(res._2, Seq(None, None, None, None, None, None, None, None))
        }
      }
      "fail if transaction is not properly authorized" in {
        val validator = mk()
        import Factory.*
        for {
          res <- validator.validateAndUpdateHeadAuthState(
            ts(0),
            List(ns1k1_k1, okm1ak5_k2, p1p1B_k2),
          )
        } yield {
          check(res._2, Seq(None, unauthorized, unauthorized))
        }
      }
      "succeed with loading existing identifier delegations" in {
        val store = mkStore()
        val validator = mk(store)
        import Factory.*
        for {
          _ <- store.append(SequencedTime(ts(0)), EffectiveTime(ts(0)), List(ns1k1_k1, id1ak4_k1))
          res <- validator.validateAndUpdateHeadAuthState(ts(1), List(ns1k2_k1, p1p2F_k2, p1p1B_k2))
        } yield {
          check(res._2, Seq(None, None, None))
        }
      }
    }

    "observing removals" should {
      "accept authorized removals" in {
        val validator = mk()
        import Factory.*
        val Rns1k2_k1 = revert(ns1k2_k1)
        val Rid1ak4_k1 = revert(id1ak4_k1)
        for {
          res <- validator.validateAndUpdateHeadAuthState(
            ts(0),
            List(ns1k1_k1, ns1k2_k1, id1ak4_k1, Rns1k2_k1, Rid1ak4_k1),
          )
        } yield {
          check(res._2, Seq(None, None, None, None, None))
        }
      }

      "reject un-authorized after removal" in {
        val validator = mk()
        import Factory.*
        val Rns1k2_k1 = revert(ns1k2_k1)
        val Rid1ak4_k1 = revert(id1ak4_k1)
        for {
          res <- validator.validateAndUpdateHeadAuthState(
            ts(0),
            List(ns1k1_k1, ns1k2_k1, id1ak4_k1, Rns1k2_k1, Rid1ak4_k1, okm1ak5_k2, p1p2F_k2),
          )
        } yield {
          check(res._2, Seq(None, None, None, None, None, unauthorized, unauthorized))
        }
      }

    }

    "correctly determine cascading update for" should {
      "namespace additions" in {
        val store = mkStore()
        val validator = mk(store)
        import Factory.*
        for {
          _ <- store.append(SequencedTime(ts(0)), EffectiveTime(ts(0)), List(ns6k6_k6))
          res <- validator.validateAndUpdateHeadAuthState(
            ts(1),
            List(ns1k1_k1, okm1bk5_k1, p1p2T_k6),
          )
        } yield {
          res._1.cascadingNamespaces shouldBe Set(ns1)
        }
      }

      "namespace removals" in {
        val store = mkStore()
        val validator = mk(store)
        import Factory.*
        val Rns1k1_k1 = revert(ns1k1_k1)
        for {
          _ <- store.append(SequencedTime(ts(0)), EffectiveTime(ts(0)), List(ns1k1_k1))
          res <- validator.validateAndUpdateHeadAuthState(ts(1), List(Rns1k1_k1, okm1bk5_k1))
        } yield {
          check(res._2, Seq(None, unauthorized))
          res._1.cascadingNamespaces shouldBe Set(ns1)
        }
      }

      "identifier additions and removals" in {
        val store = mkStore()
        val validator = mk(store)
        import Factory.*
        val Rid1ak4_k1 = revert(id1ak4_k1)
        for {
          _ <- store.append(SequencedTime(ts(0)), EffectiveTime(ts(0)), List(ns1k1_k1))
          res <- validator.validateAndUpdateHeadAuthState(ts(1), List(id1ak4_k1))
          res2 <- validator.validateAndUpdateHeadAuthState(ts(2), List(Rid1ak4_k1))
        } yield {
          res._1.cascadingNamespaces shouldBe Set()
          res._1.cascadingUids shouldBe Set(uid1a)
          res2._1.cascadingUids shouldBe Set(uid1a)
        }
      }

      "cascading invalidation pre-existing identifier uids" in {
        val store = mkStore()
        val validator = mk(store)
        import Factory.*
        import Factory.SigningKeys.*
        // scenario: we have id1ak4_k2 previously loaded. now we get a removal on k2. we need to ensure that
        // nothing can be added by k4
        val Rns1k2_k1 = revert(ns1k2_k1)
        val id6ak7_k6 = mkAdd(IdentifierDelegation(uid6, key7), key6)
        for {
          _ <- store.append(
            SequencedTime(ts(0)),
            EffectiveTime(ts(0)),
            List(ns1k1_k1, ns1k2_k1, id1ak4_k2, ns6k6_k6),
          )
          res <- validator.validateAndUpdateHeadAuthState(
            ts(1),
            List(p1p2F_k2, Rns1k2_k1, id6ak7_k6, p1p2F_k2),
          )
        } yield {
          check(res._2, Seq(None, None, None, unauthorized))
          res._1.cascadingNamespaces shouldBe Set(ns1)
          res._1.filteredCascadingUids shouldBe Set(uid6)
        }
      }
    }
  }

}
