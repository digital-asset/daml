// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.data.EitherT
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, SuppressingLogger, SuppressionRule}
import com.digitalasset.canton.time.{Clock, SimClock}
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.{Add, Remove}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.event.Level

import scala.concurrent.Future

// TODO(#15303) Remove this trait
trait TopologyManagerTest
    extends AnyWordSpec
    with BeforeAndAfterAll
    with BaseTest
    with HasExecutionContext {

  def topologyManager[E <: CantonError](
      mk: (
          Clock,
          TopologyStore[TopologyStoreId.AuthorizedStore],
          Crypto,
          NamedLoggerFactory,
      ) => Future[TopologyManager[E]]
  ): Unit = {

    val loggerFactory = SuppressingLogger(getClass)
    val clock = new SimClock(CantonTimestamp.Epoch, loggerFactory)

    class MySetup(
        val crypto: Crypto,
        val namespaceKey: SigningPublicKey,
        val alphaKey: SigningPublicKey,
        val betaKey: SigningPublicKey,
    ) {
      val namespace: Namespace = Namespace(namespaceKey.id)
      val store = new InMemoryTopologyStore(
        TopologyStoreId.AuthorizedStore,
        loggerFactory,
        timeouts,
        futureSupervisor,
      )
      val uid: UniqueIdentifier = UniqueIdentifier(Identifier.tryCreate("da"), namespace)

      def createCertNamespace(key: SigningPublicKey, root: Boolean): TopologyStateUpdate[Add] = {
        TopologyStateUpdate.createAdd(
          NamespaceDelegation(namespace, key, isRootDelegation = root),
          testedProtocolVersion,
        )
      }

      def createCertIdentifier(key: SigningPublicKey): TopologyStateUpdate[Add] = {
        TopologyStateUpdate.createAdd(
          IdentifierDelegation(uid, key),
          testedProtocolVersion,
        )
      }

      def createOwnerToKeyMapping(key: SigningPublicKey): TopologyStateUpdate[Add] = {
        TopologyStateUpdate.createAdd(
          OwnerToKeyMapping(ParticipantId(uid), key),
          testedProtocolVersion,
        )
      }
    }

    object MySetup {

      def create(str: String): Future[MySetup] = {
        val crypto = SymbolicCrypto.create(
          testedReleaseProtocolVersion,
          timeouts,
          loggerFactory,
        )

        def generateSigningKey(name: String): Future[SigningPublicKey] = {
          crypto
            .generateSigningKey(name = Some(KeyName.tryCreate(name)))
            .valueOr(err => fail(s"Failed to generate signing key: $err"))
        }

        for {
          namespaceKey <- generateSigningKey(s"namespace-$str")
          alphaKey <- generateSigningKey(s"alpha-$str")
          betaKey <- generateSigningKey(s"beta-$str")
        } yield new MySetup(crypto, namespaceKey, alphaKey, betaKey)

      }
    }

    def gen(str: String): Future[(TopologyManager[E], MySetup)] =
      for {
        mysetup <- MySetup.create(str)
        idManager <- mk(clock, mysetup.store, mysetup.crypto, loggerFactory)
      } yield (idManager, mysetup)

    def authorizeTransaction(
        mgr: TopologyManager[E],
        cert: TopologyStateUpdate[Add],
        signingKey: SigningPublicKey,
    ) = {
      for {
        auth <- mgr
          .authorize(
            cert,
            Some(signingKey.fingerprint),
            testedProtocolVersion,
          )
          .value
        transaction = auth.getOrElse(fail("root cert addition failed"))
      } yield (cert, transaction)
    }

    def addCertNamespace(
        genr: (TopologyManager[E], MySetup),
        targetKey: SigningPublicKey,
        signingKey: SigningPublicKey,
        root: Boolean,
    ) = {
      val (mgr, setup) = genr
      val cert = setup.createCertNamespace(targetKey, root)
      authorizeTransaction(mgr, cert, signingKey)
    }

    def addCertIdentifier(
        genr: (TopologyManager[E], MySetup),
        targetKey: SigningPublicKey,
        signingKey: SigningPublicKey,
    ) = {
      val (mgr, setup) = genr
      val cert = setup.createCertIdentifier(targetKey)
      authorizeTransaction(mgr, cert, signingKey)
    }

    def addOwnerToKeyMapping(
        genr: (TopologyManager[E], MySetup),
        targetKey: SigningPublicKey,
        signingKey: SigningPublicKey,
    ) = {
      val (mgr, setup) = genr
      val cert = setup.createOwnerToKeyMapping(targetKey)
      authorizeTransaction(mgr, cert, signingKey)
    }

    def checkStore(
        store: TopologyStore[TopologyStoreId],
        numTransactions: Int,
        numActive: Int,
    ): Future[Unit] =
      for {
        tr <- store.allTransactions()
        ac <- store.headTransactions
      } yield {
        tr.result should have length numTransactions.toLong
        ac.result should have length numActive.toLong
      }

    "authorize" should {
      "successfully add one" in {
        (for {
          genr @ (_, setup) <- gen("success")
          _ <- addCertNamespace(
            genr,
            setup.namespaceKey,
            setup.namespaceKey,
            root = true,
          ).failOnShutdown
          _ <- checkStore(setup.store, numTransactions = 1, numActive = 1)
        } yield succeed).futureValue
      }
      "automatically find a signing key" in {
        def add(mgr: TopologyManager[E], mapping: TopologyStateUpdateMapping) =
          mgr.authorize(
            TopologyStateUpdate.createAdd(mapping, testedProtocolVersion),
            None,
            testedProtocolVersion,
            force = true,
          )

        val res = for {
          genr <- EitherT.right(gen("success")).mapK(FutureUnlessShutdown.outcomeK)
          (mgr, setup) = genr
          _ <- EitherT.right(
            addCertNamespace(genr, setup.namespaceKey, setup.namespaceKey, root = true)
          )
          uid = UniqueIdentifier(Identifier.tryCreate("Apple"), setup.namespace)
          uid2 = UniqueIdentifier(Identifier.tryCreate("Banana"), setup.namespace)
          extUid = UniqueIdentifier(
            Identifier.tryCreate("Ext"),
            Namespace(setup.betaKey.fingerprint),
          )
          _ <- add(
            mgr,
            NamespaceDelegation(setup.namespace, setup.alphaKey, isRootDelegation = false),
          )
          _ <- add(mgr, IdentifierDelegation(uid, setup.alphaKey))
          _ <- add(mgr, OwnerToKeyMapping(ParticipantId(uid), setup.betaKey))
          _ <- add(
            mgr,
            PartyToParticipant(
              RequestSide.Both,
              PartyId(uid),
              ParticipantId(uid2),
              ParticipantPermission.Submission,
            ),
          )
          _ <- add(
            mgr,
            PartyToParticipant(
              RequestSide.From,
              PartyId(uid),
              ParticipantId(extUid),
              ParticipantPermission.Submission,
            ),
          )
          _ <- add(
            mgr,
            PartyToParticipant(
              RequestSide.To,
              PartyId(extUid),
              ParticipantId(uid),
              ParticipantPermission.Submission,
            ),
          )
        } yield succeed

        res.value.failOnShutdown.futureValue
      }
      "allow to add and remove one" in {
        (for {
          genr @ (mgr, setup) <- gen("success")
          (rootCert, _) <- addCertNamespace(
            genr,
            setup.namespaceKey,
            setup.namespaceKey,
            root = true,
          ).failOnShutdown
          authRemove <- mgr
            .authorize(
              rootCert.reverse,
              Some(setup.namespace.fingerprint),
              testedProtocolVersion,
            )
            .value
            .failOnShutdown
          _ = authRemove.value shouldBe a[SignedTopologyTransaction[_]]
          _ <- checkStore(setup.store, numTransactions = 2, numActive = 0)
        } yield succeed).futureValue
      }
      "allow to add, remove and re-add" in {
        (for {
          genr @ (mgr, setup) <- gen("success")
          (rootCert, _) <- addCertNamespace(
            genr,
            setup.namespaceKey,
            setup.namespaceKey,
            root = true,
          ).failOnShutdown
          removeCert = rootCert.reverse
          authRemove <- mgr
            .authorize(
              removeCert,
              Some(setup.namespace.fingerprint),
              testedProtocolVersion,
            )
            .value
            .failOnShutdown
          _ = authRemove.value shouldBe a[SignedTopologyTransaction[_]]
          _ <- checkStore(setup.store, numTransactions = 2, numActive = 0)
          authAdd2 <- mgr
            .authorize(
              removeCert.reverse,
              Some(setup.namespace.fingerprint),
              testedProtocolVersion,
            )
            .value
            .failOnShutdown
          _ = authAdd2.value shouldBe a[SignedTopologyTransaction[_]]
          _ <- checkStore(setup.store, numTransactions = 3, numActive = 1)
        } yield succeed).futureValue
      }
      "fail duplicate addition" in {
        (for {
          genr @ (mgr, setup) <- gen("success")
          (rootCert, _) <- addCertNamespace(
            genr,
            setup.namespaceKey,
            setup.namespaceKey,
            root = true,
          ).failOnShutdown
          authAgain <- mgr
            .authorize(rootCert, Some(setup.namespace.fingerprint), testedProtocolVersion)
            .value
            .failOnShutdown
          _ = authAgain.left.value shouldBe a[CantonError]
          _ <- checkStore(setup.store, numTransactions = 1, numActive = 1)
        } yield succeed).futureValue
      }
      "fail on duplicate removal" in {
        (for {
          genr @ (mgr, setup) <- gen("success")
          (rootCert, _) <- addCertNamespace(
            genr,
            setup.namespaceKey,
            setup.namespaceKey,
            root = true,
          ).failOnShutdown
          _ <- mgr
            .authorize(
              rootCert.reverse,
              Some(setup.namespace.fingerprint),
              testedProtocolVersion,
            )
            .value
            .failOnShutdown
          authFail <- mgr
            .authorize(
              rootCert.reverse,
              Some(setup.namespace.fingerprint),
              testedProtocolVersion,
            )
            .value
            .failOnShutdown
          _ = authFail.left.value shouldBe a[CantonError]
          _ <- checkStore(setup.store, numTransactions = 2, numActive = 0)
        } yield succeed).futureValue
      }
      "fail on invalid removal" in {
        (for {
          genr @ (mgr, setup) <- gen("success")
          (rootCert, _) <- addCertNamespace(
            genr,
            setup.namespaceKey,
            setup.namespaceKey,
            root = true,
          ).failOnShutdown
          removeRootCert = TopologyStateUpdate(Remove, rootCert.element, testedProtocolVersion)
          invalidRev = removeRootCert.reverse.reverse
          _ = assert(
            invalidRev.element.id != rootCert.element.id
          ) // ensure transaction ids are different so we are sure to fail the test
          authFail <- mgr
            .authorize(invalidRev, Some(setup.namespace.fingerprint), testedProtocolVersion)
            .value
            .failOnShutdown
          _ = authFail.left.value shouldBe a[CantonError]
          _ <- checkStore(setup.store, numTransactions = 1, numActive = 1)
        } yield succeed).futureValue
      }
      "fail on removal of namespace delegations that create dangling transactions (success if forced)" in {
        loggerFactory.assertLogsSeq(SuppressionRule.Level(Level.WARN))(
          {
            (for {
              genr @ (mgr, setup) <- gen("success")
              _ <- addCertNamespace(
                genr,
                setup.namespaceKey,
                setup.namespaceKey,
                root = true,
              ).failOnShutdown
              (cert2, _) <- addCertNamespace(
                genr,
                setup.alphaKey,
                setup.namespaceKey,
                root = true,
              ).failOnShutdown
              _ <- addCertNamespace(
                genr,
                setup.betaKey,
                setup.alphaKey,
                root = false,
              ).failOnShutdown

              removeRootCert = TopologyStateUpdate(Remove, cert2.element, testedProtocolVersion)
              authFail <- mgr
                .authorize(
                  removeRootCert,
                  Some(setup.namespaceKey.fingerprint),
                  testedProtocolVersion,
                )
                .value
                .failOnShutdown
              _ = authFail.left.value shouldBe a[CantonError]
              _ <- checkStore(setup.store, numTransactions = 3, numActive = 3)

              auth <- mgr
                .authorize(
                  removeRootCert,
                  Some(setup.namespaceKey.fingerprint),
                  testedProtocolVersion,
                  force = true,
                )
                .value
                .failOnShutdown
              _ = auth.value shouldBe a[SignedTopologyTransaction[_]]
              _ <- checkStore(setup.store, numTransactions = 4, numActive = 2)
            } yield succeed).futureValue
          },
          { entries =>
            forEvery(entries) {
              _.warningMessage should
                include regex "The following target keys of namespace (.*?) are dangling"
            }
          },
        )
      }
      "fail on removal of identifier delegations that create dangling transactions (success if forced)" in {
        (for {
          genr @ (mgr, setup) <- gen("success")
          (_, _) <- addCertNamespace(
            genr,
            setup.namespaceKey,
            setup.namespaceKey,
            root = true,
          ).failOnShutdown
          (cert2, _) <- addCertIdentifier(genr, setup.alphaKey, setup.namespaceKey).failOnShutdown
          _ <- addOwnerToKeyMapping(genr, setup.alphaKey, setup.alphaKey).failOnShutdown

          removeRootCert = TopologyStateUpdate(Remove, cert2.element, testedProtocolVersion)
          authFail <- mgr
            .authorize(removeRootCert, Some(setup.namespaceKey.fingerprint), testedProtocolVersion)
            .value
            .failOnShutdown
          _ = authFail.left.value shouldBe a[CantonError]
          _ <- checkStore(setup.store, numTransactions = 3, numActive = 3)

          auth <- mgr
            .authorize(
              removeRootCert,
              Some(setup.namespaceKey.fingerprint),
              testedProtocolVersion,
              force = true,
            )
            .value
            .failOnShutdown
          _ = auth.value shouldBe a[SignedTopologyTransaction[_]]
          _ <- checkStore(setup.store, numTransactions = 4, numActive = 2)
        } yield succeed).futureValue
      }
    }
    "add" should {
      "support importing generated topology transactions" in {
        (for {
          genr @ (mgr, setup) <- gen("success")
          (_, transaction) <- addCertNamespace(
            genr,
            setup.namespaceKey,
            setup.namespaceKey,
            root = true,
          ).failOnShutdown
          (otherMgr, otherSetup) <- gen("other")
          // check that we can import
          importCert <- otherMgr.add(transaction).value.failOnShutdown
          _ = importCert shouldBe Right(())
          _ = checkStore(otherSetup.store, 1, 1)
          // and we shouldn't be able to re-import
          reImportFail <- mgr.add(transaction).value.failOnShutdown
        } yield reImportFail.left.value shouldBe a[CantonError]).futureValue
      }
      "fail on re-adding removed topology transactions" in {
        (for {
          genr @ (mgr, setup) <- gen("success")
          (rootCert, transaction) <- addCertNamespace(
            genr,
            setup.namespaceKey,
            setup.namespaceKey,
            root = true,
          ).failOnShutdown
          (otherMgr, otherSetup) <- gen("other")
          reverted = rootCert.reverse
          authRev <- mgr
            .authorize(
              reverted,
              Some(setup.namespaceKey.fingerprint),
              testedProtocolVersion,
            )
            .value
            .failOnShutdown
          _ = authRev.value shouldBe a[SignedTopologyTransaction[_]]
          // import cert
          _ <- otherMgr.add(transaction).value.failOnShutdown
          // import reversion
          _ <- otherMgr.add(authRev.getOrElse(fail("reversion failed"))).value.failOnShutdown
          _ = checkStore(otherSetup.store, 2, 0)
          // now, re-importing previous cert should fail
          reimport <- otherMgr.add(transaction).value.failOnShutdown
          _ = reimport.left.value shouldBe a[CantonError]
        } yield succeed).futureValue
      }
    }
  }
}
