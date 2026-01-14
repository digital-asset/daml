// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import cats.syntax.either.*
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.BigDecimalImplicits.*
import com.digitalasset.canton.config.PositiveFiniteDuration
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.crypto.SessionKeyInfo
import com.digitalasset.canton.data.LightTransactionViewTree
import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.examples.java.iou.{Amount, Iou}
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransform,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.protocol.messages.EncryptedViewMessage.computeRandomnessLength
import com.digitalasset.canton.protocol.messages.{EncryptedView, EncryptedViewMessage}
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  MemberRecipient,
  Recipient,
  Recipients,
  RecipientsTree,
  SubmissionRequest,
}
import com.digitalasset.canton.serialization.DefaultDeserializationError
import com.digitalasset.canton.store.ConfirmationRequestSessionKeyStore
import com.digitalasset.canton.store.SessionKeyStore.RecipientGroup
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencer,
  SendDecision,
}
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId.Authorized
import com.digitalasset.canton.topology.{MediatorId, ParticipantId, SynchronizerId}
import monocle.macros.syntax.lens.*

import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Promise}
import scala.jdk.CollectionConverters.*

trait SessionKeyIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer {

  // participant1 and participant2 have session key caching enabled, whereas participant3 does not
  private val nodesWithSessionKey = Set("participant1", "participant2")

  var sequencer: ProgrammableSequencer = _
  var mediatorId: MediatorId = _
  var participant1Id: ParticipantId = _

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .addConfigTransforms(
        setCacheConfig(PositiveFiniteDuration.ofSeconds(20))
      )
      .withSetup { implicit env =>
        import env.*

        participants.all.synchronizers.connect_local(sequencer1, daName)
        participants.all.dars.upload(CantonExamplesPath)

        mediatorId = mediator1.id
        participant1Id = participant1.id
        sequencer = getProgrammableSequencer(sequencer1.name)
      }

  private def setCacheConfig(timeout: PositiveFiniteDuration): ConfigTransform =
    ConfigTransforms
      .updateAllParticipantConfigs { case (name, cfg) =>
        if (nodesWithSessionKey.contains(name))
          cfg
            .focus(
              _.parameters.caching.sessionEncryptionKeyCache.senderCache.expireAfterTimeout
            )
            .replace(timeout)
            .focus(
              _.parameters.caching.sessionEncryptionKeyCache.receiverCache.expireAfterTimeout
            )
            .replace(timeout)
        else // disable cache for session keys
          cfg
            .focus(_.parameters.caching.sessionEncryptionKeyCache.enabled)
            .replace(false)
      }

  private def createRecipientGroup(
      sender: LocalParticipantReference,
      informeeParticipants: Set[ParticipantId],
  ): RecipientGroup = {
    val recipientsTree = NonEmpty(
      Seq,
      RecipientsTree(
        NonEmptyUtil.fromUnsafe(
          informeeParticipants
            .map(pId => MemberRecipient(pId).asInstanceOf[Recipient])
        ),
        Seq.empty,
      ),
    )
    val recipients = Recipients(recipientsTree)
    RecipientGroup(recipients, sender.crypto.pureCrypto.defaultSymmetricKeyScheme)
  }

  private def checkSessionKeyCaches(
      senderSessionKeyStore: ConfirmationRequestSessionKeyStore,
      recipients: Set[LocalParticipantReference],
      recipientGroup: RecipientGroup,
      synchronizerId: SynchronizerId,
  )(implicit ec: ExecutionContext): SessionKeyInfo = {

    // check that the sender's cache has the correct session key information for a given recipient group
    val sessionKeyInfo = senderSessionKeyStore
      .getSessionKeyInfoIfPresent(recipientGroup)
      .valueOrFail("empty session key info")

    // check that the recipients cache has the correct session key
    forAll(recipients) { recipient =>
      val encryptedSessionKey = sessionKeyInfo.encryptedSessionKeys
        .find { encryptedSessionKey =>
          recipient.crypto.cryptoPrivateStore
            .existsDecryptionKey(encryptedSessionKey.encryptedFor)
            .valueOrFailShutdown("error checking that encryption key exists")
            .futureValue
        }
        .valueOrFail("did not find the matching encrypted session key")
      val randomness =
        getSessionKeyStoreFromLocalNode(recipient, synchronizerId).getSessionKeyRandomnessIfPresent(
          encryptedSessionKey
        )
      randomness shouldBe Some(sessionKeyInfo.sessionKeyAndReference.randomness)
    }
    sessionKeyInfo
  }

  def getSessionKeyStoreFromLocalNode(
      participant: LocalParticipantReference,
      synchronizerId: SynchronizerId,
  )(implicit ec: ExecutionContext): ConfirmationRequestSessionKeyStore =
    participant.underlying
      .valueOrFail("no underlying participant node")
      .sync
      .readyConnectedSynchronizerById(synchronizerId)
      .valueOrFail(s"could not get sync for this synchronizer $synchronizerId")
      .ephemeral
      .sessionKeyStore
      .convertStore

  "a session encryption key" must {
    "be correctly created and persisted after a transaction" in { implicit env =>
      import env.*

      val p1Sync = getSessionKeyStoreFromLocalNode(participant1, daId)
      val p2Sync = getSessionKeyStoreFromLocalNode(participant2, daId)

      // ping participant1 and check that a session key for recipient P1 has been created
      val recipientGroupP1 = createRecipientGroup(participant1, Set(participant1.id))

      p1Sync.getSessionKeyInfoIfPresent(recipientGroupP1) shouldBe None
      participant1.health.ping(participant1.id)

      val sessionKeyInfoGroupP1 =
        checkSessionKeyCaches(p1Sync, Set(participant1), recipientGroupP1, daId)

      // check that the session key has been persisted after ping
      participant1.health.ping(participant1.id)

      p1Sync.getSessionKeyInfoIfPresent(recipientGroupP1) shouldBe Some(
        sessionKeyInfoGroupP1
      )
      p1Sync.getSessionKeyRandomnessIfPresent(
        sessionKeyInfoGroupP1.encryptedSessionKeys(0)
      ) shouldBe Some(sessionKeyInfoGroupP1.sessionKeyAndReference.randomness)

      // ping participant2 and check that a session key for recipient P1/P2 has been created
      val recipientGroupP1P2 =
        createRecipientGroup(
          participant1,
          Set(participant1.id, participant2.id),
        )

      p2Sync.getSessionKeyInfoIfPresent(recipientGroupP1) shouldBe None
      p2Sync.getSessionKeyInfoIfPresent(recipientGroupP1P2) shouldBe None
      participant1.health.ping(participant2.id)

      checkSessionKeyCaches(p1Sync, Set(participant1, participant2), recipientGroupP1P2, daId)
    }

    "be able to decrypt the randomness for a given view" in { implicit env =>
      import env.*

      val p1Sync = getSessionKeyStoreFromLocalNode(participant1, daId)

      // TODO(i25076): remove synchronizeParticipants once party allocation topology events are on the ledger api
      val alice = participant1.parties.enable(
        "Alice"
      )
      val bob = participant2.parties.enable(
        "Bob"
      )
      val jerry = participant3.parties.enable(
        "Jerry"
      )

      val createIouCmd = new Iou(
        alice.toProtoPrimitive,
        bob.toProtoPrimitive,
        new Amount(100.0.toBigDecimal, "USD"),
        List(jerry.toProtoPrimitive).asJava,
      ).create.commands.asScala.toSeq

      val recipientGroupTx =
        createRecipientGroup(
          participant1,
          Set(participant1.id, participant2.id, participant3.id),
        )

      p1Sync.getSessionKeyInfoIfPresent(recipientGroupTx) shouldBe None

      // intercept the submission request
      val submissionRequestP = Promise[SubmissionRequest]()
      sequencer.setPolicy_("capture next submission request") { submissionRequest =>
        if (submissionRequest.sender == participant1Id && submissionRequest.isConfirmationRequest)
          submissionRequestP.trySuccess(submissionRequest)

        SendDecision.Process
      }
      participant1.ledger_api.javaapi.commands.submit(Seq(alice), createIouCmd)
      val submissionRequest = submissionRequestP.future.futureValue

      // caching is disabled for participant3
      val sessionKeyInfoGroupTx = checkSessionKeyCaches(
        p1Sync,
        Set(participant1, participant2),
        recipientGroupTx,
        daId,
      )

      // check that the session key can decrypt the randomness for the encrypted view
      val pureCrypto = participant1.crypto.pureCrypto
      val sessionKeyGroupTx = pureCrypto
        .createSymmetricKey(
          sessionKeyInfoGroupTx.sessionKeyAndReference.randomness
        )
        .valueOrFail("failed to create session key from randomness")
      val messages = Batch
        .openEnvelopes(submissionRequest.batch)(
          testedProtocolVersion,
          pureCrypto,
        )
        ._1
        .envelopes
        .map(_.protocolMessage)

      messages.exists {
        case encryptedViewMessage: EncryptedViewMessage[?] =>
          val attemptDecryptionOfTransactionView = for {
            encryptedTransactionViewMessage <-
              encryptedViewMessage.traverse(_.select(TransactionViewType))
            _ <- EncryptedView
              .decrypt(
                pureCrypto,
                sessionKeyGroupTx,
                encryptedTransactionViewMessage.encryptedView,
              )(
                bytes => {
                  LightTransactionViewTree
                    .fromByteString(
                      (pureCrypto, computeRandomnessLength(pureCrypto)),
                      testedProtocolVersion,
                    )(bytes)
                    .leftMap(err => DefaultDeserializationError(err.message))
                },
                defaultMaxBytesToDecompress,
              )
              .toOption
          } yield ()
          attemptDecryptionOfTransactionView.isDefined
        case _ =>
          false
      } shouldBe true

    }

    "be replaced if encryption key is changed" in { implicit env =>
      import env.*

      def compareSessionKeysAfterRotation(rotateSender: Boolean) = {
        val p1Sync = getSessionKeyStoreFromLocalNode(participant1, daId)
        val p2Sync = getSessionKeyStoreFromLocalNode(participant2, daId)
        val recipientGroup =
          createRecipientGroup(
            participant1,
            Set(participant1.id, participant2.id),
          )

        val sessionKeyInfoOld = eventually() {
          // enough time could have elapsed since the last ping and the cache could have expired
          participant1.health.ping(participant2.id)
          p1Sync.getSessionKeyInfoIfPresent(recipientGroup).valueOrFail("empty session key info")
        }

        // find the encrypted session key for the recipient (participant2)
        val receiverKey = participant2.crypto.cryptoPublicStore.encryptionKeys.futureValueUS
          .toSeq(0)
        val encryptedSessionKey = sessionKeyInfoOld.encryptedSessionKeys
          .find(_.encryptedFor == receiverKey.fingerprint)
          .valueOrFail("could not find encrypted session key for recipient")

        // get the session key randomness from the recipient
        val sessionKeyRandomnessOld = p2Sync
          .getSessionKeyRandomnessIfPresent(encryptedSessionKey)
          .valueOrFail("empty session key randomness")

        // the sessionKeyRandomness from the sender and recipient should match
        sessionKeyRandomnessOld shouldBe sessionKeyInfoOld.sessionKeyAndReference.randomness

        val participantToRotate = if (rotateSender) participant1 else participant2
        val startingOtk = participantToRotate.topology.owner_to_key_mappings
          .list(
            store = Authorized,
            filterKeyOwnerUid = participantToRotate.id.filterString,
          )
          .loneElement
        val startingSerial = startingOtk.context.serial

        logger.debug(
          s"starting serial on ${if (rotateSender) participant1.id else participant2.id}: $startingSerial"
        )
        // rotating a key first adds the key and then removes it, adding 2 new "generations" of the mapping per key
        val targetSerial = startingSerial.tryAdd(startingOtk.item.keys.size * 2)

        // rotate encryption key and ping
        if (rotateSender) participant1.keys.secret.rotate_node_keys()
        else participant2.keys.secret.rotate_node_keys()

        Seq(participant1, participant2).foreach { p =>
          eventually() {
            val serial = p.topology.owner_to_key_mappings
              .list(daId, filterKeyOwnerUid = participantToRotate.id.filterString)
              .loneElement
              .context
              .serial
              .value
            serial shouldBe targetSerial.value
          }
        }

        participant1.health.ping(participant2.id)

        val sessionKeyInfoNew =
          p1Sync.getSessionKeyInfoIfPresent(recipientGroup).valueOrFail("empty session key info")

        // because the encryption key was changed, the session key info should be different
        sessionKeyInfoNew.sessionKeyAndReference.randomness should not be
          sessionKeyInfoOld.sessionKeyAndReference.randomness

        val receiverKeyId =
          if (rotateSender) receiverKey.fingerprint
          else {
            // recipient key changed so we need to retrieve the new key
            val receiverKeyNew =
              participant2.crypto.cryptoPublicStore.encryptionKeys.futureValueUS.toSeq
                .filterNot(_.fingerprint == receiverKey.fingerprint)(0)

            // make sure the key is rotated
            receiverKeyNew should not be receiverKey

            receiverKeyNew.fingerprint
          }

        val encryptedSessionKeyNew = sessionKeyInfoNew.encryptedSessionKeys
          .find(_.encryptedFor == receiverKeyId)
          .valueOrFail("could not find encrypted session key for recipient")

        val sessionKeyRandomnessNew = p2Sync
          .getSessionKeyRandomnessIfPresent(
            encryptedSessionKeyNew
          )
          .valueOrFail("empty session key randomness")

        sessionKeyInfoOld should not be sessionKeyInfoNew
        sessionKeyRandomnessOld should not be sessionKeyRandomnessNew
        sessionKeyInfoNew.sessionKeyAndReference.randomness shouldBe sessionKeyRandomnessNew
      }

      compareSessionKeysAfterRotation(rotateSender = true)
      compareSessionKeysAfterRotation(rotateSender = false)
    }

    "be evicted if timeout in cache has elapsed" in { implicit env =>
      import env.*

      // enough time could have elapsed since the last ping and the cache could have expired
      participant1.health.ping(participant1.id)

      val p1Sync = getSessionKeyStoreFromLocalNode(participant1, daId)
      val recipientGroupP1 = createRecipientGroup(
        participant1,
        Set(participant1.id),
      )
      // make sure that the session key information is still there
      val sessionKeyInfo =
        p1Sync.getSessionKeyInfoIfPresent(recipientGroupP1).valueOrFail("empty session key info")

      eventually(timeUntilSuccess = 30.seconds) {
        // make sure that the session key information has been evicted from the cache
        p1Sync.getSessionKeyInfoIfPresent(recipientGroupP1) shouldBe None
        p1Sync.getSessionKeyRandomnessIfPresent(
          sessionKeyInfo.encryptedSessionKeys(0)
        ) shouldBe None
      }

    }

    "not be stored in cache when its disabled" in { implicit env =>
      import env.*

      val p3Sync = getSessionKeyStoreFromLocalNode(participant3, daId)
      val recipientGroupP3 = createRecipientGroup(
        participant3,
        Set(participant3.id),
      )

      p3Sync.getSessionKeyInfoIfPresent(recipientGroupP3) shouldBe None
      participant3.health.ping(participant3.id)

      // make sure we are not using the cache
      p3Sync.getSessionKeyInfoIfPresent(recipientGroupP3) shouldBe None
    }
  }

}

class SessionKeyBftOrderingIntegrationTestPostgres extends SessionKeyIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
