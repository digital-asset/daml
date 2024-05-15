// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.demo

import com.daml.ledger.api.v2.participant_offset.ParticipantOffset
import com.daml.ledger.javaapi.data.codegen.{Contract, ContractCompanion, ContractId}
import com.daml.ledger.javaapi.data.{Template, TransactionTree}
import com.digitalasset.canton.admin.api.client.data.StaticDomainParameters
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config
import com.digitalasset.canton.console.commands.DomainChoice
import com.digitalasset.canton.console.{
  ConsoleEnvironment,
  ConsoleMacros,
  ParticipantReference,
  SequencerReference,
}
import com.digitalasset.canton.demo.Step.{Action, Noop}
import com.digitalasset.canton.demo.model.ai.java as ME
import com.digitalasset.canton.demo.model.doctor.java as M
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.domain.DomainConnectionConfig
import com.digitalasset.canton.protocol.DynamicDomainParameters
import com.digitalasset.canton.sequencing.{SequencerConnection, SequencerConnections}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import java.time.{Duration, Instant}
import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext, Future, blocking}
import scala.jdk.CollectionConverters.*

class ReferenceDemoScript(
    participants: Seq[ParticipantReference],
    bankingConnection: SequencerConnection,
    medicalConnection: SequencerConnection,
    rootPath: String,
    maxWaitForPruning: Duration,
    editionSupportsPruning: Boolean,
    darPath: Option[String] = None,
    additionalChecks: Boolean = false,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends BaseScript
    with NamedLogging {

  import scala.language.implicitConversions

  import ReferenceDemoScript.*

  implicit def toPrimitive(partyId: PartyId): String = partyId.toProtoPrimitive
  implicit def toScalaSeq[A](l: java.util.List[A]): Seq[A] = l.asScala.toSeq
  implicit def toJavaList[A](l: List[A]): java.util.List[A] = l.asJava

  require(participants.lengthIs > 5, "I need 6 participants for this demo")
  private val sorted = participants.sortBy(_.name)

  private val participant1 = sorted(0)
  private val participant2 = sorted(1)
  private val participant3 = sorted(2)
  private val participant4 = sorted(3)
  private val participant5 = sorted(4)
  private val participant6 = sorted(5)

  import com.digitalasset.canton.console.ConsoleEnvironment.Implicits.*

  val maxImage: Int = 28

  private val readyToSubscribeM = new AtomicReference[Map[String, ParticipantOffset]](Map())

  override def subscriptions(): Map[String, ParticipantOffset] = readyToSubscribeM.get()

  def imagePath: String = s"file:$rootPath/images/"

  private val SequencerBankingAndConnection = (SequencerBanking, bankingConnection)
  private val SequencerMedicalAndConnection = (SequencerMedical, medicalConnection)

  private val settings = Seq(
    (
      "Alice",
      participant1,
      Seq(SequencerMedicalAndConnection),
      Seq("bank", "medical-records", "health-insurance", "doctor"),
    ),
    (
      "Doctor",
      participant2,
      Seq(SequencerMedicalAndConnection, SequencerBankingAndConnection),
      Seq("bank", "medical-records", "health-insurance", "doctor"),
    ),
    (
      "Insurance",
      participant3,
      Seq(SequencerBankingAndConnection, SequencerMedicalAndConnection),
      Seq("bank", "health-insurance"),
    ),
    ("Bank", participant4, Seq(SequencerBankingAndConnection), Seq("bank")),
    ("Registry", participant5, Seq(SequencerMedicalAndConnection), Seq("medical-records")),
  )

  override def parties(): Seq[(String, ParticipantReference)] = partyIdCache.toSeq.map {
    case (name, (_, participant)) => (name, participant)
  }

  private val partyIdCache = mutable.LinkedHashMap[String, (PartyId, ParticipantReference)]()
  private def partyId(name: String): PartyId = {
    partyIdCache.getOrElse(name, sys.error(s"Failed to lookup party $name"))._1
  }

  private def darFile(dar: String): String =
    darPath.map(path => s"$path/$dar.dar").getOrElse(s"$rootPath/dars/$dar.dar")

  private val lookupTimeoutSeconds: Long =
    System.getProperty("canton-demo.lookup-timeout-seconds", "40").toLong
  private val lookupTimeout =
    config.NonNegativeDuration.tryFromJavaDuration(
      java.time.Duration.ofSeconds(lookupTimeoutSeconds)
    )
  private val syncTimeout = Some(
    config.NonNegativeDuration.tryFromJavaDuration(
      java.time.Duration.ofSeconds(
        System.getProperty("canton-demo.sync-timeout-seconds", "30").toLong
      )
    )
  )

  private lazy val alice = partyId("Alice")
  private lazy val registry = partyId("Registry")
  private lazy val insurance = partyId("Insurance")
  private lazy val doctor = partyId("Doctor")
  private lazy val bank = partyId("Bank")
  private lazy val processor = partyId("Processor")

  private def aliceLookup[
      TC <: Contract[TCid, T],
      TCid <: ContractId[T],
      T <: Template,
  ](
      companion: ContractCompanion[TC, TCid, T]
  ): TC =
    participant1.ledger_api.javaapi.state.acs.await(companion)(alice, timeout = lookupTimeout)
  private def doctorLookup[
      TC <: Contract[TCid, T],
      TCid <: ContractId[T],
      T <: Template,
  ](
      companion: ContractCompanion[TC, TCid, T]
  ): TC =
    participant2.ledger_api.javaapi.state.acs.await(companion)(doctor, timeout = lookupTimeout)
  private def insuranceLookup[
      TC <: Contract[TCid, T],
      TCid <: ContractId[T],
      T <: Template,
  ](
      companion: ContractCompanion[TC, TCid, T]
  ): TC =
    participant3.ledger_api.javaapi.state.acs
      .await(companion)(insurance, timeout = lookupTimeout)
  private def processorLookup[
      TC <: Contract[TCid, T],
      TCid <: ContractId[T],
      T <: Template,
  ](
      companion: ContractCompanion[TC, TCid, T]
  ): TC =
    participant6.ledger_api.javaapi.state.acs
      .await(companion)(processor, timeout = lookupTimeout)
  private def registryLookup[
      TC <: Contract[TCid, T],
      TCid <: ContractId[T],
      T <: Template,
  ](
      companion: ContractCompanion[TC, TCid, T]
  ): TC =
    participant5.ledger_api.javaapi.state.acs.await(companion)(registry, timeout = lookupTimeout)

  private def execute[T](futs: Seq[Future[T]]): Seq[T] = {
    import scala.concurrent.duration.*
    val seq = Future.sequence(futs)
    Await.result(seq, 120.seconds)
  }

  private def connectDomain(
      participant: ParticipantReference,
      name: String,
      connection: SequencerConnection,
  ): Unit = {
    participant.domains.connect_by_config(
      DomainConnectionConfig(
        name,
        SequencerConnections.single(connection),
      )
    )
    participant.domains.reconnect(name).discard
  }

  private val pruningOffset = new AtomicReference[Option[(ParticipantOffset, Instant)]](None)
  val steps = TraceContext.withNewTraceContext { implicit traceContext =>
    List[Step](
      Noop, // pres page nr = page * 2 - 1
      Noop,
      Noop,
      Noop,
      Action(
        "Participants set up parties",
        "admin-api",
        "participant.parties.enable(NAME)",
        () => {
          execute(settings.map { case (name, participant, _, _) =>
            logger.info(s"Enabling party $name on participant ${participant.id.toString}")
            Future {
              blocking {
                val pid = participant.parties.enable(name)
                (name, pid, participant)
              }
            }
          }).foreach { case (name, pid, participant) =>
            partyIdCache.put(name, (pid, participant)).discard
            readyToSubscribeM
              .updateAndGet(cur => cur + (name -> ParticipantTab.LedgerBegin))
              .discard[Map[String, ParticipantOffset]]
          }

        },
      ),
      Noop,
      Action(
        "Participants connect to domain(s)",
        "admin-api",
        "participant.domains.register(<name>, \"http(s)://hostname:port\")",
        () => {
          logger.info("Connecting participants to domains")
          val res = settings.flatMap { case (_, participant, domains, _) =>
            domains.map { case (name, connection) =>
              Future {
                blocking {
                  connectDomain(participant, name, connection)
                }
              }
            }
          }
          val _ = execute(res)
        },
      ),
      Noop,
      Noop,
      Noop,
      Noop,
      Noop,
      Action(
        "Participants upload DARs",
        "admin-api",
        "participant.dars.upload(<filename>)",
        () => {
          settings.foreach { case (name, participant, domains, dars) =>
            // TODO(#14069) Uploading packages concurrently can lead to topology failures due to races between
            //  vetting topology transactions, which can lead to the transaction serial not increasing and, thus,
            //  to some transaction being rejected, so uploads are performed sequentially to avoid flakes.
            dars.map(darFile).foreach { dar =>
              logger.debug(s"Uploading dar $dar for $name")
              val _ = participant.dars.upload(dar)
            }
            // wait until parties are registered with all domains
            ConsoleMacros.utils.retry_until_true(lookupTimeout) {
              participant.parties
                .hosted(filterParty = name)
                .flatMap(_.participants)
                .flatMap(_.domains)
                .length == domains.length
            }
            // Force the time proofs to be updated after topology transactions
            // TODO(i13200) The following line can be removed once the ticket is closed
            participant.testing.fetch_domain_times()
          }
        },
      ),
      Action(
        "Create initial state by registering some cash, insurance policies and medical records",
        "ledger-api",
        "create Cash with issuer = Bank, owner = ... ; create Policy with insurance = ...; create Register with ...",
        () => {
          // create cash
          def cashFor(owner: String, qty: Int) =
            new M.bank.Cash(
              bank,
              partyId(owner),
              new M.bank.Amount(qty.toLong, "EUR"),
            ).create.commands

          val a = Future {
            blocking {
              participant4.ledger_api.javaapi.commands
                .submit(
                  Seq(bank),
                  cashFor("Insurance", 100) ++ cashFor("Doctor", 5),
                  optTimeout = syncTimeout,
                )
            }
          }

          // create policy
          val treatments = List("Flu-shot", "Hip-replacement", "General counsel")
          val b = Future {
            blocking {
              participant3.ledger_api.javaapi.commands.submit(
                Seq(insurance),
                new M.healthinsurance.Policy(
                  insurance,
                  alice,
                  bank,
                  treatments,
                  List(),
                ).create.commands,
                optTimeout = syncTimeout,
              )
            }
          }
          // create register
          val c = Future {
            blocking {
              participant5.ledger_api.javaapi.commands.submit(
                Seq(registry),
                new M.medicalrecord.Register(
                  registry,
                  alice,
                  List(),
                  List(),
                ).create.commands,
                optTimeout = syncTimeout,
              )
            }
          }
          val _ = execute(Seq(a, b, c))
        },
      ),
      Noop,
      Action(
        "Doctor offering appointment",
        "ledger-api",
        "Doctor: create OfferAppointment with patient = Alice, doctor = Doctor",
        () => {
          val offer =
            new M.doctor.OfferAppointment(doctor, alice).create.commands
          val _ =
            participant2.ledger_api.javaapi.commands
              .submit(Seq(doctor), offer, optTimeout = syncTimeout)
        },
      ),
      Action(
        "Patient Alice accepts offer",
        "ledger-api",
        "Alice: exercise <offerId> AcceptAppointment with registerId = <registerId>, policyId = <policyId>",
        () => {
          val appointmentEv = aliceLookup(M.doctor.OfferAppointment.COMPANION)
          val policyId = aliceLookup(M.healthinsurance.Policy.COMPANION).id
          val registerId = aliceLookup(M.medicalrecord.Register.COMPANION).id
          val acceptOffer =
            appointmentEv.id
              .exerciseAcceptAppointment(registerId, policyId)
              .commands
          val _ = participant1.ledger_api.javaapi.commands
            .submit(Seq(alice), acceptOffer, optTimeout = syncTimeout)
        },
      ),
      Action(
        "Doctor finalises appointment",
        "ledger-api",
        "Doctor: exercise <appointmentId> TickOff with description=...",
        () => {
          val tickOff = doctorLookup(M.doctor.Appointment.COMPANION).id
            .exerciseTickOff(
              "Did a hip replacement",
              "Hip-replacement",
              new M.bank.Amount(15, "EUR"),
            )
            .commands
          val _ = participant2.ledger_api.javaapi.commands
            .submit(Seq(doctor), tickOff, optTimeout = syncTimeout)
        },
      ),
      Action(
        "Insurance settles claim",
        "ledger-api",
        "Insurance: exercise <claimId> AcceptAndSettleClaim with cashId = <cashId>",
        () => {
          // Force the time proofs to be updated after topology transactions
          // TODO(i13200) The following line can be removed once the ticket is closed
          participant3.testing.fetch_domain_times()
          val withdraw = {
            insuranceLookup(M.bank.Cash.COMPANION).id.exerciseSplit(15).commands
          }
          participant3.ledger_api.javaapi.commands
            .submit(Seq(insurance), withdraw, optTimeout = syncTimeout)
            .discard[TransactionTree]

          def findCashCid =
            participant3.ledger_api.javaapi.state.acs
              .await(M.bank.Cash.COMPANION)(insurance, _.data.amount.quantity == 15)

          // settle claim (will invoke auto-transfer to the banking domain)
          val settleClaim = {
            insuranceLookup(M.healthinsurance.Claim.COMPANION).id
              .exerciseAcceptAndSettleClaim(findCashCid.id)
              .commands
          }
          participant3.ledger_api.javaapi.commands
            .submit(Seq(insurance), settleClaim, optTimeout = syncTimeout)
            .discard[TransactionTree]
        },
      ),
      Noop,
      Action(
        "Alice takes control over medical records",
        "ledger-api",
        "exercise <registerId> TransferRecords with newRegistry = Alice",
        () => {
          val archiveRequest = aliceLookup(M.medicalrecord.Register.COMPANION).id
            .exerciseTransferRecords(alice)
            .commands
          participant1.ledger_api.javaapi.commands
            .submit(Seq(alice), archiveRequest, optTimeout = syncTimeout)
            .discard[TransactionTree]
          // wait until the acs of the registry is empty
          ConsoleMacros.utils.retry_until_true(lookupTimeout) {
            participant5.ledger_api.state.acs.of_party(registry).isEmpty
          }
          // now, remember the offset to prune at
          val participantOffset = participant5.ledger_api.state.end()
          // Trigger advancement of the clean head, so the previous contracts become safe to prune
          if (editionSupportsPruning) {
            participant5.health
              .ping(
                participant5,
                timeout = 60.seconds,
              )
              .discard // sequencer integrations can be veeeerrrry slow
          }
          pruningOffset.set(Some((participantOffset, Instant.now)))
        },
      ),
      Action(
        "Registry Prunes Ledger",
        "admin-api",
        "pruning prune ledgerEndOffset",
        () => {
          // Wait for the previous contracts to exit the time window needed for crash recovery
          if (editionSupportsPruning) {

            val prunedOffset = pruningOffset
              .get()
              .map { case (offset, started) =>
                val waitUntil = started.plus(maxWaitForPruning).plusSeconds(1)
                // now wait until mediator & participant timeouts elapsed
                val now = Instant.now()
                val waitDurationMaybeNegative = Duration.between(now, waitUntil)
                val waitDuration =
                  if (waitDurationMaybeNegative.isNegative) Duration.ZERO
                  else waitDurationMaybeNegative
                logger.info(s"I have to wait for $waitDuration before I can kick off pruning")
                Threading.sleep(waitDuration.toMillis)
                // now, flush all participants that have some business with this node
                Seq(participant1, participant2, participant5).foreach(p =>
                  participant5.health
                    .ping(p, timeout = 60.seconds)
                    .discard[scala.concurrent.duration.Duration]
                )
                // give the ACS commitment processor some time to catchup
                Threading.sleep(5.seconds.toMillis)
                logger.info(s"Pruning ledger up to offset $offset inclusively")
                participant5.pruning.prune(offset)
                logger.info(s"Pruned ledger up to offset $offset inclusively.")
                offset
              }
              .getOrElse(throw new RuntimeException("Unable to prune the ledger."))
            if (additionalChecks) {
              val transactions =
                participant5.ledger_api.updates
                  .flat(Set(registry), completeAfter = 5, beginOffset = prunedOffset)
              // ensure we don't see any transactions
              require(transactions.isEmpty, s"transactions should be empty but was ${transactions}")
            }
          }
          // ensure registry tab resubscribes after the pruning offset
          pruningOffset
            .get()
            .foreach { prunedOffset =>
              readyToSubscribeM.updateAndGet(_ + ("Registry" -> prunedOffset._1))
            }
        },
      ),
      Noop,
      Noop,
      Noop,
      Noop,
      Action(
        "New AI processor participant joins",
        "admin-api",
        "participant parties.enable | domains.connect | upload_dar ai-analysis.dar",
        () => {
          val registerDomainF = Future {
            blocking {
              connectDomain(participant6, SequencerMedical, medicalConnection)
            }
          }
          val filename = darFile("ai-analysis")
          val allF = Seq(participant5, participant1, participant6).map(participant => {
            Future {
              blocking {
                participant.dars.upload(filename)
              }
            }
          }) :+ Future {
            blocking {}
          } :+ registerDomainF
          // once all dars are uploaded and we've connected the domain, register the party (as we can flush everything there ...)
          val sf = Future
            .sequence(allF)
            .flatMap(_ =>
              Future {
                blocking {
                  val processorId =
                    participant6.parties.enable("Processor", waitForDomain = DomainChoice.All)
                  partyIdCache.put("Processor", (processorId, participant6))
                }
              }
            )
          execute(Seq(sf.map(_ => {
            val offer = new ME.aianalysis.OfferAnalysis(
              registry,
              alice,
              processor,
            ).create.commands
            participant5.ledger_api.javaapi.commands
              .submit(Seq(registry), offer, optTimeout = syncTimeout)
              .discard[TransactionTree]
          }))).discard
        },
      ),
      Action(
        "Alice accepts AI Analytics service offer",
        "ledger-api",
        "exercise offer AcceptAnalysis with registerId",
        () => {
          val registerId = aliceLookup(ME.medicalrecord.Register.COMPANION)
          val accept = aliceLookup(ME.aianalysis.OfferAnalysis.COMPANION).id
            .exerciseAcceptAnalysis(registerId.id)
            .commands
          participant1.ledger_api.javaapi.commands
            .submit(Seq(alice), accept, optTimeout = syncTimeout)
            .discard[TransactionTree]
        },
      ),
      Action(
        "Records are processed and result is recorded",
        "ledger-api",
        "exercise records ProcessingDone with diagnosis = ...; exercise pendingAnalysis RecordResult",
        () => {
          val processingDone = processorLookup(ME.aianalysis.AnonymizedRecords.COMPANION).id
            .exerciseProcessingDone("The patient is very healthy.")
            .commands

          participant6.ledger_api.javaapi.commands
            .submit(Seq(processor), processingDone, optTimeout = syncTimeout)
            .discard[TransactionTree]

          val resultId = registryLookup(ME.aianalysis.AnalysisResult.COMPANION)
          val recordedResult = registryLookup(ME.aianalysis.PendingAnalysis.COMPANION).id
            .exerciseRecordResult(resultId.id)
            .commands

          participant5.ledger_api.javaapi.commands
            .submit(Seq(registry), recordedResult, optTimeout = syncTimeout)
            .discard[TransactionTree]
        },
      ),
    )
  }
}

object ReferenceDemoScript {

  private val SequencerBanking = "sequencerBanking"
  private val SequencerMedical = "sequencerMedical"

  private def computeMaxWaitForPruning = {
    val defaultDynamicDomainParameters = DynamicDomainParameters.initialValues(
      topologyChangeDelay = NonNegativeFiniteDuration.tryOfMillis(250),
      protocolVersion = ProtocolVersion.latest,
    )
    val mediatorReactionTimeout = defaultDynamicDomainParameters.mediatorReactionTimeout
    val confirmationResponseTimeout = defaultDynamicDomainParameters.confirmationResponseTimeout

    mediatorReactionTimeout.unwrap.plus(confirmationResponseTimeout.unwrap)
  }

  def startup(adjustPath: Boolean, testScript: Boolean)(implicit
      consoleEnvironment: ConsoleEnvironment
  ): Unit = {

    def getSequencer(str: String): SequencerReference =
      consoleEnvironment.sequencers.all
        .find(_.name == str)
        .getOrElse(sys.error(s"can not find domain named ${str}"))

    val bankingSequencers = consoleEnvironment.sequencers.all.filter(_.name == SequencerBanking)
    val bankingMediators = consoleEnvironment.mediators.all.filter(_.name == "mediatorBanking")
    val bankingDomainId = ConsoleMacros.bootstrap.domain(
      domainName = SequencerBanking,
      sequencers = bankingSequencers,
      mediators = bankingMediators,
      domainOwners = bankingSequencers ++ bankingMediators,
      staticDomainParameters = StaticDomainParameters.defaultsWithoutKMS(ProtocolVersion.latest),
    )
    val medicalSequencers = consoleEnvironment.sequencers.all.filter(_.name == SequencerMedical)
    val medicalMediators = consoleEnvironment.mediators.all.filter(_.name == "mediatorMedical")
    val medicalDomainId = ConsoleMacros.bootstrap.domain(
      domainName = SequencerMedical,
      sequencers = medicalSequencers,
      mediators = medicalMediators,
      domainOwners = medicalSequencers ++ medicalMediators,
      staticDomainParameters = StaticDomainParameters.defaultsWithoutKMS(ProtocolVersion.latest),
    )

    val banking = getSequencer(SequencerBanking)
    val medical = getSequencer(SequencerMedical)

    // determine where the assets are
    val location = sys.env.getOrElse("DEMO_ROOT", "demo")
    val noPhoneHome = sys.env.keys.exists(_ == "NO_PHONE_HOME")

    // start all nodes before starting the ui (the ui requires this)
    val (maxWaitForPruning, bankingConnection, medicalConnection) = (
      ReferenceDemoScript.computeMaxWaitForPruning,
      banking.sequencerConnection,
      medical.sequencerConnection,
    )
    val loggerFactory = consoleEnvironment.environment.loggerFactory

    // update domain parameters
    banking.topology.domain_parameters.propose_update(
      bankingDomainId,
      _.update(reconciliationInterval = config.PositiveDurationSeconds.ofSeconds(1)),
    )
    medical.topology.domain_parameters.propose_update(
      medicalDomainId,
      _.update(reconciliationInterval = config.PositiveDurationSeconds.ofSeconds(1)),
    )

    val script = new ReferenceDemoScript(
      consoleEnvironment.participants.all,
      bankingConnection,
      medicalConnection,
      location,
      maxWaitForPruning,
      editionSupportsPruning = consoleEnvironment.environment.isEnterprise,
      darPath =
        if (adjustPath) Some("./community/demo/target/scala-2.13/resource_managed/main") else None,
      additionalChecks = testScript,
      loggerFactory = loggerFactory,
    )(consoleEnvironment.environment.executionContext)
    if (testScript) {
      script.run()
      println("The last emperor is always the worst.")
    } else {
      if (!noPhoneHome) {
        Notify.send()
      }
      val runner = new DemoRunner(new DemoUI(script, loggerFactory))
      runner.startBackground()
    }
    ()
  }
}
