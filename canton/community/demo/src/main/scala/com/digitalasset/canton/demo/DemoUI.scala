// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.demo

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v2.participant_offset.ParticipantOffset
import com.daml.ledger.api.v2.transaction_filter.{Filters, TransactionFilter}
import com.daml.ledger.api.v2.update_service.GetUpdatesResponse.Update
import com.daml.ledger.api.v2.update_service.{
  GetUpdatesRequest,
  GetUpdatesResponse,
  UpdateServiceGrpc,
}
import com.daml.ledger.api.v2.value.{Record, Value}
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.ClientChannelBuilder
import com.digitalasset.canton.topology.{Identifier, UniqueIdentifier}
import com.digitalasset.canton.tracing.{NoTracing, TraceContext}
import com.digitalasset.canton.util.LoggerUtil.clue
import com.digitalasset.canton.util.PekkoUtil
import io.grpc.stub.StreamObserver
import io.grpc.{ManagedChannel, Status}
import org.apache.pekko.actor.ActorSystem
import scalafx.animation.PauseTransition
import scalafx.application.{JFXApp3, Platform}
import scalafx.beans.property.StringProperty
import scalafx.collections.ObservableBuffer
import scalafx.concurrent.{ScheduledService, Task}
import scalafx.geometry.{Insets, Pos}
import scalafx.scene.Scene
import scalafx.scene.control.Alert.AlertType
import scalafx.scene.control.*
import scalafx.scene.image.{Image, ImageView}
import scalafx.scene.layout.Priority.Always
import scalafx.scene.layout.*
import scalafx.scene.text.Font
import scalafx.util.Duration as FXDuration

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.{Executor, TimeUnit}
import scala.annotation.{nowarn, tailrec}
import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContextExecutor, blocking}
import scala.util.{Failure, Success, Try}

sealed trait Step

object Step {
  final case class Action(
      description: String,
      api: String,
      command: String,
      action: () => Unit,
  ) extends Step
  object Noop extends Step
}
import com.digitalasset.canton.demo.Step.*

trait BaseScript extends NamedLogging {

  def steps: Seq[Step]
  def parties(): Seq[(String, ParticipantReference)]
  def subscriptions(): Map[String, ParticipantOffset]
  def maxImage: Int
  def imagePath: String

  def run(): Unit = {
    import TraceContext.Implicits.Empty.*
    logger.debug("Running script")
    steps.foreach {
      case Noop => ()
      case x: Action =>
        clue(s"running step ${x.description}")(
          x.action()
        )
    }
    logger.debug("Finished running script")
  }
}

private[demo] final case class DarData(filenameS: String, hashS: String) {
  val filename = new StringProperty(this, "fileName", filenameS)
}

private[demo] final case class MetaInfo(darData: Seq[DarData], hosted: String, connections: String)

class ParticipantTab(
    party: String,
    participant: ParticipantReference,
    isClosing: => Boolean,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    executor: Executor
) extends NamedLogging
    with NoTracing {

  private case class TxData(
      txIdO: String,
      cIdO: String,
      timestampO: String,
      activeO: Boolean,
      templateIdO: String,
      packageIdO: String,
      argumentsO: String,
      domainId: String,
  ) {
    val state = new StringProperty(this, "state", if (activeO) "Active" else "Archived")
    val templateId = new StringProperty(this, "templateId", templateIdO)
    val arguments = new StringProperty(this, "arguments", argumentsO)
    val domain = new StringProperty(this, "domain", domainId)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private class ActiveTableColumn(label: String) extends TableColumn[TxData, String](label) {
    cellFactory = new javafx.util.Callback[
      javafx.scene.control.TableColumn[TxData, String],
      javafx.scene.control.TableCell[TxData, String],
    ] {
      def call(
          tv: javafx.scene.control.TableColumn[TxData, String]
      ): javafx.scene.control.TableCell[TxData, String] = {
        new javafx.scene.control.TableCell[TxData, String] {
          val sfxThis = new TableCell(this)
          override def updateItem(item: String, empty: Boolean): Unit = {
            super.updateItem(item, empty)
            setText(if (empty) "" else item)
            val style = if (!empty && item.equals("Archived")) {
              "-fx-background-color:lightcoral"
            } else if (!empty && item.equals("Active")) {
              "-fx-background-color:lightgreen"
            } else {
              ""
            }
            Option[javafx.scene.control.TableRow[_]](getTableRow).foreach(x => x.setStyle(style))
          }
        }
      }
    }
  }

  private val uid = participant.id.uid

  private def getString(strO: Option[String]) = strO.getOrElse("invalid")
  private def formatHash(str: String) = str.substring(0, 16)
  private def renderSum(value: Value.Sum): String = value match {
    case v: Value.Sum.Record => renderRecord(v.value)
    case v: Value.Sum.Party =>
      val uid = UniqueIdentifier.tryFromProtoPrimitive(v.value)
      uid.id.unwrap + "::" + formatHash(uid.namespace.fingerprint.unwrap)
    case v: Value.Sum.List => "{" + v.value.elements.map(x => renderSum(x.sum)).mkString(",") + "}"
    case v => v.value.toString
  }

  private def renderRecord(record: Record): String = {
    val stringify = record.fields
      .flatMap(x => x.value.map(v => (x.label, v.sum)).toList)
      .map { case (k, v) =>
        s"$k=${renderSum(v)}"
      }
    "{" + stringify.mkString(", ") + "}"
  }

  case class SubscriptionState(
      running: Boolean,
      flushing: Boolean,
      channel: Option[ManagedChannel],
      resubscribe: Boolean,
      subscribeFrom: ParticipantOffset,
  )

  private val currentChannel = new AtomicReference[SubscriptionState](
    SubscriptionState(
      running = true,
      flushing = false,
      channel = None,
      resubscribe = false,
      subscribeFrom = ParticipantTab.LedgerBegin,
    )
  )

  private def terminateChannel(channel: ManagedChannel): Unit = {
    logger.debug(s"Closing channel for ${uid.id}")
    channel.shutdownNow()
    val _ = channel.awaitTermination(3, TimeUnit.SECONDS)
  }

  def shutdown(): Unit = {
    val _ = currentChannel.getAndUpdate(_.copy(running = false))
    flushSubscriptionState()
  }

  private def flushSubscriptionState(): Unit = {
    val current = currentChannel.getAndUpdate(_.copy(flushing = true))
    val checkAgain: Boolean =
      try {
        // only flush if we aren't flushing right now
        if (!current.flushing) {
          if (!current.running) {
            // terminate if necessary
            currentChannel.updateAndGet(_.copy(resubscribe = false, channel = None))
            current.channel.foreach(terminateChannel)
            false
          } else {
            (current.channel, current.resubscribe) match {
              // close channel if we need to re-subscribe
              case (Some(channel), true) =>
                terminateChannel(channel)
                if (txdata.nonEmpty) {
                  logger.info(s"Cleared data for $party")
                  txdata.clear()
                }
                currentChannel.updateAndGet(_.copy(channel = None))
                // don't resubscribe immediately, give the index server some time to come back.
                val timer = new PauseTransition(FXDuration(500))
                timer.onFinished = { _ =>
                  flushSubscriptionState()
                }
                // Re-subscribe shortly
                timer.play()
                false
              // re-subscribe
              case (None, true) =>
                subscribeToChannel(current.subscribeFrom)
                true
              case (Some(_), false) => // do nothing
                true
              case (None, false) => // do nothing
                true
            }
          }
        } else {
          false
        }
      } catch {
        case ex: Exception =>
          logger.error(s"Flushing subscription state $current failed with exception", ex)
          false
      } finally {
        val _ = currentChannel.updateAndGet(_.copy(flushing = false))
      }
    val completed = currentChannel.get()
    // reflush if a re-subscription got scheduled while we were changing our subscription state
    if (checkAgain && completed.resubscribe && completed.running && !completed.flushing) {
      flushSubscriptionState()
    }
  }

  def reStart(newOffsetO: Option[ParticipantOffset] = None): Unit = {
    val cur = currentChannel.updateAndGet(x =>
      x.copy(resubscribe = true, subscribeFrom = newOffsetO.getOrElse(x.subscribeFrom))
    )
    logger.debug(s"Triggering resubscribe at ${cur.subscribeFrom}")
    Platform.runLater {
      flushSubscriptionState()
    }
  }

  private def subscribeToChannel(offset: ParticipantOffset): Unit = {
    val channel =
      ClientChannelBuilder.createChannelToTrustedServer(participant.config.clientLedgerApi)
    logger.debug(s"Subscribing ${participant.name} at ${offset.toString}")
    val current = currentChannel.getAndUpdate { cur =>
      // store channel and set subscribed offset to None unless it has changed in the meantime

      cur.copy(
        channel = Some(channel),
        resubscribe = cur.subscribeFrom != offset,
      )
    }
    require(
      current.channel.isEmpty && current.resubscribe,
      s"re-subscribing with inconsistent state??? $current",
    )
    val updatesService = UpdateServiceGrpc.stub(channel)
    val obs = new StreamObserver[GetUpdatesResponse]() {
      override def onNext(value: GetUpdatesResponse): Unit = {
        value.update match {
          case Update.Transaction(transaction) =>
            import com.digitalasset.canton.protocol.LfContractId
            val coids = transaction.events
              .flatMap(_.event.created.toList)
              .map(_.contractId)
              .map(LfContractId.assertFromString)
              .toSet
            val coidMap =
              if (isClosing) Map.empty[LfContractId, String]
              else
                participant.ledger_api.state.acs
                  .of_all()
                  .flatMap(_.entry.activeContract)
                  .map(c => c.domainId -> c.createdEvent)
                  .collect { case (domainId, Some(createdEvent)) =>
                    LfContractId.assertFromString(createdEvent.contractId) -> domainId
                  }
                  .filter { case (contractId, createdEventO) =>
                    coids.contains(contractId)
                  }
                  .toMap
            transaction.events.foreach(event => {
              event.event.created.foreach(ce =>
                Platform.runLater {
                  txdata.append(
                    TxData(
                      transaction.updateId,
                      ce.contractId,
                      getString(transaction.effectiveAt.map(_.seconds.toString)),
                      activeO = true,
                      getString(ce.templateId.map(x => x.moduleName + "." + x.entityName)),
                      getString(ce.templateId.map(x => formatHash(x.packageId))),
                      getString(ce.createArguments.map(r => renderRecord(r))),
                      coidMap.getOrElse(LfContractId.assertFromString(ce.contractId), ""),
                    )
                  )
                }
              )
              event.event.archived.foreach(ae =>
                Platform.runLater {
                  blocking(txdata.synchronized {
                    val idx = txdata.indexWhere(x => x.cIdO == ae.contractId)
                    if (idx > -1) {
                      val item = txdata.get(idx)
                      txdata.update(idx, item.copy(activeO = false))
                    }
                  })
                }
              )
            })
          case _ => ()
        }
      }

      @nowarn("msg=match may not be exhaustive")
      override def onError(t: Throwable): Unit = {
        t match {
          case se: io.grpc.StatusRuntimeException
              if se.getStatus.getCode == Status.UNAVAILABLE.getCode =>
            reStart()
          case se: io.grpc.StatusRuntimeException
              if se.getStatus.getCode == Status.NOT_FOUND.getCode =>
            val message = se.getStatus.getDescription
            logger.info(s"Attempt to access pruned participant ledger state: ${message}")
            val errorPattern =
              "Transactions request from ([0-9a-fA-F]*) to ([0-9a-fA-F]*) precedes pruned offset ([0-9a-fA-F]+)".r
            Try {
              val errorPattern(_badStartHexOffset, _endHexOffset, hexPrunedOffset) = message
              logger.info(s"Identified pruning offset position as ${hexPrunedOffset}")
              new ParticipantOffset().withAbsolute(hexPrunedOffset)
            } match {
              case Success(prunedOffset) =>
                logger.info(s"Resubscribing from offset $prunedOffset instead")
                reStart(Some(prunedOffset))
              case Failure(t) =>
                logger.error("Out-of-range error does not match pruning error", t)

            }
          case _ =>
            logger.error("stream error", t)
        }
      }
      override def onCompleted(): Unit = reStart()
    }

    val partyId = UniqueIdentifier(Identifier.tryCreate(party), uid.namespace).toProtoPrimitive
    val req = new GetUpdatesRequest(
      beginExclusive = Some(offset),
      filter = Some(TransactionFilter(filtersByParty = Map(partyId -> Filters()))),
      verbose = true,
    )
    updatesService.getUpdates(req, obs)
  }

  private val txdata = ObservableBuffer[TxData]()

  val tab = new Tab() {

    text = party + "/" + uid.id.unwrap

    private val colState = new ActiveTableColumn("State") {
      cellValueFactory = { _.value.state }
      prefWidth = 60.0
    }
    private val coltemplateId = new TableColumn[TxData, String]("TemplateId") {
      cellValueFactory = { _.value.templateId }
      prefWidth = 100.0
    }
    private val colDomainId = new TableColumn[TxData, String]("Domain") {
      cellValueFactory = { _.value.domain }
      prefWidth = 100.0
    }
    private val colArgs = new TableColumn[TxData, String]("Arguments") {
      cellValueFactory = { _.value.arguments }
    }

    private val contracts = new TableView[TxData](txdata) {
      editable = false
      hgrow = Always
      vgrow = Always
      (columns ++= List(colState, coltemplateId, colDomainId, colArgs)).discard
    }
    // resize the argument column automatically such that it fills out the entire rest of the table while
    // still permitting the user to resize the columns himself (so don't set a resize policy on the tableview)
    colArgs
      .prefWidthProperty()
      .bind(
        contracts
          .widthProperty()
          .subtract(colDomainId.widthProperty())
          .subtract(colState.widthProperty())
          .subtract(coltemplateId.widthProperty())
          .subtract(2)
      )

    private val dars = ObservableBuffer[DarData]()
    private val domains = new Label("")
    private val parties = new Label("")
    private val rendered = new AtomicReference[Option[MetaInfo]](None)

    private val service = new ScheduledService[MetaInfo](() => { () =>
      {
        // append dars
        if (isClosing) {
          MetaInfo(Seq(), "", "")
        } else {
          val currentDars =
            participant.dars.list().map(x => DarData(x.name, x.hash.substring(0, 16)))
          val hosted = participant.parties
            .hosted()
            .map(_.party.uid.id.unwrap)
            .toSet
            .mkString("\n")
          val connected =
            participant.domains.list_connected().map(_.domainAlias.unwrap).mkString("\n")
          MetaInfo(currentDars, hosted, connected)
        }
      }
    }) {
      value.onChange { (obs, _, _) =>
        val cur = rendered.get()
        Option(obs.value) match {
          // no change? skip!
          case `cur` => ()
          case Some(updated) =>
            dars.clear()
            dars.appendAll(updated.darData)
            parties.setText(updated.hosted)
            domains.setText(updated.connections)
            rendered.set(Some(updated))
          case None =>
            () // ignore invalidations (if the observable value is "unknown" / invalidated, we'll get null here)
        }
      }.discard
      period = FXDuration(500)
    }
    service.start()

    // Note: 1,000 ms = 1 sec
    private val timer = new PauseTransition()
    content = new GridPane {
      add(
        new VBox {
          fillWidth = true
          padding = Insets(15, 15, 15, 15)
          spacing = 15
          children = Seq(
            new Label("Private Contract Store") {
              font = new Font(14)
            },
            contracts,
          )
        },
        0,
        0,
      )
      add(
        new VBox {
          spacing = 15
          padding = Insets(15, 15, 15, 15)
          children = Seq(
            new Label("Daml Archives (DARs)") {
              font = new Font(14)
            },
            new TableView[DarData](dars) {
              editable = false
              columnResizePolicy = TableView.ConstrainedResizePolicy
              (columns ++= List(
                new TableColumn[DarData, String]() {
                  text = "Archive Name"
                  cellValueFactory = { _.value.filename }
                  hgrow = Always
                  minWidth = 80
                }
              )).discard
            },
            new Label("Participant Settings") {
              font = new Font(14)
            },
            new GridPane() {
              padding = Insets(15, 15, 15, 15)
              hgap = 10
              vgap = 10
              add(new Label("Ledger-Api"), 0, 0)
              add(
                new Label({
                  val c = participant.config.clientLedgerApi
                  s"${c.address}:${c.port}"
                }),
                1,
                0,
              )
              add(new Label("Namespace"), 0, 1)
              add(new Label(uid.namespace.fingerprint.unwrap.substring(0, 16) + "..."), 1, 1)
              add(new Label("Parties"), 0, 2)
              add(parties, 1, 2)
              add(new Label("Domains"), 0, 3)
              add(domains, 1, 3)
            },
          )
        },
        1,
        0,
      )
      columnConstraints = List(new ColumnConstraints() {
        fillWidth = true
        hgrow = Always
      })
      rowConstraints = List(new RowConstraints() {
        fillHeight = true
        vgrow = Always
      })
    }
  }
}

object ParticipantTab {
  val LedgerBegin: ParticipantOffset = ParticipantOffset(
    ParticipantOffset.Value.Boundary(
      ParticipantOffset.ParticipantBoundary.PARTICIPANT_BOUNDARY_BEGIN
    )
  )
}

class Controls(
    title: String,
    imagePath: String,
    backwards: () => Unit,
    forward: () => Unit,
    run: () => Unit,
    offset: Int,
) {

  val view: ImageView = new ImageView() {
    preserveRatio = true
    cache = true
    smooth = true
    alignmentInParent = Pos.Center
    vgrow = Always
    hgrow = Always
  }

  private val labelNextDescription = new Label("")
  private val labelNextCommand = new Label("") {
    style = "-fx-font-family: monospace"
  }

  val backButton: Button = new Button("Previous") {
    onAction = _ => backwards()
    graphic = new ImageView(new Image(imagePath + "left.png")) {
      fitWidth = 30
      preserveRatio = true
      cache = true
      smooth = true
    }
  }

  private val nextCommand = new VBox() {
    spacing = 5
    margin = Insets(0, 5, 0, 5)
    children = List(
      labelNextDescription,
      labelNextCommand,
    )
  }
  val nextButton: Button = new Button("Next") {
    onAction = _ => forward()
    graphic = new ImageView(new Image(imagePath + "right.png")) {
      fitWidth = 30
      preserveRatio = true
      cache = true
      smooth = true
    }
  }

  val runButton: Button = new Button("Run") {
    onAction = _ => run()
    disable = true
  }

  private val controls = new BorderPane() {
    hgrow = Always
    // style = "-fx-border-color: red"
    padding = Insets(5, 25, 5, 25)
    left = backButton
    right = new HBox() {
      spacing = 5
      alignment = Pos.Center
      children = Seq(runButton, nextButton)
    }
    center = nextCommand
  }

  private val pgrid = new GridPane {
    // style = "-fx-border-color: blue"
    columnConstraints = List(new ColumnConstraints() {
      fillWidth = true
      hgrow = Always
    })
    rowConstraints = List(new RowConstraints() {
      fillHeight = true
      vgrow = Always
    })
    add(view, 0, 0)
    add(controls, 0, 1)
  }

  val tab = new Tab() {
    text = title
    content = pgrid
  }

  def clear(): Unit = {
    labelNextDescription.setText("")
    labelNextCommand.setText("")
    runButton.setDisable(true)
  }

  def setAction(x: Action, canRun: Boolean): Unit = {
    if (canRun) {
      labelNextDescription.setText("Next command: " + x.description)
      labelNextCommand.setText(x.api + "> " + x.command)
    } else {
      labelNextDescription.setText("Executed command: " + x.description)
      labelNextCommand.setText(x.api + "> " + x.command)
    }
    runButton.setDisable(!canRun)
  }

  def loadImage(idx: Int): Unit = {
    val image = new Image(imagePath + s"demo${idx * 2 + offset}.png")
    view.setImage(image)
  }

}

class DemoUI(script: BaseScript, val loggerFactory: NamedLoggerFactory)
    extends JFXApp3
    with NamedLogging
    with NoTracing {

  private val closeMe = new AtomicInteger(0)

  def backgroundCloseStage(): Unit = {
    val _ = closeMe.updateAndGet(current => if (current == 0) 1 else current)
  }

  private def isClosing: Boolean = closeMe.get() > 0

  private lazy val presentationView =
    new Controls(
      "Presentation",
      script.imagePath,
      () => prevImage(),
      () => nextImage(),
      () => runNext(),
      0,
    )
  private lazy val notesView =
    new Controls(
      "Notes",
      script.imagePath,
      () => prevImage(),
      () => nextImage(),
      () => runNext(),
      1,
    )

  private lazy val controls = Seq(presentationView, notesView)

  private val viewIndex = new AtomicInteger(0)
  private val actionIndex = new AtomicInteger(0)
  private lazy val tabPane = {
    val tmp = new TabPane() {
      prefWidth = 1200
      vgrow = Always
      hgrow = Always
    }
    tmp.setStyle("-fx-font-family: 'sans-serif'")
    tmp.getTabs.add(presentationView.tab)
    tmp.getTabs.add(notesView.tab)
    tmp
  }

  private implicit val executionContext: ExecutionContextExecutor =
    Threading.newExecutionContext(
      "demo-ui",
      noTracingLogger,
    )
  private implicit val actorSystem: ActorSystem = PekkoUtil.createActorSystem("demo-ui")
  private implicit val sequencerPool: ExecutionSequencerFactory =
    PekkoUtil.createExecutionSequencerFactory("demo-ui", noTracingLogger)(actorSystem)

  private val participantTabs = mutable.HashMap[String, ParticipantTab]()

  private def addNewTabsIfNecessary(): Unit = {
    script.parties().foreach {
      case (party, participant) if !participantTabs.contains(party) =>
        val pt =
          new ParticipantTab(party, participant, isClosing, loggerFactory.append("party", party))
        script.subscriptions().get(party).foreach(offset => pt.reStart(Some(offset)))
        tabPane.getTabs.add(pt.tab)
        participantTabs.put(party, pt).discard[Option[ParticipantTab]]
      case _ => ()
    }
  }

  private def fitViews(x: ImageView): Unit = {
    x.fitWidth.bind(tabPane.widthProperty().multiply(0.95))
    x.fitHeight.bind(tabPane.heightProperty().multiply(0.95).add(-100))
  }

  import javafx.stage.Screen

  private lazy val dwidth = Screen.getPrimary.getBounds.getWidth
  private lazy val dheight = Screen.getPrimary.getBounds.getHeight

  private lazy val adjustedWidth = Screen.getPrimary.getBounds.getWidth * 0.8
  private lazy val adjustedHeight = Screen.getPrimary.getBounds.getHeight * 0.8

  override def start(): Unit = {

    addNewTabsIfNecessary()
    loadImage(0)
    updateButton()

    stage = new JFXApp3.PrimaryStage {
      icons.add(new Image(script.imagePath + "/canton-logo-small.png"))
      title = "Canton Demo"
      scene = new Scene {
        root = tabPane
      }
      width = adjustedWidth
      height = adjustedHeight
    }

    // ugly shutdown method. we can't directly shutdown the ui from ammonite
    // as both ammonite and java-fx assume to be running as the "main thing".
    // therefore, we signal here to the app that we want the main stage closed
    // and pick it up in the background thread
    val timer = new PauseTransition(FXDuration(100))
    timer.onFinished = { _ =>
      if (closeMe.compareAndSet(1, 2)) {
        println("Closing stage")
        stage.close()
      } else if (closeMe.get == 0) {
        timer.playFromStart()
      }
    }
    timer.play()

    fitViews(presentationView.view)
    fitViews(notesView.view)

  }

  private def loadImage(idx: Int): Unit = {
    viewIndex.set(idx)
    controls.foreach(_.loadImage(idx))
  }

  @tailrec
  private def executeActionUntil(idx: Int): Unit = {
    if (actionIndex.get() < idx && actionIndex.get() < script.steps.length) {
      val actionIdx = actionIndex.getAndUpdate(_ + 1)
      script.steps(actionIdx) match {
        case Noop => ()
        case x: Action =>
          try {
            val pre = script.subscriptions()
            x.action()
            script
              .subscriptions()
              .filterNot { case (party, post) =>
                pre.get(party).contains(post)
              }
              .foreach { case (party, offset) =>
                Platform.runLater {
                  // subscribe participant tabs once the parties are ready or the offsets changed
                  participantTabs.get(party).foreach(_.reStart(Some(offset)))
                }
              }
          } catch {
            case ex: Exception =>
              showError(x.command, ex)
          }
      }
      executeActionUntil(idx)
    }
  }

  private def showError(cmd: String, ex: Exception): Unit = {
    logger.error(s"Command $cmd failed with an exception.", ex)
    val _ = new Alert(AlertType.Error) {
      initOwner(stage)
      title = "Error"
      headerText = "The action failed due to an error."
      contentText = s"Failed to run $cmd due to ${ex.getMessage}"
    }.showAndWait()
  }

  private def nextImage(): Unit = {
    val idx = viewIndex.updateAndGet(x => Math.min(script.maxImage, x + 1))
    // run all commands up to now
    executeInBackground(idx, () => loadImage(idx))
  }

  private def executeInBackground(idx: Int, post: () => Unit) = {
    controls.foreach { ctrl =>
      ctrl.nextButton.setDisable(true)
      ctrl.backButton.setDisable(true)
      ctrl.runButton.setDisable(true)
    }
    val task = new Task[Unit](() => {
      executeActionUntil(idx)
    }) {
      onSucceeded = _ => {
        addNewTabsIfNecessary()
        post()
        updateButton()
      }
    }
    val thread = new Thread(task)
    thread.setName(s"ui-action-${idx}")
    thread.setDaemon(true)
    thread.start()
  }

  private def runNext(): Unit = {
    val idx = viewIndex.get()
    executeInBackground(idx + 1, () => ())
  }

  private def updateButton(): Unit = {
    // load command
    def clear(): Unit = controls.foreach(_.clear())
    val idx = viewIndex.get()
    if (idx < script.steps.length) {
      script.steps(idx) match {
        case Noop =>
          clear()
        case x: Action =>
          controls.foreach(c => c.setAction(x, idx >= actionIndex.get()))
      }
    } else {
      clear()
    }
    if (idx == 0) {
      controls.foreach(_.nextButton.setDisable(false))
      controls.foreach(_.backButton.setDisable(true))
    } else if (idx == script.maxImage) {
      controls.foreach(_.nextButton.setDisable(true))
      controls.foreach(_.backButton.setDisable(false))
    } else {
      controls.foreach(_.nextButton.setDisable(false))
      controls.foreach(_.backButton.setDisable(false))
    }
  }

  private def prevImage(): Unit = {
    loadImage(viewIndex.updateAndGet(x => Math.max(0, x - 1)))
    updateButton()
  }

  def close(): Unit = {
    import scala.concurrent.duration.*
    participantTabs.values.foreach(_.shutdown())
    sequencerPool.close()
    val _ = Await.result(actorSystem.terminate(), 2.seconds)
  }

  def advanceTo(idx: Int): Unit = {
    if (viewIndex.get() < idx && viewIndex.get() < script.maxImage) {
      nextImage()
      advanceTo(idx)
    }
  }
}
