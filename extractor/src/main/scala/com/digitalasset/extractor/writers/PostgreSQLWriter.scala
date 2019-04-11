// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor.writers

import com.digitalasset.daml.lf.iface.reader.{InterfaceType, Interface}
import com.digitalasset.extractor.config.ExtractorConfig
import com.digitalasset.extractor.ledger.LedgerReader
import com.digitalasset.extractor.ledger.LedgerReader.PackageStore
import com.digitalasset.extractor.ledger.types._
import com.digitalasset.extractor.logging.Logging
import com.digitalasset.extractor.targets.PostgreSQLTarget
import com.digitalasset.extractor.Types._
import com.digitalasset.extractor.writers.postgresql._
import com.digitalasset.extractor.writers.postgresql.DataFormatState._
import com.digitalasset.extractor.writers.Writer._
import doobie._
import doobie.implicits._
import doobie.free.connection
import cats.effect.{ContextShift, IO}
import cats.implicits._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, ExecutionContext}
import scalaz._
import Scalaz._
import com.digitalasset.daml.lf.iface.Record

class PostgreSQLWriter(config: ExtractorConfig, target: PostgreSQLTarget, ledgerId: String)
    extends Writer
    with Logging {

  // Uncomment this to have queries logged
  // implicit val lh = doobie.util.log.LogHandler.jdkLogHandler

  import postgresql.Queries._

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  private val multiTableFormat = new MultiTableDataFormat(
    schemaPerPackage = target.schemaPerPackage,
    mergeIdentical = target.mergeIdentical,
    stripPrefix = target.stripPrefix
  )
  private val singleTableFormat = new SingleTableDataFormat()

  private val useSingleTableFormat = List("single-table", "combined").contains(target.outputFormat)
  private val useMultiTableFormat = List("multi-table", "combined").contains(target.outputFormat)

  @volatile private var multiTableState = MultiTableState(Map.empty, Map.empty)

  @volatile private var witnessedPackages: Set[String] = Set.empty

  // A transactor that gets connections from java.sql.DriverManager
  private val xa = Transactor.fromDriverManager[IO](
    "org.postgresql.Driver", // driver classname
    target.connectUrl, // connect URL (driver-specific)
    target.user,
    target.password
  )

  def init(): Future[Unit] = {
    log.info("PostgreSQLWriter initializing...")

    val io = for {
      _ <- StateHandler.init()
      previousState <- StateHandler.retrieveStatus
      io <- previousState.fold {
        // There were no state, start with a clean slate
        val drop = dropTransactionsTable.update.run
        val createTrans = createTransactionsTable.update.run
        val indexTrans = transactionsIndex.update.run
        val createExercise = createExerciseTable.update.run
        val singleInit =
          if (useSingleTableFormat)
            singleTableFormat.init()
          else
            connection.pure(())
        val multiInit =
          if (useMultiTableFormat)
            multiTableFormat.init()
          else
            connection.pure(())

        drop *> createTrans *> indexTrans *> createExercise *> singleInit *> multiInit
      } { statusOrRetrieveError =>
        val statusOrError = for {
          prevStatus <- statusOrRetrieveError
          _ <- StateHandler.validateArgumentsAgainstStatus(prevStatus, ledgerId, config, target)
        } yield prevStatus

        statusOrError.fold(
          e => connection.raiseError(DataIntegrityError(e)), { status =>
            multiTableState = status.multiTableState
            witnessedPackages = status.witnessedPackages

            connection.pure(())
          }
        )
      }
    } yield io

    io.transact(xa).unsafeToFuture()
  }

  def handlePackages(packageStore: LedgerReader.PackageStore): Future[Unit] = {
    val (newMultiTableState, mtQueries) =
      if (useMultiTableFormat)
        handlePackagesWithMultiTable(packageStore)
      else
        (multiTableState, connection.pure(()))

    val updatedWitnessedPackages = packageStore.packages.keySet

    val saveStatus = StateHandler.saveStatus(
      ledgerId,
      config,
      target,
      newMultiTableState,
      updatedWitnessedPackages)

    (mtQueries *> saveStatus)
      .transact(xa)
      .map { _ =>
        witnessedPackages = updatedWitnessedPackages
        multiTableState = newMultiTableState

        log.trace(s"Multi-table state: ${multiTableState}")
        log.trace(s"Witnessed packages: ${witnessedPackages}")
      }
      .unsafeToFuture()
  }

  private def handlePackagesWithMultiTable(
      newPackageStore: PackageStore): (MultiTableState, ConnectionIO[Unit]) = {
    val newPackages: Map[String, Interface] = newPackageStore.packages.filterNot {
      case (key, _) =>
        witnessedPackages.contains(key)
    }

    val (mtStateWithSchemas, mtSchemaQueries) = newPackages.keys
      .foldLeft((multiTableState, connection.pure(()))) {
        case ((state, queries), packageId) =>
          val (updatedState, thisQueries) = multiTableFormat.handlePackageId(state, packageId)

          (updatedState, queries *> thisQueries)
      }

    val templateDecls: Map[Identifier, Record.FWT] = newPackages.flatMap {
      case (packageId, interface) =>
        interface.typeDecls.collect {
          case (id, InterfaceType.Template(r, _)) =>
            Identifier(packageId, id.qualifiedName) -> r
        }
    }

    templateDecls
      .foldLeft((mtStateWithSchemas, mtSchemaQueries)) {
        case ((state, queries), params) =>
          val (updatedState, thisQueries) =
            multiTableFormat.handleTemplate(state, newPackageStore, params)

          (updatedState, queries *> thisQueries)
      }
  }

  def handleTransaction(transaction: TransactionTree): Future[RefreshPackages \/ Unit] = {
    log.trace(s"Handling transaction:")
    log.trace(com.digitalasset.extractor.pformat(transaction))

    val insertIO = insertTransaction(transaction).update.run.void

    val createdEvents: List[CreatedEvent] = transaction.events.values.collect {
      case e @ CreatedEvent(_, _, _, _, _) => e
    }(scala.collection.breakOut)

    val exercisedEvents: List[ExercisedEvent] = transaction.events.values.collect {
      case e @ ExercisedEvent(_, _, _, _, _, _, _, _, _, _) => e
    }(scala.collection.breakOut)

    log.trace(s"Create events:")
    createdEvents.foreach(e => log.trace(com.digitalasset.extractor.pformat(e)))
    log.trace(s"Exercise events:")
    exercisedEvents.foreach(e => log.trace(com.digitalasset.extractor.pformat(e)))

    (for {
      archiveIOsMulti <- if (useMultiTableFormat)
        exercisedEvents.traverseU(
          multiTableFormat.handleExercisedEvent(multiTableState, transaction, _)
        )
      else
        List.empty[ConnectionIO[Unit]].right
      createIOsMulti <- if (useMultiTableFormat)
        createdEvents.traverseU(
          multiTableFormat.handleCreatedEvent(multiTableState, transaction, _)
        )
      else
        List.empty[ConnectionIO[Unit]].right
      archiveIOsSingle <- if (useSingleTableFormat)
        exercisedEvents.traverseU(
          singleTableFormat.handleExercisedEvent(SingleTableState, transaction, _)
        )
      else
        List.empty[ConnectionIO[Unit]].right
      createIOsSingle <- if (useSingleTableFormat)
        createdEvents.traverseU(
          singleTableFormat.handleCreatedEvent(SingleTableState, transaction, _)
        )
      else
        List.empty[ConnectionIO[Unit]].right
    } yield {
      val sqlTransaction =
        (archiveIOsMulti ++ createIOsMulti ++ archiveIOsSingle ++ createIOsSingle)
          .foldLeft(insertIO)(_ *> _)

      sqlTransaction.transact(xa).unsafeToFuture()
    }).sequence
  }

  def getLastOffset: Future[Option[String]] = {
    lastOffset.query[String].option.transact(xa).unsafeToFuture()
  }
}
