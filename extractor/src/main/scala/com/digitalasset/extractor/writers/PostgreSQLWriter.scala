// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.extractor.writers

import com.daml.lf.iface.{Interface, InterfaceType}
import com.daml.extractor.config.ExtractorConfig
import com.daml.ledger.service.LedgerReader
import com.daml.ledger.service.LedgerReader.PackageStore
import com.daml.extractor.ledger.types._
import com.daml.extractor.targets.PostgreSQLTarget
import com.daml.extractor.Types._
import com.daml.extractor.writers.postgresql._
import com.daml.extractor.writers.postgresql.DataFormatState._
import com.daml.extractor.writers.Writer._
import doobie._
import doobie.implicits._
import doobie.free.connection
import cats.effect.{ContextShift, IO}
import cats.syntax.apply._
import cats.syntax.functor._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, Future}
import scalaz._
import Scalaz._
import com.daml.lf.iface.Record
import com.typesafe.scalalogging.StrictLogging

class PostgreSQLWriter(config: ExtractorConfig, target: PostgreSQLTarget, ledgerId: String)
    extends Writer
    with StrictLogging {

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
    logger.info("PostgreSQLWriter initializing...")

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

    val updatedWitnessedPackages = packageStore.keySet

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

        logger.trace(s"Multi-table state: ${multiTableState}")
        logger.trace(s"Witnessed packages: ${witnessedPackages}")
      }
      .unsafeToFuture()
  }

  private def handlePackagesWithMultiTable(
      newPackageStore: PackageStore): (MultiTableState, ConnectionIO[Unit]) = {
    val newPackages: Map[String, Interface] = newPackageStore.filterNot {
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
    logger.trace(s"Handling transaction: ${com.daml.extractor.pformat(transaction)}")

    val insertIO = insertTransaction(transaction).update.run.void

    val events = transaction.events.map(_._2)

    logger.trace(s"Events events: ${com.daml.extractor.pformat(events)}")

    (for {
      statements <- events.traverse {
        case e: CreatedEvent =>
          if (useMultiTableFormat) {
            multiTableFormat.handleCreatedEvent(multiTableState, transaction, e)
          } else {
            singleTableFormat.handleCreatedEvent(SingleTableState, transaction, e)
          }
        case e: ExercisedEvent =>
          if (useMultiTableFormat) {
            multiTableFormat.handleExercisedEvent(multiTableState, transaction, e)
          } else {
            singleTableFormat.handleExercisedEvent(SingleTableState, transaction, e)
          }
      }
    } yield {
      val sqlTransaction =
        statements.foldLeft(insertIO)(_ *> _)
      sqlTransaction.transact(xa).unsafeToFuture()
    }).sequence
  }

  def getLastOffset: Future[Option[String]] = {
    lastOffset.query[String].option.transact(xa).unsafeToFuture()
  }
}
