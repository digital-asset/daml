// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.data

import java.sql.DriverManager
import java.util.concurrent.Executors.newWorkStealingPool

import cats.effect.{Blocker, ContextShift, IO}
import cats.implicits._
import com.daml.ledger.api.refinements.ApiTypes
import com.daml.navigator.model._
import com.typesafe.scalalogging.LazyLogging
import doobie._
import doobie.implicits._
import scalaz.syntax.tag._

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

/** This class is responsible for running the queries
  * and make the transformation between Scala and data store types
  */
class DatabaseActions extends LazyLogging {

  /** Uncomment the log handler to enable query logging
    */
  //  implicit private val lh: LogHandler = doobie.util.log.LogHandler.jdkLogHandler
  implicit private val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  // How many transactions can be executed in parallel.
  // 256 comes from https://github.com/scala/scala/blob/v2.12.12/src/library/scala/concurrent/impl/ExecutionContextImpl.scala#L115-L116
  private val maxConnections = 256

  /** Initializing a new database.
    * Every :memory: database is distinct from every other.
    * So, opening two database connections each with the filename
    * ":memory:" will create two independent in-memory databases.
    * See https://www.sqlite.org/inmemorydb.html
    */
  private val xa = Transactor.fromConnection[IO](
    DriverManager.getConnection("jdbc:sqlite::memory:"),
    Blocker liftExecutorService newWorkStealingPool(maxConnections),
  )

  /** Creating the tables when initializing the DatabaseActions object
    */
  (Queries.createContractTable.update.run *>
    Queries.contractIdIndex.update.run *>
    Queries.contractIsActive.update.run *>
    Queries.contractTemplateIdIsActive.update.run *>

    Queries.createEventTable.update.run *>
    Queries.eventIdIndex.update.run *>
    Queries.eventTransactionIdParentId.update.run *>
    Queries.eventContractIdSubclass.update.run *>

    Queries.createTransactionTable.update.run *>
    Queries.transactionIdIndex.update.run *>

    Queries.createCommandStatusTable.update.run *>
    Queries.commandStatusCommandIdIndex.update.run *>

    Queries.createCommandTable.update.run *>
    Queries.commandIdIndex.update.run)
    .transact(xa)
    .unsafeRunSync()

  def schema(): Try[String] = {
    Try(
      Queries
        .schema()
        .query[Option[String]]
        .to[List]
        .transact(xa)
        .unsafeRunSync()
        .flatMap(_.toList)
        .mkString("\n")
    )
  }

  private def exec: PreparedStatementIO[SqlQueryResult] = {

    // Read the specified columns from the resultset.
    def readAll(cols: List[Int]): ResultSetIO[List[List[Object]]] =
      readOne(cols).whileM[List](HRS.next)

    // Take a list of column offsets and read a parallel list of values.
    def readOne(cols: List[Int]): ResultSetIO[List[Object]] =
      cols.traverse(FRS.getObject)

    for {
      md <- HPS.getMetaData
      cols = (1 to md.getColumnCount).toList
      colNames = cols.map(md.getColumnName)
      data <- HPS.executeQuery(readAll(cols))
    } yield SqlQueryResult(colNames, data.map(_.map(Option(_).map(_.toString).getOrElse("null"))))
  }

  /** Returns the given value, logging failures */
  private def logErrors[T](result: Try[T]): Try[T] = {
    result.failed.foreach { t =>
      logger.error("Error executing database action, Navigator may be in a corrupted state", t)
    }
    result
  }

  def runQuery(query: String): Try[SqlQueryResult] = logErrors {
    Try(Queries.query(query).execWith(exec).transact(xa).unsafeRunSync())
  }

  def insertEvent(event: Event): Try[Int] = logErrors {
    Try {
      Queries
        .insertEvent(EventRow.fromEvent(event))
        .update
        .run
        .transact(xa)
        .unsafeRunSync()
    }
  }

  def eventById(id: ApiTypes.EventId, types: PackageRegistry): Try[Option[Event]] = logErrors {
    Try {
      Queries
        .eventById(id.unwrap)
        .query[EventRow]
        .to[List]
        .transact(xa)
        .unsafeRunSync()
        .headOption
        .map { data =>
          data.toEvent(types)
        }
    }.flatMap(_.sequence)
  }

  def eventsByParentId(parentId: ApiTypes.EventId, types: PackageRegistry): Try[List[Event]] =
    logErrors {
      Try {
        Queries
          .eventsByParentId(parentId.unwrap)
          .query[EventRow]
          .to[List]
          .transact(xa)
          .unsafeRunSync()
          .map { data =>
            data.toEvent(types)
          }
      }.flatMap(_.sequence)
    }

  def createEventByContractId(
      id: ApiTypes.ContractId,
      types: PackageRegistry,
  ): Try[ContractCreated] = logErrors {
    Try {
      Queries
        .eventByTypeAndContractId("ContractCreated", id.unwrap)
        .query[EventRow]
        .to[List]
        .transact(xa)
        .unsafeRunSync()
        .headOption
        .map(data => (data.toEvent(types), data)) match {
        case Some((Success(c: ContractCreated), _)) => Success(c)
        case Some((_, data)) =>
          Failure(DeserializationFailed(s"Failed to deserialize row as ContractCreated: $data"))
        case None => Failure(RecordNotFound(s"Create event not found for contractId: $id"))
      }
    }.flatten
  }

  def archiveEventByContractId(
      id: ApiTypes.ContractId,
      types: PackageRegistry,
  ): Try[Option[ChoiceExercised]] = logErrors {
    Try {
      Queries
        .eventByTypeAndContractId("ContractArchived", id.unwrap)
        .query[EventRow]
        .to[List]
        .transact(xa)
        .unsafeRunSync()
        .headOption
        .map { data =>
          data.toEvent(types) match {
            case Success(a: ChoiceExercised) => Success(a)
            case _ =>
              Failure(DeserializationFailed(s"Failed to deserialize row as ChoiceExercised: $data"))
          }
        }
    }.flatMap(_.sequence)
  }

  def choiceExercisedEventByContractById(
      id: ApiTypes.ContractId,
      types: PackageRegistry,
  ): Try[List[ChoiceExercised]] = logErrors {
    Try {
      Queries
        .eventByTypeAndContractId("ChoiceExercised", id.unwrap)
        .query[EventRow]
        .to[List]
        .transact(xa)
        .unsafeRunSync()
        .map { data =>
          data.toEvent(types) match {
            case Success(e: ChoiceExercised) => Success(e)
            case _ =>
              Failure(DeserializationFailed(s"Failed to deserialize row as ChoiceExercised: $data"))
          }
        }
    }.flatMap(_.sequence)
  }

  def insertTransaction(tx: Transaction): Try[Int] = logErrors {
    Try {
      Queries
        .insertTransaction(TransactionRow.fromTransaction(tx))
        .update
        .run
        .transact(xa)
        .unsafeRunSync()
    }
  }

  def transactionById(
      id: ApiTypes.TransactionId,
      types: PackageRegistry,
  ): Try[Option[Transaction]] = logErrors {
    Try {
      Queries
        .topLevelEventsByTransactionId(id.unwrap)
        .query[EventRow]
        .to[List]
        .transact(xa)
        .unsafeRunSync()
        .map { data =>
          data.toEvent(types)
        }
        .sequence
        .map { events =>
          Queries
            .transactionById(id.unwrap)
            .query[TransactionRow]
            .to[List]
            .transact(xa)
            .unsafeRunSync()
            .headOption
            .map { data =>
              data.toTransaction(events)
            }
        }
    }.flatten
  }

  def lastTransaction(types: PackageRegistry): Try[Option[Transaction]] = logErrors {
    Try {
      val txData = Queries
        .lastTransaction()
        .query[TransactionRow]
        .to[List]
        .transact(xa)
        .unsafeRunSync()
        .headOption

      txData.map { tx =>
        Queries
          .topLevelEventsByTransactionId(tx.id)
          .query[EventRow]
          .to[List]
          .transact(xa)
          .unsafeRunSync()
          .map { data =>
            data.toEvent(types)
          }
          .sequence
          .map { events =>
            tx.toTransaction(events)
          }
      }
    }.flatMap(_.sequence)
  }

  def upsertCommandStatus(commandId: ApiTypes.CommandId, cs: CommandStatus): Try[Int] = logErrors {
    Try {
      Queries
        .upsertCommandStatus(CommandStatusRow.fromCommandStatus(commandId, cs))
        .update
        .run
        .transact(xa)
        .unsafeRunSync()
    }
  }

  def updateCommandStatus(commandId: ApiTypes.CommandId, cs: CommandStatus): Try[Int] = logErrors {
    Try {
      Queries
        .updateCommandStatus(CommandStatusRow.fromCommandStatus(commandId, cs))
        .update
        .run
        .transact(xa)
        .unsafeRunSync()
    }
  }

  def commandStatusByCommandId(
      commandId: ApiTypes.CommandId,
      types: PackageRegistry,
  ): Try[Option[CommandStatus]] = logErrors {
    Try {
      Queries
        .commandStatusByCommandId(commandId.unwrap)
        .query[CommandStatusRow]
        .to[List]
        .transact(xa)
        .unsafeRunSync()
        .headOption
        .map { data =>
          data.toCommandStatus(transactionById(_, types))
        }
    }.flatMap(_.sequence)
  }

  def insertCommand(cmd: Command): Try[Int] = logErrors {
    Try {
      Queries
        .insertCommand(CommandRow.fromCommand(cmd))
        .update
        .run
        .transact(xa)
        .unsafeRunSync()
    }
  }

  def commandById(id: ApiTypes.CommandId, types: PackageRegistry): Try[Option[Command]] =
    logErrors {
      Try {
        Queries
          .commandById(id.unwrap)
          .query[CommandRow]
          .to[List]
          .transact(xa)
          .unsafeRunSync()
          .headOption
          .map(_.toCommand(types))
      }.flatMap(_.sequence)
    }

  def allCommands(types: PackageRegistry): Try[List[Command]] = logErrors {
    Try {
      Queries
        .allCommands()
        .query[CommandRow]
        .to[List]
        .transact(xa)
        .unsafeRunSync()
        .map(_.toCommand(types))
    }.flatMap(_.sequence)
  }

  def insertContract(contract: Contract): Try[Int] = logErrors {
    Try {
      Queries
        .insertContract(ContractRow.fromContract(contract))
        .update
        .run
        .transact(xa)
        .unsafeRunSync()
    }
  }

  def archiveContract(
      contractId: ApiTypes.ContractId,
      archiveTransactionId: ApiTypes.TransactionId,
  ): Try[Int] = logErrors {
    Try {
      Queries
        .archiveContract(contractId.unwrap, archiveTransactionId.unwrap)
        .update
        .run
        .transact(xa)
        .unsafeRunSync()
    }
  }

  def contractCount(): Try[Int] = logErrors {
    Try {
      Queries
        .contractCount()
        .query[Int]
        .unique
        .transact(xa)
        .unsafeRunSync()
    }
  }

  def activeContractCount(): Try[Int] = logErrors {
    Try {
      Queries
        .activeContractCount()
        .query[Int]
        .unique
        .transact(xa)
        .unsafeRunSync()
    }
  }

  def contract(id: ApiTypes.ContractId, types: PackageRegistry): Try[Option[Contract]] = logErrors {
    Try {
      Queries
        .contract(id.unwrap)
        .query[ContractRow]
        .to[List]
        .transact(xa)
        .unsafeRunSync()
        .headOption
        .map(_.toContract(types))
    }.flatMap(_.sequence)
  }

  def contracts(types: PackageRegistry): Try[List[Contract]] = logErrors {
    Try {
      Queries.contracts
        .query[ContractRow]
        .to[List]
        .transact(xa)
        .unsafeRunSync()
        .map(_.toContract(types))
    }.flatMap(_.sequence)
  }

  def activeContracts(types: PackageRegistry): Try[List[Contract]] = logErrors {
    Try {
      Queries.activeContracts
        .query[ContractRow]
        .to[List]
        .transact(xa)
        .unsafeRunSync()
        .map(_.toContract(types))
    }.flatMap(_.sequence)
  }

  def contractsForTemplate(tId: DamlLfIdentifier, types: PackageRegistry): Try[List[Contract]] =
    logErrors {
      Try {
        Queries
          .contractsForTemplate(tId.asOpaqueString)
          .query[ContractRow]
          .to[List]
          .transact(xa)
          .unsafeRunSync()
          .map(_.toContract(types))
      }.flatMap(_.sequence)
    }

  def activeContractsForTemplate(
      tId: DamlLfIdentifier,
      types: PackageRegistry,
  ): Try[List[Contract]] = logErrors {
    Try {
      Queries
        .activeContractsForTemplate(tId.asOpaqueString)
        .query[ContractRow]
        .to[List]
        .transact(xa)
        .unsafeRunSync()
        .map(_.toContract(types))
    }.flatMap(_.sequence)
  }
}
