// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.NotUsed
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, RunnableGraph, Sink, Source}
import akka.stream.{ClosedShape, FanOutShape2, Materializer}
import com.daml.http.dbbackend.{ContractDao, SupportedJdbcDriver}
import com.daml.http.dbbackend.Queries.{DBContract, SurrogateTpId}
import com.daml.http.domain.ContractTypeId
import com.daml.http.LedgerClientJwt.Terminates
import com.daml.http.util.ApiValueToLfValueConverter.apiValueToLfValue
import com.daml.http.json.JsonProtocol.LfValueDatabaseCodec.{
  apiValueToJsValue => lfValueToDbJsValue
}
import com.daml.http.util.Logging.InstanceUUID
import com.daml.fetchcontracts.util.{
  AbsoluteBookmark,
  BeginBookmark,
  ContractStreamStep,
  InsertDeleteStep,
  LedgerBegin,
}
import com.daml.scalautil.ExceptionOps._
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.NonEmptyReturningOps._
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.{v1 => lav1}
import com.daml.logging.{ContextualizedLogger, LoggingContextOf}
import doobie.free.{connection => fconn}
import fconn.ConnectionIO
import scalaz.std.vector._
import scalaz.std.list._
import scalaz.std.option.none
import scalaz.syntax.show._
import scalaz.syntax.tag._
import scalaz.syntax.functor._
import scalaz.syntax.foldable._
import scalaz.syntax.order._
import scalaz.syntax.std.option._
import scalaz.{~>, \/, NaturalTransformation}
import spray.json.{JsNull, JsValue}

import scala.concurrent.ExecutionContext
import com.daml.ledger.api.{domain => LedgerApiDomain}

private class ContractsFetch(
    getActiveContracts: LedgerClientJwt.GetActiveContracts,
    getCreatesAndArchivesSince: LedgerClientJwt.GetCreatesAndArchivesSince,
    getTermination: LedgerClientJwt.GetTermination,
)(implicit dblog: doobie.LogHandler, sjd: SupportedJdbcDriver.TC) {

  import ContractsFetch._
  import com.daml.fetchcontracts.AcsTxStreams._
  import com.daml.fetchcontracts.util.AkkaStreamsDoobie.{connectionIOFuture, sinkCioSequence_}
  import sjd.retrySqlStates

  private[this] val logger = ContextualizedLogger.get(getClass)

  /** run `within` repeatedly after fetchAndPersist until the latter is
    * consistent before and after `within`
    */
  def fetchAndPersistBracket[A](
      jwt: Jwt,
      ledgerId: LedgerApiDomain.LedgerId,
      parties: domain.PartySet,
      templateIds: List[domain.ContractTypeId.Resolved],
      tickFetch: ConnectionIO ~> ConnectionIO = NaturalTransformation.refl,
  )(within: BeginBookmark[Terminates.AtAbsolute] => ConnectionIO[A])(implicit
      ec: ExecutionContext,
      mat: Materializer,
      lc: LoggingContextOf[InstanceUUID],
  ): ConnectionIO[A] = {
    import ContractDao.laggingOffsets
    val initTries = 10
    val fetchContext = FetchContext(jwt, ledgerId, parties)
    def go(
        maxAttempts: Int,
        fetchTemplateIds: List[domain.ContractTypeId.Resolved],
        absEnd: Terminates.AtAbsolute,
    ): ConnectionIO[A] = for {
      bb <- tickFetch(fetchToAbsEnd(fetchContext, fetchTemplateIds, absEnd))
      a <- within(bb)
      // fetchTemplateIds can be a subset of templateIds (or even empty),
      // but we only get away with that by checking _all_ of templateIds,
      // which can then indicate that a larger set than fetchTemplateIds
      // has desynchronized
      lagging <- (templateIds.toSet, bb.map(_.toDomain)) match {
        case (NonEmpty(tids), AbsoluteBookmark(expectedOff)) =>
          laggingOffsets(parties, expectedOff, tids)
        case _ => fconn.pure(none[(domain.Offset, Set[domain.ContractTypeId.Resolved])])
      }
      retriedA <- lagging.cata(
        { case (newOff, laggingTids) =>
          if (maxAttempts > 1)
            go(
              maxAttempts - 1,
              laggingTids.toList,
              Terminates fromDomain newOff,
            )
          else
            fconn.raiseError(
              new IllegalStateException(
                s"failed after $initTries attempts to synchronize database for $fetchContext, $templateIds"
              )
            )
        },
        fconn.pure(a),
      )
    } yield retriedA

    // we assume that if the ledger termination is LedgerBegin, then
    // `within` will not yield concurrency-relevant results
    connectionIOFuture(getTermination(jwt, ledgerId)(lc)) flatMap {
      _.cata(go(initTries, templateIds, _), within(LedgerBegin))
    }
  }

  def fetchAndPersist(
      jwt: Jwt,
      ledgerId: LedgerApiDomain.LedgerId,
      parties: domain.PartySet,
      templateIds: List[domain.ContractTypeId.Resolved],
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
      lc: LoggingContextOf[InstanceUUID],
  ): ConnectionIO[BeginBookmark[Terminates.AtAbsolute]] =
    connectionIOFuture(getTermination(jwt, ledgerId)(lc)) flatMap {
      _.cata(
        fetchToAbsEnd(FetchContext(jwt, ledgerId, parties), templateIds, _),
        fconn.pure(LedgerBegin),
      )
    }

  private[this] def fetchToAbsEnd(
      fetchContext: FetchContext,
      templateIds: List[domain.ContractTypeId.Resolved],
      absEnd: Terminates.AtAbsolute,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
      lc: LoggingContextOf[InstanceUUID],
  ): ConnectionIO[BeginBookmark[Terminates.AtAbsolute]] = {
    import cats.instances.list._, cats.syntax.foldable.{toFoldableOps => ToFoldableOps},
    cats.syntax.traverse.{toTraverseOps => ToTraverseOps}, cats.syntax.functor._, doobie.implicits._
    // we can fetch for all templateIds on a single acsFollowingAndBoundary
    // by comparing begin offsets; however this is trickier so we don't do it
    // right now -- Stephen / Leo
    //
    // traverse once, use the max _returned_ bookmark,
    // re-traverse any that != the max returned bookmark (overriding lastOffset)
    // fetch cannot go "too far" the second time
    templateIds
      .traverse(fetchAndPersist(fetchContext, false, absEnd, _))
      .flatMap { actualAbsEnds =>
        val newAbsEndTarget = {
          import scalaz.std.list._, scalaz.syntax.foldable._, domain.Offset.`Offset ordering`
          // it's fine if all yielded LedgerBegin, so we don't want to conflate the "fallback"
          // with genuine results
          actualAbsEnds.maximum getOrElse AbsoluteBookmark(absEnd.toDomain)
        }
        newAbsEndTarget match {
          case LedgerBegin =>
            fconn.pure(AbsoluteBookmark(absEnd))
          case AbsoluteBookmark(feedback) =>
            val feedbackTerminator = Terminates fromDomain feedback
            // contractsFromOffsetIo can go _past_ absEnd, because the ACS ignores
            // this argument; see https://github.com/digital-asset/daml/pull/8226#issuecomment-756446537
            // for an example of this happening.  We deal with this race condition
            // by detecting that it has happened and rerunning any other template IDs
            // to "catch them up" to the one that "raced" ahead
            (actualAbsEnds zip templateIds)
              .collect { case (`newAbsEndTarget`, templateId) => templateId }
              .traverse_ {
                // passing a priorBookmark prevents contractsIo_ from using the ACS,
                // and it cannot go "too far" reading only the tx stream
                fetchAndPersist(
                  fetchContext,
                  true,
                  feedbackTerminator,
                  _,
                )
              }
              .as(AbsoluteBookmark(feedbackTerminator))
        }
      }
  }

  private[this] def fetchAndPersist(
      fetchContext: FetchContext,
      disableAcs: Boolean,
      absEnd: Terminates.AtAbsolute,
      templateId: domain.ContractTypeId.Resolved,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
      lc: LoggingContextOf[InstanceUUID],
  ): ConnectionIO[BeginBookmark[domain.Offset]] = {

    import doobie.implicits._, cats.syntax.apply._

    def loop(maxAttempts: Int): ConnectionIO[BeginBookmark[domain.Offset]] = {
      logger.debug(s"contractsIo, maxAttempts: $maxAttempts")
      (contractsIo_(fetchContext, disableAcs, absEnd, templateId) <* fconn.commit)
        .exceptSql {
          case e if maxAttempts > 0 && retrySqlStates(e.getSQLState) =>
            logger.debug(s"contractsIo, exception: ${e.description}, state: ${e.getSQLState}")
            fconn.rollback flatMap (_ => loop(maxAttempts - 1))
          case e @ _ =>
            logger.error(s"contractsIo3 exception: ${e.description}, state: ${e.getSQLState}")
            fconn.raiseError(e)
        }
    }

    loop(5)
  }

  private def contractsIo_(
      fetchContext: FetchContext,
      disableAcs: Boolean,
      absEnd: Terminates.AtAbsolute,
      templateId: domain.ContractTypeId.Resolved,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
      lc: LoggingContextOf[InstanceUUID],
  ): ConnectionIO[BeginBookmark[domain.Offset]] = {
    import fetchContext.parties
    for {
      offsets <- ContractDao.lastOffset(parties, templateId)
      offset1 <- contractsFromOffsetIo(
        fetchContext,
        templateId,
        offsets,
        disableAcs,
        absEnd,
      )
      _ = logger.debug(
        s"contractsFromOffsetIo($fetchContext, $templateId, $offsets, $disableAcs, $absEnd): $offset1"
      )
    } yield offset1
  }

  private def prepareCreatedEventStorage(
      ce: lav1.event.CreatedEvent,
      d: ContractTypeId.Resolved,
  ): Exception \/ PreInsertContract = {
    import scalaz.syntax.traverse._
    import scalaz.std.option._
    import com.daml.lf.crypto.Hash
    for {
      ac <-
        domain.ActiveContract fromLedgerApi (domain.ResolvedQuery(d), ce) leftMap (de =>
          new IllegalArgumentException(s"contract ${ce.contractId}: ${de.shows}"): Exception
        )
      lfKey <- ac.key.traverse(apiValueToLfValue).leftMap(_.cause: Exception)
      lfArg <- apiValueToLfValue(ac.payload) leftMap (_.cause: Exception)
    } yield DBContract(
      contractId = ac.contractId.unwrap,
      templateId = ac.templateId,
      key = lfKey.cata(lfValueToDbJsValue, JsNull),
      keyHash = lfKey.map(
        Hash
          .assertHashContractKey(ContractTypeId.toLedgerApiValue(ac.templateId), _)
          .toHexString
      ),
      payload = lfValueToDbJsValue(lfArg),
      signatories = ac.signatories,
      observers = ac.observers,
      agreementText = ac.agreementText,
    )
  }

  private def jsonifyInsertDeleteStep[D <: ContractTypeId.Resolved](
      a: InsertDeleteStep[Any, lav1.event.CreatedEvent],
      d: D,
  ): InsertDeleteStep[D, PreInsertContract] =
    a.leftMap(_ => d)
      .mapPreservingIds(prepareCreatedEventStorage(_, d) valueOr (e => throw e))

  private def contractsFromOffsetIo(
      fetchContext: FetchContext,
      templateId: domain.ContractTypeId.Resolved,
      offsets: Map[domain.Party, domain.Offset],
      disableAcs: Boolean,
      absEnd: Terminates.AtAbsolute,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
      lc: LoggingContextOf[InstanceUUID],
  ): ConnectionIO[BeginBookmark[domain.Offset]] = {

    import domain.Offset._, fetchContext.{jwt, ledgerId, parties}
    val startOffset = offsets.values.toList.minimum.cata(AbsoluteBookmark(_), LedgerBegin)

    val graph = RunnableGraph.fromGraph(
      GraphDSL.createGraph(
        Sink.queue[ConnectionIO[Unit]](),
        Sink.last[BeginBookmark[domain.Offset]],
      )(Keep.both) { implicit builder => (acsSink, offsetSink) =>
        import GraphDSL.Implicits._

        val txnK = getCreatesAndArchivesSince(
          jwt,
          ledgerId,
          transactionFilter(parties, List(templateId)),
          _: lav1.ledger_offset.LedgerOffset,
          absEnd,
        )(lc)

        // include ACS iff starting at LedgerBegin
        val (idses, lastOff) = (startOffset, disableAcs) match {
          case (LedgerBegin, false) =>
            val stepsAndOffset = builder add acsFollowingAndBoundary(txnK)
            stepsAndOffset.in <~ getActiveContracts(
              jwt,
              ledgerId,
              transactionFilter(parties, List(templateId)),
              true,
            )(lc)
            (stepsAndOffset.out0, stepsAndOffset.out1)

          case (AbsoluteBookmark(_), _) | (LedgerBegin, true) =>
            val stepsAndOffset = builder add transactionsFollowingBoundary(txnK)
            stepsAndOffset.in <~ Source.single(startOffset)
            (
              (stepsAndOffset: FanOutShape2[_, ContractStreamStep.LAV1, _]).out0,
              stepsAndOffset.out1,
            )
        }

        val transactInsertsDeletes = Flow
          .fromFunction(jsonifyInsertDeleteStep(_, templateId))
          .via(conflation)
          .map(insertAndDelete)

        idses.map(_.toInsertDelete) ~> transactInsertsDeletes ~> acsSink
        lastOff ~> offsetSink

        ClosedShape
      }
    )

    val (acsQueue, lastOffsetFuture) = graph.run()

    for {
      _ <- sinkCioSequence_(acsQueue)
      offset0 <- connectionIOFuture(lastOffsetFuture)
      offsetOrError <- offset0 max AbsoluteBookmark(absEnd.toDomain) match {
        case ab @ AbsoluteBookmark(newOffset) =>
          ContractDao
            .updateOffset(parties, templateId, newOffset, offsets)
            .map(_ => ab)
        case LedgerBegin =>
          fconn.pure(LedgerBegin)
      }
    } yield offsetOrError
  }
}

private[http] object ContractsFetch {

  type PreInsertContract =
    DBContract[ContractTypeId.RequiredPkg, JsValue, JsValue, Seq[domain.Party]]

  private def surrogateTemplateIds[K <: ContractTypeId.RequiredPkg](
      ids: Set[K]
  )(implicit
      log: doobie.LogHandler,
      sjd: SupportedJdbcDriver.TC,
      lc: LoggingContextOf[InstanceUUID],
  ): ConnectionIO[Map[K, SurrogateTpId]] = {
    import doobie.implicits._, cats.instances.vector._, cats.syntax.functor._,
    cats.syntax.traverse._
    import sjd.q.queries.surrogateTemplateId
    ids.toVector
      .traverse(k => surrogateTemplateId(k.packageId, k.moduleName, k.entityName) tupleLeft k)
      .map(_.toMap)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def insertAndDelete(
      step: InsertDeleteStep[ContractTypeId.Resolved, PreInsertContract]
  )(implicit
      log: doobie.LogHandler,
      sjd: SupportedJdbcDriver.TC,
      lc: LoggingContextOf[InstanceUUID],
  ): ConnectionIO[Unit] = {
    import doobie.implicits._, cats.syntax.functor._
    surrogateTemplateIds(
      (step.inserts.iterator.map(_.templateId) ++ step.deletes.valuesIterator).toSet
    ).flatMap { stidMap =>
      import cats.syntax.apply._, cats.instances.vector._
      import json.JsonProtocol._
      import sjd.q.queries
      // cid -> ctid
      // we want ctid
      def mapToId(a: ContractTypeId.RequiredPkg) =
        stidMap.getOrElse(
          a,
          throw new IllegalStateException(
            "template ID missing from prior retrieval; impossible"
          ),
        )

      (queries.deleteContracts(step.deletes.groupMap1(_._2)(_._1).map { case (tid, cids) =>
        (mapToId(tid), cids.toSet)
      }) *>
        queries.insertContracts(
          step.inserts map (dbc =>
            dbc.copy(
              templateId = mapToId(dbc.templateId),
              signatories = domain.Party.unsubst(dbc.signatories),
              observers = domain.Party.unsubst(dbc.observers),
            )
          )
        ))
    }.void
  }

  private def conflation[D, C: InsertDeleteStep.Cid]
      : Flow[InsertDeleteStep[D, C], InsertDeleteStep[D, C], NotUsed] = {
    // when considering this cost, keep in mind that each deleteContracts
    // may entail a table scan.  Backpressure indicates that DB operations
    // are slow, the idea here is to set the DB up for success
    val maxCost = 250L
    Flow[InsertDeleteStep[D, C]]
      .batchWeighted(max = maxCost, costFn = _.size.toLong, identity)(_ append _)
  }

  private final case class FetchContext(
      jwt: Jwt,
      ledgerId: LedgerApiDomain.LedgerId,
      parties: domain.PartySet,
  )
}
