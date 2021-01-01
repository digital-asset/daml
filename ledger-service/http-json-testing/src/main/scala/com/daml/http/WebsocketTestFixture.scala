// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage, WebSocketRequest}
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import com.daml.http.json.SprayJson
import com.daml.jwt.domain.Jwt
import com.typesafe.scalalogging.StrictLogging
import org.scalacheck.Gen
import org.scalatest.Assertions
import org.scalatest.matchers.{MatchResult, Matcher}
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scalaz.\/
import scalaz.std.option._
import scalaz.syntax.apply._
import scalaz.syntax.tag._
import scalaz.syntax.traverse._
import scalaz.syntax.std.option._
import scalaz.std.vector._
import scalaz.std.option._
import scalaz.std.vector._
import scalaz.syntax.std.option._
import scalaz.syntax.tag._
import scalaz.syntax.traverse._

import scala.concurrent.Future

private[http] object WebsocketTestFixture extends StrictLogging with Assertions {
  import spray.json._
  import WebsocketEndpoints._

  def validSubprotocol(jwt: Jwt) = Option(s"""$tokenPrefix${jwt.value},$wsProtocol""")

  def dummyFlow[A](source: Source[A, NotUsed]): Flow[A, A, NotUsed] =
    Flow.fromSinkAndSource(Sink.foreach(println), source)

  val contractIdAtOffsetKey = "contractIdAtOffset"

  private[http] case class SimpleScenario(
      id: String,
      path: Uri.Path,
      input: Source[Message, NotUsed])

  private[http] final case class ShouldHaveEnded(
      liveStartOffset: domain.Offset,
      msgCount: Int,
      lastSeenOffset: domain.Offset)

  private[http] object ContractDelta {
    private val tagKeys = Set("created", "archived", "error")
    type T = (Vector[(String, JsValue)], Vector[domain.ArchivedContract], Option[domain.Offset])

    def unapply(
        jsv: JsValue
    ): Option[T] =
      for {
        JsObject(eventsWrapper) <- Some(jsv)
        JsArray(sums) <- eventsWrapper.get("events")
        pairs = sums collect { case JsObject(fields) => fields.filterKeys(tagKeys).head }
        if pairs.length == sums.length
        sets = pairs groupBy (_._1)
        creates = sets.getOrElse("created", Vector()) collect {
          case (_, JsObject(fields)) => fields
        }

        createPairs = creates collect (Function unlift { add =>
          (add get "contractId" collect { case JsString(v) => v }) tuple (add get "payload")
        }): Vector[(String, JsValue)]

        archives = sets.getOrElse("archived", Vector()) collect {
          case (_, adata) =>
            import json.JsonProtocol.ArchivedContractFormat
            adata.convertTo[domain.ArchivedContract]
        }: Vector[domain.ArchivedContract]

        offset = eventsWrapper
          .get("offset")
          .collect { case JsString(str) => domain.Offset(str) }: Option[domain.Offset]

      } yield (createPairs, archives, offset)
  }

  private[http] object IouAmount {
    def unapply(jsv: JsObject): Option[BigDecimal] =
      for {
        JsObject(payload) <- jsv.fields get "payload"
        JsString(amount) <- payload get "amount"
      } yield BigDecimal(amount)
  }

  private[http] object NumList {
    def unapply(jsv: JsValue): Option[Vector[BigDecimal]] =
      for {
        JsArray(numvs) <- Some(jsv)
        nums = numvs collect { case JsNumber(n) => n }
        if numvs.length == nums.length
      } yield nums
  }

  private[http] abstract class JsoField(label: String) {
    def unapply(jsv: JsObject): Option[(JsValue, JsObject)] =
      jsv.fields get label map ((_, JsObject(jsv.fields - label)))
  }

  private[http] object Created extends JsoField("created")
  private[http] object Archived extends JsoField("archived")
  private[http] object MatchedQueries extends JsoField("matchedQueries")

  private[http] final case class EventsBlock(events: Vector[JsValue], offset: Option[JsValue])
  private[http] object EventsBlock {
    import spray.json._
    import DefaultJsonProtocol._

    // cannot rely on default reader, offset: JsNull gets read as None, I want Some(JsNull) for LedgerBegin
    implicit val EventsBlockReader: RootJsonReader[EventsBlock] = (json: JsValue) => {
      val obj = json.asJsObject
      val events = obj.fields("events").convertTo[Vector[JsValue]]
      val offset: Option[JsValue] = obj.fields.get("offset").collect {
        case x: JsString => x
        case JsNull => JsNull
      }
      EventsBlock(events, offset)
    }
  }

  type IouSplitResult =
    JsValue \/ (Vector[(domain.ContractId, BigDecimal)], Vector[domain.ContractId])

  sealed abstract class SplitSeq[+X] extends Product with Serializable {
    import SplitSeq._
    def x: X

    def fold[Z](leaf: X => Z, node: (X, Z, Z) => Z): Z = {
      def go(self: SplitSeq[X]): Z = self match {
        case Leaf(x) => leaf(x)
        case Node(x, l, r) => node(x, go(l), go(r))
      }
      go(this)
    }

    def map[B](f: X => B): SplitSeq[B] =
      fold[SplitSeq[B]](x => Leaf(f(x)), (x, l, r) => Node(f(x), l, r))
  }

  object SplitSeq {
    final case class Leaf[+X](x: X) extends SplitSeq[X]
    final case class Node[+X](x: X, l: SplitSeq[X], r: SplitSeq[X]) extends SplitSeq[X]

    type Amount = Long

    val gen: Gen[SplitSeq[Amount]] =
      Gen.posNum[Amount] flatMap (x => Gen.sized(genSplit(x, _)))

    private def genSplit(x: Amount, size: Int): Gen[SplitSeq[Amount]] =
      if (size > 1 && x > 1)
        Gen.frequency(
          (1, Gen const Leaf(x)),
          (8 min size, Gen.chooseNum(1: Amount, x - 1) flatMap { split =>
            Gen zip (genSplit(split, size / 2), genSplit(x - split, size / 2)) map {
              case (l, r) => Node(x, l, r)
            }
          })
        )
      else Gen const Leaf(x)
  }

  def singleClientQueryStream(
      jwt: Jwt,
      serviceUri: Uri,
      query: String,
      offset: Option[domain.Offset] = None)(implicit asys: ActorSystem): Source[Message, NotUsed] =
    singleClientWSStream(jwt, "query", serviceUri, query, offset)

  def singleClientFetchStream(
      jwt: Jwt,
      serviceUri: Uri,
      request: String,
      offset: Option[domain.Offset] = None
  )(implicit asys: ActorSystem): Source[Message, NotUsed] =
    singleClientWSStream(jwt, "fetch", serviceUri, request, offset)

  def singleClientWSStream(
      jwt: Jwt,
      path: String,
      serviceUri: Uri,
      query: String,
      offset: Option[domain.Offset]
  )(implicit asys: ActorSystem): Source[Message, NotUsed] = {

    import spray.json._, json.JsonProtocol._
    val uri = serviceUri.copy(scheme = "ws").withPath(Uri.Path(s"/v1/stream/$path"))
    logger.info(
      s"---- singleClientWSStream uri: ${uri.toString}, query: $query, offset: ${offset.toString}")
    val webSocketFlow =
      Http().webSocketClientFlow(WebSocketRequest(uri = uri, subprotocol = validSubprotocol(jwt)))
    offset
      .cata(
        off =>
          Source.fromIterator(() =>
            Seq(Map("offset" -> off.unwrap).toJson.compactPrint, query).iterator),
        Source single query)
      .map(TextMessage(_))
      // akka-http will cancel the whole stream once the input ends so we use
      // Source.maybe to keep the input open.
      .concatMat(Source.maybe[Message])(Keep.left)
      .via(webSocketFlow)
  }

  val collectResultsAsTextMessageSkipOffsetTicks: Sink[Message, Future[Seq[String]]] =
    Flow[Message]
      .collect { case m: TextMessage => m.getStrictText }
      .filterNot(isOffsetTick)
      .toMat(Sink.seq)(Keep.right)

  val collectResultsAsTextMessage: Sink[Message, Future[Seq[String]]] =
    Flow[Message]
      .collect { case m: TextMessage => m.getStrictText }
      .toMat(Sink.seq)(Keep.right)

  private def isOffsetTick(str: String): Boolean =
    SprayJson
      .decode[EventsBlock](str)
      .map(isOffsetTick)
      .valueOr(_ => false)

  def isOffsetTick(v: JsValue): Boolean =
    SprayJson
      .decode[EventsBlock](v)
      .map(isOffsetTick)
      .valueOr(_ => false)

  def isOffsetTick(x: EventsBlock): Boolean = {
    val hasOffset = x.offset
      .collect {
        case JsString(offset) => offset.length > 0
        case JsNull => true // JsNull is for LedgerBegin
      }
      .getOrElse(false)

    x.events.isEmpty && hasOffset
  }

  def isAbsoluteOffsetTick(x: EventsBlock): Boolean = {
    val hasAbsoluteOffset = x.offset
      .collect {
        case JsString(offset) => offset.length > 0
      }
      .getOrElse(false)

    x.events.isEmpty && hasAbsoluteOffset
  }

  def isAcs(x: EventsBlock): Boolean =
    x.events.nonEmpty && x.offset.isEmpty

  def eventsBlockVector(msgs: Vector[String]): SprayJson.JsonReaderError \/ Vector[EventsBlock] =
    msgs.traverse(SprayJson.decode[EventsBlock])

  def matchJsValue(expected: JsValue) = new JsValueMatcher(expected)

  def matchJsValues(expected: Seq[JsValue]) = new MultipleJsValuesMatcher(expected)

  final class JsValueMatcher(right: JsValue) extends Matcher[JsValue] {
    override def apply(left: JsValue): MatchResult = {
      import spray.json._
      val result = (left, right) match {
        case (JsArray(l), JsArray(r)) =>
          l.length == r.length && matchJsValues(r)(l).matches
        case (JsObject(l), JsObject(r)) =>
          r.keys.forall(k => matchJsValue(r(k))(l(k)).matches)
        case (JsString(l), JsString(r)) => l == r
        case (JsNumber(l), JsNumber(r)) => l == r
        case (JsBoolean(l), JsBoolean(r)) => l == r
        case (JsNull, JsNull) => true
        case _ => false
      }
      MatchResult(result, s"$left did not match $right", s"$left matched $right")
    }
  }

  final class MultipleJsValuesMatcher(right: Seq[JsValue]) extends Matcher[Seq[JsValue]] {
    override def apply(left: Seq[JsValue]): MatchResult = {
      val result = left.length == right.length && (left, right).zipped.forall {
        case (l, r) => matchJsValue(r)(l).matches
      }
      MatchResult(result, s"$left did not match $right", s"$left matched $right")
    }
  }

  def parseResp(
      implicit ec: ExecutionContext,
      fm: Materializer): Flow[Message, JsValue, NotUsed] = {
    import spray.json._
    Flow[Message]
      .mapAsync(1) {
        case _: BinaryMessage => fail("shouldn't get BinaryMessage")
        case tm: TextMessage => tm.toStrict(1.second).map(_.text.parseJson)
      }
      .filter {
        case JsObject(fields) => !(fields contains "heartbeat")
        case _ => true
      }
  }
  val remainingDeltas: Sink[JsValue, Future[ContractDelta.T]] =
    Sink.fold[ContractDelta.T, JsValue]((Vector.empty, Vector.empty, Option.empty[domain.Offset])) {
      (acc, jsv) =>
        import domain.Offset.semigroup
        import scalaz.std.tuple._
        import scalaz.std.vector._
        import scalaz.syntax.semigroup._
        jsv match {
          case ContractDelta(c, a, o) => acc |+| ((c, a, o))
          case _ => acc
        }
    }

}
