// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package model
package test

import scala.util.parsing.combinator._
import Ledgers._
import com.daml.lf.model.test.Lexer.Token

import scala.annotation.nowarn
import scala.util.matching.Regex
import scala.util.parsing.input.{NoPosition, Position, Reader}

object Lexer extends RegexParsers {
  sealed trait Token
  final case object INDENT extends Token
  final case object DEDENT extends Token
  final case class INDENTATION(n: Int) extends Token
  final case class NAT(value: Int) extends Token
  final case object BRACE_OPEN extends Token
  final case object BRACE_CLOSE extends Token
  final case object COMMA extends Token
  final case object PAREN_OPEN extends Token
  final case object PAREN_CLOSE extends Token
  final case object EQUAL extends Token
  final case object BIG_PARTICIPANT extends Token
  final case object SMALL_PARTICIPANT extends Token
  final case object PARTIES extends Token
  final case object CREATE extends Token
  final case object CREATE_WITH_KEY extends Token
  final case object LOOKUP_BY_KEY extends Token
  final case object SUCCESS extends Token
  final case object FAILURE extends Token
  final case object EXERCISE extends Token
  final case object EXERCISE_BY_KEY extends Token
  final case object FETCH extends Token
  final case object FETCH_BY_KEY extends Token
  final case object ROLLBACK extends Token
  final case object CONSUMING extends Token
  final case object NON_CONSUMING extends Token
  final case object KEY extends Token
  final case object CTL extends Token
  final case object COBS extends Token
  final case object SIGS extends Token
  final case object OBS extends Token
  final case object SCENARIO extends Token
  final case object TOPOLOGY extends Token
  final case object LEDGER extends Token
  final case object COMMANDS extends Token
  final case object ACT_AS extends Token
  final case object DISCLOSURES extends Token

  override def skipWhitespace = true
  override val whiteSpace: Regex = "[ \t\r\f]+".r

  private def indentation: Parser[Token] = "\n *".r ^^ { s => INDENTATION(s.length - 1) }
  private def nat: Parser[Token] = """\d+""".r ^^ { s => NAT(s.toInt) }
  private def braceOpen: Parser[Token] = "{" ^^^ BRACE_OPEN
  private def braceClose: Parser[Token] = "}" ^^^ BRACE_CLOSE
  private def comma: Parser[Token] = "," ^^^ COMMA
  private def parenOpen: Parser[Token] = "(" ^^^ PAREN_OPEN
  private def parenClose: Parser[Token] = ")" ^^^ PAREN_CLOSE
  private def equal: Parser[Token] = "=" ^^^ EQUAL
  private def bigParticipant: Parser[Token] = "Participant" ^^^ BIG_PARTICIPANT
  private def smallParticipant: Parser[Token] = "participant" ^^^ SMALL_PARTICIPANT
  private def parties: Parser[Token] = "parties" ^^^ PARTIES
  private def create: Parser[Token] = "Create" ^^^ CREATE
  private def createWithKey: Parser[Token] = "CreateWithKey" ^^^ CREATE_WITH_KEY
  private def lookupByKey: Parser[Token] = "LookupByKey" ^^^ LOOKUP_BY_KEY
  private def exercise: Parser[Token] = "Exercise" ^^^ EXERCISE
  private def exerciseByKey: Parser[Token] = "ExerciseByKey" ^^^ EXERCISE_BY_KEY
  private def fetch: Parser[Token] = "Fetch" ^^^ FETCH
  private def fetchByKey: Parser[Token] = "FetchByKey" ^^^ FETCH_BY_KEY
  private def rollback: Parser[Token] = "Rollback" ^^^ ROLLBACK
  private def consuming: Parser[Token] = "Consuming" ^^^ CONSUMING
  private def nonConsuming: Parser[Token] = "NonConsuming" ^^^ NON_CONSUMING
  private def success: Parser[Token] = "success" ^^^ SUCCESS
  private def failure: Parser[Token] = "failure" ^^^ FAILURE
  private def key: Parser[Token] = "key" ^^^ KEY
  private def ctl: Parser[Token] = "ctl" ^^^ CTL
  private def cobs: Parser[Token] = "cobs" ^^^ COBS
  private def sigs: Parser[Token] = "sigs" ^^^ SIGS
  private def obs: Parser[Token] = "obs" ^^^ OBS
  private def scenario: Parser[Token] = "Scenario" ^^^ SCENARIO
  private def topology: Parser[Token] = "Topology" ^^^ TOPOLOGY
  private def ledger: Parser[Token] = "Ledger" ^^^ LEDGER
  private def commands: Parser[Token] = "Commands" ^^^ COMMANDS
  private def actAs: Parser[Token] = "actAs" ^^^ ACT_AS
  private def disclosures: Parser[Token] = "disclosures" ^^^ DISCLOSURES

  private def processIndentation(tokens: List[Token], indents: List[Int]): List[Token] = {
    tokens match {
      case INDENTATION(n) :: tail =>
        if (n > indents.head) {
          INDENT :: processIndentation(tail, n :: indents)
        } else if (n < indents.head) {
          val (dropped, kept) = indents.span(_ > n)
          dropped.map(_ => DEDENT) ++ processIndentation(tail, kept)
        } else {
          processIndentation(tail, indents)
        }
      case token :: tail =>
        token :: processIndentation(tail, indents)
      case Nil =>
        indents.map(_ => DEDENT)
    }
  }

  def tokens: Parser[List[Token]] = phrase(
    rep1(
      indentation | nat | braceOpen | braceClose | comma | parenOpen
        | parenClose | equal | bigParticipant | smallParticipant | parties
        | fetchByKey | fetch | rollback | createWithKey | create | lookupByKey
        | exerciseByKey | exercise | success | failure | consuming
        | nonConsuming | key | ctl | cobs | sigs | obs | scenario | topology
        | ledger | commands | actAs | disclosures
    )
  ) ^^ { tokens => processIndentation(tokens, List(0)) }

  @nowarn("msg=match may not be exhaustive")
  def lex(s: String): List[Token] = parse(tokens, s) match {
    case Success(res, _) => res
    case NoSuccess(msg, next) =>
      throw new IllegalArgumentException(s"lexing error: ${msg} ${next.pos} ${next}")
  }
}

private class TokenReader(tokens: List[Token]) extends Reader[Token] {
  override def first: Token = tokens.head
  override def atEnd: Boolean = tokens.isEmpty
  override def pos: Position = NoPosition
  override def rest: Reader[Token] = new TokenReader(tokens.tail)
  override def toString: String = tokens.take(10).mkString(", ") + " ..."
}

object Parser extends Parsers {

  implicit class RichParser[A](val parser: Parser[A]) extends AnyVal {
    def commit: Parser[A] = Parser.super.commit(parser)
  }

  import Lexer._

  type Elem = Token
  type Keys = Map[ContractId, (KeyId, PartySet)]

  def sequence[A](xs: List[(A, Keys)]): (List[A], Keys) = {
    val (as, keyss) = xs.unzip
    (as, keyss.foldLeft(Map.empty[ContractId, (KeyId, PartySet)])(_ ++ _))
  }

  def partyId: Parser[PartyId] = accept("party ID", { case NAT(n) => n })

  def contractId: Parser[ContractId] = accept("contract ID", { case NAT(n) => n })

  def participantId: Parser[ParticipantId] = accept("participant ID", { case NAT(n) => n })

  def keyId: Parser[KeyId] = accept("key ID", { case NAT(n) => n })

  def partySet: Parser[PartySet] = for {
    _ <- accept(BRACE_OPEN)
    parties <- repsep(partyId, COMMA)
    _ <- accept(BRACE_CLOSE)
  } yield parties.toSet

  def key: Parser[(KeyId, PartySet)] = for {
    _ <- accept(PAREN_OPEN)
    keyId <- keyId
    _ <- accept(COMMA)
    maintainers <- partySet
    _ <- accept(PAREN_CLOSE)
  } yield (keyId, maintainers)

  def participant: Parser[Participant] = for {
    _ <- accept(BIG_PARTICIPANT)
    id <- participantId.commit
    _ <- accept(PARTIES).commit
    _ <- accept(EQUAL).commit
    parties <- partySet.commit
  } yield Participant(id, parties)

  def createWithKey: Parser[CreateWithKey] = for {
    _ <- accept(CREATE_WITH_KEY)
    cid <- contractId.commit
    _ <- accept(KEY).commit
    _ <- accept(EQUAL).commit
    key <- key.commit
    _ <- accept(SIGS).commit
    _ <- accept(EQUAL).commit
    sigs <- partySet.commit
    _ <- accept(OBS).commit
    _ <- accept(EQUAL).commit
    obs <- partySet.commit
  } yield {
    val (keyId, maintainers) = key
    CreateWithKey(cid, keyId, maintainers, sigs, obs)
  }

  def create: Parser[Create] = for {
    _ <- accept(CREATE)
    cid <- contractId.commit
    _ <- accept(SIGS).commit
    _ <- accept(EQUAL).commit
    sigs <- partySet.commit
    _ <- accept(OBS).commit
    _ <- accept(EQUAL).commit
    obs <- partySet.commit
  } yield Create(cid, sigs, obs)

  def lookupByKey(keys: Keys): Parser[LookupByKey] = for {
    _ <- accept(LOOKUP_BY_KEY)
    res <- lookupByKeySuccess(keys) | lookupByKeyFailure
  } yield res

  def lookupByKeySuccess(keys: Keys): Parser[LookupByKey] = for {
    _ <- accept(SUCCESS)
    cid <- contractId.commit
  } yield {
    val (keyId, maintainers) = keys(cid)
    LookupByKey(Some(cid), keyId, maintainers)
  }

  def lookupByKeyFailure: Parser[LookupByKey] = for {
    _ <- accept(FAILURE)
    _ <- accept(KEY).commit
    _ <- accept(EQUAL).commit
    key <- key.commit
  } yield {
    val (keyId, maintainers) = key
    LookupByKey(None, keyId, maintainers)
  }

  def exerciseKind: Parser[ExerciseKind] =
    accept(CONSUMING) ^^^ Consuming | accept(NON_CONSUMING) ^^^ NonConsuming

  def subTransaction(keys: Keys): Parser[(List[Action], Keys)] = {
    val nonEmpty = for {
      _ <- accept(INDENT)
      subTransactionAndKeys <- transaction(keys).commit
      _ <- accept(DEDENT).commit
    } yield subTransactionAndKeys

    val empty = success((Nil, keys))

    nonEmpty | empty
  }

  def exercise(keys: Keys): Parser[(Exercise, Keys)] = for {
    _ <- accept(EXERCISE)
    kind <- exerciseKind.commit
    id <- contractId.commit
    _ <- accept(CTL).commit
    _ <- accept(EQUAL).commit
    ctl <- partySet.commit
    _ <- accept(COBS).commit
    _ <- accept(EQUAL).commit
    cobs <- partySet.commit
    subTransactionAndKeys <- subTransaction(keys).commit
  } yield {
    val (subTransaction, keys) = subTransactionAndKeys
    (
      Exercise(kind, id, ctl, cobs, subTransaction),
      keys,
    )
  }

  def exerciseByKey(keys: Keys): Parser[(ExerciseByKey, Keys)] = for {
    _ <- accept(EXERCISE_BY_KEY)
    kind <- exerciseKind.commit
    cid <- contractId.commit
    _ <- accept(CTL).commit
    _ <- accept(EQUAL).commit
    ctl <- partySet.commit
    _ <- accept(COBS).commit
    _ <- accept(EQUAL).commit
    cobs <- partySet.commit
    subTransactionAndKeys <- subTransaction(keys).commit
  } yield {
    val (subTransaction, keys) = subTransactionAndKeys
    val (keyId, maintainers) = keys(cid)
    (
      ExerciseByKey(kind, cid, keyId, maintainers, ctl, cobs, subTransaction),
      keys,
    )
  }

  def fetch: Parser[Fetch] = for {
    _ <- accept(FETCH)
    cid <- contractId.commit
  } yield Fetch(cid)

  def fetchByKey(keys: Keys): Parser[FetchByKey] = for {
    _ <- accept(FETCH_BY_KEY)
    cid <- contractId.commit
  } yield {
    val (keyId, maintainers) = keys(cid)
    FetchByKey(cid, keyId, maintainers)
  }

  def rollback(keys: Keys): Parser[(Rollback, Keys)] = for {
    _ <- accept(ROLLBACK)
    subTransactionAndKeys <- subTransaction(keys).commit
  } yield {
    val (subTransaction, keys) = subTransactionAndKeys
    (Rollback(subTransaction), keys)
  }

  def action(keys: Keys): Parser[(Action, Keys)] =
    (createWithKey.map(cwk => (cwk, keys + (cwk.contractId -> (cwk.keyId, cwk.maintainers))))
      | create.map(_ -> keys)
      | exercise(keys)
      | exerciseByKey(keys)
      | fetch.map(_ -> keys)
      | fetchByKey(keys).map(_ -> keys)
      | rollback(keys)
      | lookupByKey(keys).map(_ -> keys))

  def sequence[A](parser: Keys => Parser[(A, Keys)], keys: Keys): Parser[(List[A], Keys)] =
    opt(parser(keys)).flatMap {
      case Some((elem, keys1)) =>
        sequence(parser, keys1).commit.map { case (elems, keys2) =>
          (elem :: elems, keys2)
        }
      case None =>
        success((Nil, keys))
    }

  def transaction(keys: Keys): Parser[(List[Action], Keys)] =
    sequence(action, keys)

  def commands(keys: Keys): Parser[(Commands, Keys)] = for {
    _ <- accept(COMMANDS)
    _ <- accept(SMALL_PARTICIPANT).commit
    _ <- accept(EQUAL).commit
    participant <- participantId.commit
    _ <- accept(ACT_AS).commit
    _ <- accept(EQUAL).commit
    actAs <- partySet.commit
    _ <- accept(DISCLOSURES).commit
    _ <- accept(EQUAL).commit
    disclosures <- partySet.commit
    _ <- accept(INDENT).commit
    transactionAndKeys <- transaction(keys).commit
    _ <- accept(DEDENT).commit
  } yield {
    val (transaction, keys1) = transactionAndKeys
    (Commands(participant, actAs, disclosures, transaction), keys1)
  }

  def ledger(keys: Keys): Parser[(Ledger, Keys)] = for {
    _ <- accept(LEDGER)
    _ <- accept(INDENT).commit
    commandsAndKeys <- sequence(commands, keys).commit
    _ <- accept(DEDENT).commit
  } yield commandsAndKeys

  def topology: Parser[Topology] = for {
    _ <- accept(TOPOLOGY)
    _ <- accept(INDENT).commit
    participants <- rep(participant).commit
    _ <- accept(DEDENT).commit
  } yield participants

  def scenario: Parser[Scenario] = for {
    _ <- accept(SCENARIO)
    _ <- accept(INDENT).commit
    topology <- topology.commit
    ledgerAndKeys <- ledger(Map.empty).commit
    _ <- accept(DEDENT).commit
  } yield Scenario(topology, ledgerAndKeys._1)

  @nowarn("msg=match may not be exhaustive")
  def parseScenario(s: String): Scenario = {
    scenario(new TokenReader(lex(s))) match {
      case Success(res, _) => res
      case NoSuccess(msg, next) =>
        throw new IllegalArgumentException(s"parsing error: ${msg} at ${next}")
    }
  }
}
