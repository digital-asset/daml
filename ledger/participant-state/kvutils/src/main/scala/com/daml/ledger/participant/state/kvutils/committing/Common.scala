// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committing

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntry,
  DamlStateKey,
  DamlStateValue
}
import com.daml.ledger.participant.state.kvutils.{Conversions, Err}
import com.daml.ledger.participant.state.v1.Configuration
import org.slf4j.Logger

import scala.annotation.tailrec

object Common {
  type DamlStateMap = Map[DamlStateKey, DamlStateValue]

  final case class CommitContext(
      /* The input state as declared by the submission. */
      inputState: DamlStateMap,
      /* The intermediate and final state that is committed. */
      resultState: DamlStateMap,
  )

  /** A monadic computation that represents the process of committing which accumulates
    * ledger state and finishes with the final state and a log entry.
    * This is essentially State + Either.
    */
  final case class Commit[A](run: CommitContext => Either[CommitDone, (A, CommitContext)]) {
    def flatMap[A1](f: A => Commit[A1]): Commit[A1] =
      Commit { state =>
        run(state) match {
          case Left(done) => Left(done)
          case Right((x, state2)) =>
            f(x).run(state2)
        }
      }

    def pure[A1](a: A1): Commit[A1] =
      Commit { state =>
        Right(a -> state)
      }

    def map[A1](f: A => A1): Commit[A1] =
      flatMap(a => pure(f(a)))
  }

  /** The terminal state for the commit computation. */
  final case class CommitDone(logEntry: DamlLogEntry, state: DamlStateMap)

  object Commit {

    def sequence(acts: Iterable[Commit[Unit]]): Commit[Unit] = {
      @tailrec
      def go(
          state: CommitContext,
          act: Commit[Unit],
          rest: Iterable[Commit[Unit]]
      ): Either[CommitDone, (Unit, CommitContext)] = {
        act.run(state) match {
          case Left(done) =>
            Left(done)
          case Right(((), state2)) =>
            if (rest.isEmpty)
              Right(() -> state2)
            else
              go(state2, rest.head, rest.tail)
        }
      }
      if (acts.isEmpty)
        pass
      else
        Commit { state0 =>
          go(state0, acts.head, acts.tail)
        }
    }

    /** Sequence commit actions which produces no intermediate values. */
    def sequence(acts: Commit[Unit]*): Commit[Unit] =
      sequence(acts)

    /** Run a sequence of commit computations, producing a log entry and the state. */
    def runSequence(inputState: DamlStateMap, acts: Commit[Unit]*): (DamlLogEntry, DamlStateMap) =
      sequence(acts).run(CommitContext(inputState, Map.empty)) match {
        case Left(done) => done.logEntry -> done.state
        case Right(_) =>
          throw Err.InternalError("Commit.runSequence: The commit processing did not terminate!")
      }

    /** A no-op computation that produces no result. Useful when validating,
      * e.g. if (somethingIsCorrect) pass else done(someFailure). */
    val pass: Commit[Unit] =
      Commit { state =>
        Right(() -> state)
      }

    /** Lift a pure value into the computation. */
    def pure[A](a: A): Commit[A] =
      Commit { state =>
        Right(a -> state)
      }

    /** Delay a commit. Useful for delaying expensive computation, e.g.
      * delay { val foo = someExpensiveComputation; if (foo) done(err) else pass }
      */
    def delay[A](act: => Commit[A]): Commit[A] =
      Commit { state =>
        act.run(state)
      }

    /** Set value(s) in the state. */
    def set(additionalState: (DamlStateKey, DamlStateValue)*): Commit[Unit] =
      set(additionalState)

    /** Set value(s) in the state. */
    def set(additionalState: Iterable[(DamlStateKey, DamlStateValue)]): Commit[Unit] =
      Commit { state =>
        Right(() -> (state.copy(resultState = state.resultState ++ additionalState)))
      }

    /** Get a value from the state built up thus far, or if not found then from input state. */
    def get(key: DamlStateKey): Commit[Option[DamlStateValue]] =
      Commit { state =>
        Right(
          state.resultState.get(key).orElse(state.inputState.get(key))
            -> state)
      }

    def getDamlState: Commit[DamlStateMap] =
      Commit { state =>
        Right((state.inputState ++ state.resultState) -> state)
      }

    /** Finish the computation and produce a log entry, along with the
      * state built thus far by the computation. */
    def done[A](logEntry: DamlLogEntry): Commit[A] =
      Commit { state =>
        Left(CommitDone(logEntry, state.resultState))
      }
  }

  def getCurrentConfiguration(
      defaultConfig: Configuration,
      inputState: Map[DamlStateKey, Option[DamlStateValue]],
      logger: Logger): Configuration =
    inputState
      .get(Conversions.configurationStateKey)
      .flatten
      .flatMap { v =>
        Conversions
          .parseDamlConfiguration(v.getConfiguration)
          .fold({ err =>
            logger.error(s"Failed to parse configuration: $err, using default configuration.")
            None
          }, Some(_))
      }
      .getOrElse(defaultConfig)

}
