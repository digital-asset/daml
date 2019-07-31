package com.daml.ledger.participant.state.kvutils.committing

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntry,
  DamlStateKey,
  DamlStateValue
}
import com.daml.ledger.participant.state.kvutils.KeyValueCommitting.Err

object Common {
  type DamlStateMap = Map[DamlStateKey, DamlStateValue]

  /** A computation that represents the process of committing which accumulates
    * ledger state and finishes with the final state and a log entry.
    */
  final case class Commit[A](run: DamlStateMap => Either[CommitDone, (A, DamlStateMap)]) {
    def flatMap[A1 >: A](f: A => Commit[A1]): Commit[A1] =
      Commit { state =>
        run(state) match {
          case Left(done) => Left(done)
          case Right((x, state2)) =>
            f(x).run(state2)
        }
      }

  }
  final case class CommitDone(logEntry: DamlLogEntry, state: DamlStateMap)

  object Commit {

    /** Sequence commit actions which produces no intermediate values. */
    def sequence[A](act: Commit[Unit], acts: Commit[Unit]*): Commit[Unit] = {
      def go(
          state: DamlStateMap,
          act: Commit[Unit],
          rest: Seq[Commit[Unit]]
      ): Either[CommitDone, (Unit, DamlStateMap)] = {
        act.run(state) match {
          case Left(done) =>
            Left(done)
          case Right(((), state2)) =>
            rest match {
              case a +: as =>
                go(state2, a, as)
              case _ =>
                Right(() -> state2)
            }
        }
      }
      Commit { state0 =>
        go(state0, act, acts)
      }
    }

    def run(act: Commit[Unit]): (DamlLogEntry, DamlStateMap) =
      act.run(Map.empty) match {
        case Left(done) => done.logEntry -> done.state
        case Right(_) =>
          throw Err.InternalError("Commit.run: The commit processing did not terminate!")
      }

    val pass: Commit[Unit] =
      Commit { state =>
        Right(() -> state)
      }

    def delay(act: => Commit[Unit]): Commit[Unit] =
      Commit { state =>
        act.run(state)
      }

    def addState(additionalState: (DamlStateKey, DamlStateValue)*): Commit[Unit] =
      Commit { state =>
        Right(() -> (state ++ additionalState))
      }

    def addState(additionalState: Iterable[(DamlStateKey, DamlStateValue)]): Commit[Unit] =
      Commit { state =>
        Right(() -> (state ++ additionalState))
      }

    def getState(key: DamlStateKey): Commit[Option[DamlStateValue]] =
      Commit { state =>
        Right(state.get(key) -> state)
      }

    def done[A](logEntry: DamlLogEntry): Commit[A] =
      Commit { finalState =>
        Left(CommitDone(logEntry, finalState))
      }
  }
}
