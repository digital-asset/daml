package com.daml.lf.engine.trigger

import scalaz.{-\/, \/, \/-}
import akka.NotUsed
import akka.stream.scaladsl.Flow

import scala.annotation.tailrec

import com.daml.scalautil.Statement.discard

import scala.collection.compat._
import scala.collection.generic.CanBuildFrom
import scala.collection.immutable.{IndexedSeq, LinearSeq}

/** A variant of [[scalaz.CorecursiveList]] that emits a final state
  * at the end of the list.
  */
private[trigger] sealed abstract class UnfoldState[+T, +A] {
  type S
  val init: S
  val step: S => T \/ (A, S)

  def withInit(init: S): UnfoldState.Aux[S, T, A]

  final def foreach(f: A => Unit): T = {
    @tailrec def go(s: S): T = step(s) match {
      case -\/(t) => t
      case \/-((a, s2)) =>
        f(a)
        go(s2)
    }
    go(init)
  }

  final def runTo[FA](implicit cbf: CanBuildFrom[Nothing, A, FA]): (FA, T) = {
    val b = cbf()
    val t = foreach(a => discard(b += a))
    (b.result(), t)
  }
}

private[trigger] object UnfoldState {
  type Aux[S0, +T, +A] = UnfoldState[T, A] { type S = S0 }

  def apply[S, T, A](init: S)(step: S => T \/ (A, S)): UnfoldState[T, A] = {
    type S0 = S
    final case class UnfoldStateImpl(init: S, step: S => T \/ (A, S)) extends UnfoldState[T, A] {
      type S = S0
      override def withInit(init: S) = copy(init = init)
    }
    UnfoldStateImpl(init, step)
  }

  def fromLinearSeq[A](list: LinearSeq[A]): UnfoldState[Unit, A] =
    apply(list) {
      case hd +: tl => \/-((hd, tl))
      case _ => -\/(())
    }

  def fromIndexedSeq[A](vector: IndexedSeq[A]): UnfoldState[Unit, A] =
    apply(0) { n =>
      if (vector.sizeIs > n) \/-((vector(n), n + 1))
      else -\/(())
    }

  def flatMapConcat[T, A, B](zero: T)(f: (T, A) => UnfoldState[T, B]): Flow[A, B, NotUsed] =
    Flow fromGraph (new AsFlow(zero, f))

  import akka.stream.FlowShape
  import akka.stream.stage.GraphStage

  private[this] class AsFlow[T, A, B](zero: T, f: (T, A) => UnfoldState[T, B])
      extends GraphStage[FlowShape[A, B]] {
    import akka.stream.{Attributes, Inlet, Outlet}
    import akka.stream.stage.{GraphStageLogic, InHandler, OutHandler}

    val in: Inlet[A] = Inlet("UnfoldState in")
    val out: Outlet[B] = Outlet("UnfoldState out")
    override val shape = FlowShape(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) {
        private[this] var state: T \/ UnfoldState[T, B] = -\/(zero)

        setHandler(
          in,
          new InHandler {
            override def onPush() =
              state fold ({ t =>
                state = \/-(f(t, grab(in)))
              // TODO push?
              }, _ => throw new IllegalStateException("no buffer space for elements"))

            override def onUpstreamFinish() = {
              state fold (_ => (), { tbs =>
                discard(tbs.foreach(push(out, _)))
              })
              complete(out)
            }
          }
        )

        setHandler(
          out,
          new OutHandler {
            override def onPull() =
              state fold (_ => pull(in), { tbs =>
                tbs.step(tbs.init) fold ({ newT =>
                  state = -\/(newT)
                  pull(in)
                }, {
                  case (b, s) =>
                    state = \/-(tbs withInit s)
                    push(out, b)
                })
              })
          }
        )
      }
  }
}
