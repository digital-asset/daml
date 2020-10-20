package com.daml.lf.engine.trigger

import scalaz.{-\/, \/, \/-}
import akka.NotUsed
import akka.stream.scaladsl.Flow

import scala.annotation.tailrec

/** A variant of [[scalaz.CorecursiveList]] that emits a final state
  * at the end of the list.
  */
private[trigger] sealed abstract class UnfoldState[+T, +A] {
  type S
  val init: S
  val step: S => T \/ (A, S)

  def foreach(f: A => Unit): T = {
    @tailrec def go(s: S): T = step(s) match {
      case -\/(t) => t
      case \/-((a, s2)) =>
        f(a)
        go(s2)
    }
    go(init)
  }

  def withInit(init: S): UnfoldState.Aux[S, T, A]
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

        setHandler(in, new InHandler {
          override def onPush() = ()
        })

        setHandler(out, new OutHandler {
          override def onPull() = ()
        })
      }
  }
}
