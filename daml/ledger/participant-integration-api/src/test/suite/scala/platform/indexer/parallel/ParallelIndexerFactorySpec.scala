// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.parallel

import akka.stream.KillSwitch
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.platform.indexer.ha.Handle
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.{ExecutionContext, Future, Promise}

class ParallelIndexerFactorySpec extends AsyncFlatSpec with Matchers with AkkaBeforeAndAfterAll {

  // AsyncFlatSpec is with serial execution context
  private implicit val ec: ExecutionContext = system.dispatcher

  behavior of "initializeHandle"

  it should "correctly chain initializations and teardown-steps in the happy path" in {
    val t = test
    import t._

    resourceReleasing.isCompleted shouldBe false
    initialized.isCompleted shouldBe false
    initHandleStarted.isCompleted shouldBe false

    waitALittle()

    resourceReleasing.isCompleted shouldBe false
    initialized.isCompleted shouldBe false
    initHandleStarted.isCompleted shouldBe false

    resourceInitPromise.success("happy")

    val completePromise = Promise[Unit]()

    for {
      s <- initHandleStarted
      _ = {
        waitALittle()
        s shouldBe "happy"
        resourceReleasing.isCompleted shouldBe false
        initialized.isCompleted shouldBe false
        initHandleFinished.success(Handle(completePromise.future, SomeKillSwitch))
      }
      handle <- initialized
      _ = {
        waitALittle()
        resourceReleasing.isCompleted shouldBe false
        handle.completed.isCompleted shouldBe false
        handle.killSwitch shouldBe SomeKillSwitch
        completePromise.success(())
      }
      _ <- resourceReleasing
      _ = {
        waitALittle()
        handle.completed.isCompleted shouldBe false
        resourceReleased.success(())
      }
      _ <- handle.completed
    } yield {
      1 shouldBe 1
    }
  }

  it should "propagate error from releasing resource" in {
    val t = test
    import t._

    resourceReleasing.isCompleted shouldBe false
    initialized.isCompleted shouldBe false
    initHandleStarted.isCompleted shouldBe false

    waitALittle()

    resourceReleasing.isCompleted shouldBe false
    initialized.isCompleted shouldBe false
    initHandleStarted.isCompleted shouldBe false

    resourceInitPromise.success("happy")

    val completePromise = Promise[Unit]()

    for {
      s <- initHandleStarted
      _ = {
        waitALittle()
        s shouldBe "happy"
        resourceReleasing.isCompleted shouldBe false
        initialized.isCompleted shouldBe false
        initHandleFinished.success(Handle(completePromise.future, SomeKillSwitch))
      }
      handle <- initialized
      _ = {
        waitALittle()
        resourceReleasing.isCompleted shouldBe false
        handle.completed.isCompleted shouldBe false
        handle.killSwitch shouldBe SomeKillSwitch
        completePromise.success(())
      }
      _ <- resourceReleasing
      _ = {
        waitALittle()
        handle.completed.isCompleted shouldBe false
        resourceReleased.failure(new Exception("releasing resource failed"))
      }
      failure <- handle.completed.failed
    } yield {
      failure.getMessage shouldBe "releasing resource failed"
    }
  }

  it should "propagate failure from resource initialization" in {
    val t = test
    import t._

    resourceReleasing.isCompleted shouldBe false
    initialized.isCompleted shouldBe false
    initHandleStarted.isCompleted shouldBe false

    waitALittle()

    resourceReleasing.isCompleted shouldBe false
    initialized.isCompleted shouldBe false
    initHandleStarted.isCompleted shouldBe false

    resourceInitPromise.failure(new Exception("resource init failed"))

    for {
      failure <- initialized.failed
    } yield {
      waitALittle()
      failure.getMessage shouldBe "resource init failed"
      initHandleStarted.isCompleted shouldBe false
      resourceReleasing.isCompleted shouldBe false
    }
  }

  it should "propagate failure from handle initialization, complete only after releasing resource" in {
    val t = test
    import t._

    resourceReleasing.isCompleted shouldBe false
    initialized.isCompleted shouldBe false
    initHandleStarted.isCompleted shouldBe false

    waitALittle()

    resourceReleasing.isCompleted shouldBe false
    initialized.isCompleted shouldBe false
    initHandleStarted.isCompleted shouldBe false

    resourceInitPromise.success("happy")

    for {
      s <- initHandleStarted
      _ = {
        waitALittle()
        s shouldBe "happy"
        resourceReleasing.isCompleted shouldBe false
        initialized.isCompleted shouldBe false
        initHandleFinished.failure(new Exception("handle initialization failed"))
      }
      _ <- resourceReleasing
      _ = {
        waitALittle()
        initialized.isCompleted shouldBe false
        resourceReleased.success(())
      }
      failure <- initialized.failed
    } yield {
      failure.getMessage shouldBe "handle initialization failed"
    }
  }

  it should "propagate failure from handle initialization, complete only after releasing resource, even if releasing failed" in {
    val t = test
    import t._

    resourceReleasing.isCompleted shouldBe false
    initialized.isCompleted shouldBe false
    initHandleStarted.isCompleted shouldBe false

    waitALittle()

    resourceReleasing.isCompleted shouldBe false
    initialized.isCompleted shouldBe false
    initHandleStarted.isCompleted shouldBe false

    resourceInitPromise.success("happy")

    for {
      s <- initHandleStarted
      _ = {
        waitALittle()
        s shouldBe "happy"
        resourceReleasing.isCompleted shouldBe false
        initialized.isCompleted shouldBe false
        initHandleFinished.failure(new Exception("handle initialization failed"))
      }
      _ <- resourceReleasing
      _ = {
        waitALittle()
        initialized.isCompleted shouldBe false
        resourceReleased.failure(new Exception("releasing resource failed"))
      }
      failure <- initialized.failed
    } yield {
      failure.getMessage shouldBe "releasing resource failed"
    }
  }

  it should "propagate failure from completion, but only after releasing resource finished" in {
    val t = test
    import t._

    resourceReleasing.isCompleted shouldBe false
    initialized.isCompleted shouldBe false
    initHandleStarted.isCompleted shouldBe false

    waitALittle()

    resourceReleasing.isCompleted shouldBe false
    initialized.isCompleted shouldBe false
    initHandleStarted.isCompleted shouldBe false

    resourceInitPromise.success("happy")

    val completePromise = Promise[Unit]()

    for {
      s <- initHandleStarted
      _ = {
        waitALittle()
        s shouldBe "happy"
        resourceReleasing.isCompleted shouldBe false
        initialized.isCompleted shouldBe false
        initHandleFinished.success(Handle(completePromise.future, SomeKillSwitch))
      }
      handle <- initialized
      _ = {
        waitALittle()
        resourceReleasing.isCompleted shouldBe false
        handle.completed.isCompleted shouldBe false
        handle.killSwitch shouldBe SomeKillSwitch
        completePromise.failure(new Exception("completion failed"))
      }
      _ <- resourceReleasing
      _ = {
        waitALittle()
        handle.completed.isCompleted shouldBe false
        resourceReleased.success(())
      }
      failure <- handle.completed.failed
    } yield {
      failure.getMessage shouldBe "completion failed"
    }
  }

  def test: TestHandle = {
    val resourceInitPromise = Promise[String]()
    val resourceReleasing = Promise[Unit]()
    val resourceReleased = Promise[Unit]()
    val initHandleStarted = Promise[String]()
    val initHandleFinished = Promise[Handle]()
    val result = ParallelIndexerFactory.initializeHandle(
      new ResourceOwner[String] {
        override def acquire()(implicit context: ResourceContext): Resource[String] =
          Resource(
            resourceInitPromise.future
          ) { _ =>
            resourceReleasing.success(())
            resourceReleased.future
          }
      }
    ) { s =>
      initHandleStarted.success(s)
      initHandleFinished.future
    }(ResourceContext(implicitly))

    TestHandle(
      resourceInitPromise = resourceInitPromise,
      initHandleStarted = initHandleStarted.future,
      initHandleFinished = initHandleFinished,
      initialized = result,
      resourceReleasing = resourceReleasing.future,
      resourceReleased = resourceReleased,
    )
  }

  // Motivation: if we are expecting a stabilized state of the async system, but it would be not stable yet, then let's wait a little bit, so we give a chance to the system to stabilize, so we can observe our expectations fail
  private def waitALittle(): Unit = Thread.sleep(10)

  case class TestHandle(
      resourceInitPromise: Promise[String],
      initHandleStarted: Future[String],
      initHandleFinished: Promise[Handle],
      initialized: Future[Handle],
      resourceReleasing: Future[Unit],
      resourceReleased: Promise[Unit],
  )

  object SomeKillSwitch extends KillSwitch {
    override def shutdown(): Unit = ()

    override def abort(ex: Throwable): Unit = ()
  }
}
