// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.digitalasset.canton.version.ProtocolVersion
import org.scalactic.source
import org.scalatest.compatible.Assertion
import org.scalatest.wordspec.{
  AnyWordSpecLike,
  AsyncWordSpecLike,
  FixtureAnyWordSpecLike,
  FixtureAsyncWordSpecLike,
}

import scala.concurrent.Future

/** Adds utilities for checking protocol versions in FixtureAnyWordSpec tests.
  * For example:
  * {{{
  * class MyTest extends EnterpriseIntegrationTest with ProtocolVersionChecksFixtureAnyWordSpec {
  *   "some feature" onlyRunWithOrGreaterThan ProtocolVersion.v4 in { implicit env =>
  *     // this test is run only if the protocol version is v4 or later.
  *     // otherwise scalatest reports this test as ignored.
  *     testedProtocolVersion should be >= ProtocolVersion.v4
  *   }
  * }
  * }}}
  *
  * See [[com.digitalasset.canton.BaseTest.testedProtocolVersion]] on how to set the
  * protocol version for tests at runtime.
  */
trait ProtocolVersionChecksFixtureAnyWordSpec {
  this: TestEssentials & FixtureAnyWordSpecLike =>

  implicit class ProtocolCheckString(verb: String) {
    def onlyRunWithOrGreaterThan(
        minProtocolVersion: ProtocolVersion
    ): OnlyRunWhenWordSpecStringWrapper =
      new OnlyRunWhenWordSpecStringWrapper(verb, testedProtocolVersion >= minProtocolVersion)

    def onlyRunWith(protocolVersion: ProtocolVersion): OnlyRunWhenWordSpecStringWrapper =
      new OnlyRunWhenWordSpecStringWrapper(verb, testedProtocolVersion == protocolVersion)

    def onlyRunWhen(condition: ProtocolVersion => Boolean): OnlyRunWhenWordSpecStringWrapper =
      new OnlyRunWhenWordSpecStringWrapper(verb, condition(testedProtocolVersion))

    def onlyRunLessThan(
        minProtocolVersion: ProtocolVersion
    ): OnlyRunWhenWordSpecStringWrapper =
      new OnlyRunWhenWordSpecStringWrapper(verb, testedProtocolVersion < minProtocolVersion)

    def onlyRunWithOrLessThan(
        minProtocolVersion: ProtocolVersion
    ): OnlyRunWhenWordSpecStringWrapper =
      new OnlyRunWhenWordSpecStringWrapper(verb, testedProtocolVersion <= minProtocolVersion)
  }

  implicit class ProtocolCheckTaggedString(verb: ResultOfTaggedAsInvocationOnString) {
    def onlyRunWithOrGreaterThan(
        minProtocolVersion: ProtocolVersion
    ): OnlyRunWhenResultOfTaggedAsInvocationOnString =
      new OnlyRunWhenResultOfTaggedAsInvocationOnString(
        verb,
        testedProtocolVersion >= minProtocolVersion,
      )

    def onlyRunWithOrLessThan(
        minProtocolVersion: ProtocolVersion
    ): OnlyRunWhenResultOfTaggedAsInvocationOnString =
      new OnlyRunWhenResultOfTaggedAsInvocationOnString(
        verb,
        testedProtocolVersion <= minProtocolVersion,
      )
  }

  protected final class OnlyRunWhenResultOfTaggedAsInvocationOnString(
      verb: ResultOfTaggedAsInvocationOnString,
      condition: => Boolean,
  ) {
    def in(testFun: FixtureParam => Any /* Assertion */ )(implicit pos: source.Position): Unit = {
      if (condition) verb.in(testFun) else verb.ignore(testFun)
    }
  }
  protected final class OnlyRunWhenWordSpecStringWrapper(
      verb: WordSpecStringWrapper,
      condition: => Boolean,
  ) {
    def in(testFun: FixtureParam => Any /* Assertion */ )(implicit pos: source.Position): Unit = {
      if (condition) verb.in(testFun) else verb.ignore(testFun)
    }
    def ignore(
        testFun: FixtureParam => Any /* Assertion */
    )(implicit pos: source.Position): Unit = {
      verb.ignore(testFun)
    }

    def when(testFun: => Unit /* Assertion */ )(implicit pos: source.Position): Unit = {
      if (condition) verb.when(testFun) else verb.ignore(() => testFun)
    }
  }
}

/** Adds utilities for checking protocol versions in FixtureAsyncWordSpec tests.
  * For example:
  * {{{
  * class MyTest extends FixtureAsyncWordSpec with ProtocolVersionChecksFixtureAsyncWordSpec {
  *   "some feature" onlyRunWithOrGreaterThan ProtocolVersion.v4 in { implicit env =>
  *     // this test is run only if the protocol version is v4 or later.
  *     // otherwise scalatest reports this test as ignored.
  *     testedProtocolVersion should be >= ProtocolVersion.v4
  *   }
  * }
  * }}}
  *
  * See [[com.digitalasset.canton.BaseTest.testedProtocolVersion]] on how to set the
  * protocol version for tests at runtime.
  */
trait ProtocolVersionChecksFixtureAsyncWordSpec {
  this: TestEssentials & FixtureAsyncWordSpecLike =>

  implicit class ProtocolCheckString(verb: String) {
    def onlyRunWithOrGreaterThan(
        minProtocolVersion: ProtocolVersion
    ): OnlyRunWhenWordSpecStringWrapper =
      onlyRunWhen(testedProtocolVersion >= minProtocolVersion)

    def onlyRunWith(protocolVersion: ProtocolVersion): OnlyRunWhenWordSpecStringWrapper =
      onlyRunWhen(testedProtocolVersion == protocolVersion)

    def onlyRunWhen(condition: Boolean): OnlyRunWhenWordSpecStringWrapper =
      new OnlyRunWhenWordSpecStringWrapper(verb, condition)
  }

  implicit class ProtocolCheckTaggedString(verb: ResultOfTaggedAsInvocationOnString) {
    def onlyRunWithOrGreaterThan(
        minProtocolVersion: ProtocolVersion
    ): OnlyRunWhenResultOfTaggedAsInvocationOnString =
      new OnlyRunWhenResultOfTaggedAsInvocationOnString(
        verb,
        testedProtocolVersion >= minProtocolVersion,
      )

    def onlyRunWithOrLessThan(
        minProtocolVersion: ProtocolVersion
    ): OnlyRunWhenResultOfTaggedAsInvocationOnString =
      new OnlyRunWhenResultOfTaggedAsInvocationOnString(
        verb,
        testedProtocolVersion <= minProtocolVersion,
      )
  }

  protected final class OnlyRunWhenResultOfTaggedAsInvocationOnString(
      verb: ResultOfTaggedAsInvocationOnString,
      condition: => Boolean,
  ) {
    def in(testFun: FixtureParam => Future[Assertion])(implicit pos: source.Position): Unit = {
      if (condition) verb.in(testFun) else verb.ignore(testFun)
    }
  }
  protected final class OnlyRunWhenWordSpecStringWrapper(
      verb: WordSpecStringWrapper,
      condition: => Boolean,
  ) {
    def in(testFun: FixtureParam => Future[Assertion])(implicit pos: source.Position): Unit = {
      if (condition) verb.in(testFun) else verb.ignore(testFun)
    }

    def when(testFun: => Unit)(implicit pos: source.Position): Unit = {
      if (condition) verb.when(testFun)
      else
        verb.ignore { _ =>
          Future.successful { testFun; succeed }
        }
    }
  }
}

/** Adds utilities for checking protocol versions in AsyncWordSpec tests.
  * For example:
  * {{{
  * class MyTest extends AsyncWordSpec with ProtocolVersionChecksAsyncWordSpec {
  *   "some feature" onlyRunWithOrGreaterThan ProtocolVersion.v4 in {
  *     // this test is run only if the protocol version is v4 or later.
  *     // otherwise scalatest reports this test as ignored.
  *     Future {
  *       testedProtocolVersion should be >= ProtocolVersion.v4
  *     }
  *   }
  * }
  * }}}
  *
  * See [[com.digitalasset.canton.BaseTest.testedProtocolVersion]] on how to set the
  * protocol version for tests at runtime.
  */
trait ProtocolVersionChecksAsyncWordSpec {
  this: TestEssentials & AsyncWordSpecLike =>

  implicit class ProtocolCheckString(verb: String) {
    def onlyRunWithOrGreaterThan(
        minProtocolVersion: ProtocolVersion
    ): OnlyRunWhenWordSpecStringWrapper =
      onlyRunWhen(_ >= minProtocolVersion)

    def onlyRunWithOrLowerThan(
        maxProtocolVersion: ProtocolVersion
    ): OnlyRunWhenWordSpecStringWrapper =
      onlyRunWhen(_ >= maxProtocolVersion)

    private def onlyRunWhen(
        condition: ProtocolVersion => Boolean
    ): OnlyRunWhenWordSpecStringWrapper =
      new OnlyRunWhenWordSpecStringWrapper(verb, condition(testedProtocolVersion))
  }

  protected final class OnlyRunWhenWordSpecStringWrapper(
      verb: WordSpecStringWrapper,
      condition: => Boolean,
  ) {
    def in(testFun: => Future[Assertion])(implicit pos: source.Position): Unit = {
      if (condition) verb.in(testFun) else verb.ignore(testFun)
    }

    def when(testFun: => Unit)(implicit pos: source.Position): Unit = {
      if (condition) verb.when(testFun)
    }
  }
}

/** Adds utilities for checking protocol versions in AnyWordSpec tests.
  * For example:
  * {{{
  * class MyTest extends AnyWordSpec with ProtocolVersionChecksAnyWordSpec {
  *   "some feature" onlyRunWithOrGreaterThan ProtocolVersion.v4 in {
  *     // this test is run only if the protocol version is v4 or later.
  *     // otherwise scalatest reports this test as ignored.
  *     testedProtocolVersion should be >= ProtocolVersion.v4
  *   }
  * }
  * }}}
  *
  * See [[com.digitalasset.canton.BaseTest.testedProtocolVersion]] on how to set the
  * protocol version for tests at runtime.
  */
trait ProtocolVersionChecksAnyWordSpec {
  this: TestEssentials & AnyWordSpecLike =>

  implicit class ProtocolCheckString(verb: String) {
    def onlyRunWithOrGreaterThan(
        minProtocolVersion: ProtocolVersion
    ): OnlyRunWhenWordSpecStringWrapper =
      new OnlyRunWhenWordSpecStringWrapper(verb, testedProtocolVersion >= minProtocolVersion)

    def onlyRunWith(protocolVersion: ProtocolVersion): OnlyRunWhenWordSpecStringWrapper =
      new OnlyRunWhenWordSpecStringWrapper(verb, testedProtocolVersion == protocolVersion)
  }

  protected final class OnlyRunWhenWordSpecStringWrapper(
      verb: WordSpecStringWrapper,
      condition: => Boolean,
  ) {
    def in(testFun: => Any /* Assertion */ )(implicit pos: source.Position): Unit = {
      if (condition) verb.in(testFun) else verb.ignore(testFun)
    }

    def when(testFun: => Unit /* Assertion */ )(implicit pos: source.Position): Unit = {
      if (condition) verb.when(testFun) else verb.ignore(testFun)
    }
  }
}
