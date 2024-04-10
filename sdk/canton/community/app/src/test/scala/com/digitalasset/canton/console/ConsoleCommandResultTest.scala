// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.console.CommandErrors.GenericCommandError
import org.scalatest.wordspec.AnyWordSpec

class ConsoleCommandResultTest extends AnyWordSpec with BaseTest {
  "ConsoleCommandResult" should {
    "forAll" should {
      "return successfully if action runs on all instances" in {
        val instance1 = mock[LocalInstanceReference]
        val instance2 = mock[LocalInstanceReference]

        when(instance1.startCommand()).thenReturn(CommandSuccessful())
        when(instance2.startCommand()).thenReturn(CommandSuccessful())

        ConsoleCommandResult.forAll(Seq(instance1, instance2)) {
          _.startCommand()
        } should matchPattern { case CommandSuccessful(_) =>
        }
      }
      "continue after failure" in {
        val instance1 = mockInstance("instance-1")
        val instance2 = mockInstance("instance-2")
        val instance3 = mockInstance("instance-3")

        when(instance1.startCommand()).thenReturn(GenericCommandError("BOOM"))
        when(instance2.startCommand()).thenReturn(CommandSuccessful())
        when(instance3.startCommand()).thenReturn(GenericCommandError("BANG"))

        ConsoleCommandResult.forAll(Seq(instance1, instance2, instance3)) {
          _.startCommand()
        } shouldEqual GenericCommandError(
          "Command failed on 2 out of 3 instances: (failure on instance-1): BOOM, (failure on instance-3): BANG"
        )

        // verify start was still called on instance2
        verify(instance2).startCommand()
      }
    }
  }

  def mockInstance(name: String): LocalInstanceReference = {
    val ref = mock[LocalInstanceReference]
    when(ref.name).thenReturn(name)
    ref
  }
}
