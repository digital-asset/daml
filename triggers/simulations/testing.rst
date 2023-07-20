.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

How to Test Trigger Code
========================

Testing Trigger State
---------------------

.. code-block:: scala
    class CatFeedingTriggerTest extends AsyncWordSpec with TriggerSimulationTesting {
        "Cat slow feeding trigger" should {
            "initialize lambda" in {
                // TODO: initialise user state correctly

                // TODO: generate ledger submissions correctly
            }

            "update lambda" in {
                // TODO: update user state correctly

                // TODO: generate ledger submissions correctly
            }
        }
    }

Asserting Properties of User State
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

TODO: show how to generate an arbitrary user state

Trigger starting states may be defined using a Daml-LF expression. To aid with generating these Daml-LF expressions, the method `unsafeSValueFromLf: String => SValue` can be used.

.. note::
  Currently, a good of understanding of `Daml-LF <https://github.com/digital-asset/daml/blob/main/daml-lf/spec/daml-lf-1.rst>`_ (which parses to a ``Value``) is required when defining create or archive events. This is a known limitation.

TODO: show how to assert against a given user state - add in code to evaluate Daml-LF expressions that interrogate the resulting SValue?

Asserting Properties of the Trigger ACS
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

TODO: show how to generate an arbitrary ACS

Each trigger has its own in-memory active contract store (ACS) and this ACS is a cached view of the ledger's ACS (see :doc:`Overview of Trigger Code Development <overview.rst>` for details).

As a trigger's ACS influences how its user code behaves, we need to understand how to define arbitrary ACS data structures for testing purposes. This may be achieved by defining the `CreatedEvent` events we want to use to populate our trigger internal ACS.

TODO: show how to assert against the trigger ACS - resulting ACS is presented as an SValue - add in code to evaluate Daml-LF expressions that interrogate the resulting SValue?

Testing User Rule Behaviour
---------------------------

TODO: show how to generate an arbitrary trigger message

TODO: show how to assert against generated submissions - need to convert SValue to something usable in testing!

TODO: show how to evaluate trigger user rules - then we use above to test against expected state/submission outcomes

Testing the User Defined Initial State Lambda
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: scala
    class CatFeedingTriggerTest extends AsyncWordSpec with TriggerSimulationTesting {

        import TriggerRuleSimulationLib._

        "Cat slow feeding trigger" should {
            "initialize lambda" should {
                for {
                  client <- defaultLedgerClient()
                  party <- allocateParty(client)
                  (trigger, simulator) = getSimulator(
                    client,
                    QualifiedName.assertFromString("Cats:feedingTrigger"),
                    packageId,
                    applicationId,
                    compiledPackages,
                    timeProviderType,
                    triggerRunnerConfiguration,
                    party.unwrap,
                  )
                  acs = Seq.empty
                  (submissions, _, state) <- simulator.initialStateLambda(acs)
                } yield {
                  val userState = unsafeSValueApp("""\(st: TriggerState @Int64) -> st.userState""", state)

                  assertEqual(userState, "400")
                  submissions should be ('empty)
                }
            }

            "update lambda" in {
                // TODO: update user state correctly

                // TODO: generate ledger submissions correctly
            }
        }
    }

Testing the User Defined Update State Lambda
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: scala
    class CatFeedingTriggerTest extends AsyncWordSpec with TriggerSimulationTesting {
        "Cat slow feeding trigger" should {
            "initialize lambda" in {
                // TODO: initialise user state correctly

                // TODO: generate ledger submissions correctly
            }

            "update lambda" in {
                for {
                  client <- defaultLedgerClient()
                  party <- allocateParty(client)
                  (trigger, simulator) = getSimulator(
                    client,
                    QualifiedName.assertFromString("Cats:feedingTrigger"),
                    packageId,
                    applicationId,
                    compiledPackages,
                    timeProviderType,
                    triggerRunnerConfiguration,
                    party.unwrap,
                  )
                  converter = new Converter(compiledPackages, trigger)
                  userStartState = "3"
                  acs = Seq(createdEvent("Cats:Cat", s"<$party, 1>"))
                  msg = TriggerMsg.Heartbeat
                  startState = converter.fromTriggerUpdateState(
                    acs,
                    unsafeSValueFromLf(userStartState),
                    TriggerParties(party, Set.empty),
                    triggerRunnerConfiguration,
                  )
                  (submissions, _, state) <- simulator.updateStateLambda(startState, msg)
                } yield {
                  val userEndState = unsafeSValueApp("""\(st: TriggerState @Int64) -> st.userState""", endState)
                  val startACS = unsafeSValueApp("""\(st: TriggerState @Int64) -> st.acs.activeContracts""", startState)
                  val endACS = unsafeSValueApp("""\(st: TriggerState @Int64) -> st.acs.activeContracts""", endState)

                  assertEqual(userEndState, s"$userStartState - 1")
                  submissions.map(numberOfExerciseCommands).sum should be(1)
                  startACS should be(endACS)
                }
            }
        }
    }

Testing Ledger Observations
---------------------------

Testing Command Completion Observations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

TODO: show how to fire trigger with a completion message - then we use above to test against expected state/submission outcomes

Testing Create and Archive Observations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

TODO: show how to fire trigger with a transaction message - then we use above to test against expected state/submission outcomes

.. code-block:: scala
      for {
        client <- defaultLedgerClient()
        party <- allocateParty(client)
        (trigger, simulator) = getSimulator(
          client,
          QualifiedName.assertFromString("Cats:feedingTrigger"),
          packageId,
          applicationId,
          compiledPackages,
          timeProviderType,
          triggerRunnerConfiguration,
          party.unwrap,
        )
        converter = new Converter(compiledPackages, trigger)
        userStartState = "3"
        acs = Seq(createdEvent("Cats:Cat", s"<$party, 1>"))
        msg = TriggerMsg.Transaction(createdEvent("Cats:Cat", s""))
        startState = converter.fromTriggerUpdateState(
          acs,
          unsafeSValueFromLf(userStartState),
          TriggerParties(party, Set.empty),
          triggerRunnerConfiguration,
        )
       (submissions, _, state) <- simulator.updateStateLambda(startState, msg)
     } yield {
       val userEndState = unsafeSValueApp("""\(st: TriggerState @Int64) -> st.userState""", endState)
       val startACS = unsafeSValueApp("""\(st: TriggerState @Int64) -> st.acs.activeContracts""", startState)
       val endACS = unsafeSValueApp("""\(st: TriggerState @Int64) -> st.acs.activeContracts""", endState)

       assertEqual(userEndState, s"$userStartState - 1")
       submissions.map(numberOfExerciseCommands).sum should be(1)
       startACS should be(endACS)
     }

.. code-block:: scala
      for {
        client <- defaultLedgerClient()
        party <- allocateParty(client)
        (trigger, simulator) = getSimulator(
          client,
          QualifiedName.assertFromString("Cats:feedingTrigger"),
          packageId,
          applicationId,
          compiledPackages,
          timeProviderType,
          triggerRunnerConfiguration,
          party.unwrap,
        )
        converter = new Converter(compiledPackages, trigger)
        userStartState = "3"
        acs = Seq(createdEvent("Cats:Cat", s"<$party, 1>", contractId = "???"))
        msg = TriggerMsg.Transaction(archivedEvent("Cats:Cat", contractId = "???"))
        startState = converter.fromTriggerUpdateState(
          acs,
          unsafeSValueFromLf(userStartState),
          TriggerParties(party, Set.empty),
          triggerRunnerConfiguration,
        )
       (submissions, _, state) <- simulator.updateStateLambda(startState, msg)
     } yield {
       val userEndState = unsafeSValueApp("""\(st: TriggerState @Int64) -> st.userState""", endState)
       val startACS = unsafeSValueApp("""\(st: TriggerState @Int64) -> st.acs.activeContracts""", startState)
       val endACS = unsafeSValueApp("""\(st: TriggerState @Int64) -> st.acs.activeContracts""", endState)

       assertEqual(userEndState, s"$userStartState - 1")
       submissions.map(numberOfExerciseCommands).sum should be(1)
       startACS should be(endACS)
     }

Testing Periodic Behaviour
--------------------------

TODO: show how to fire trigger with a heartbeat message - then we use above to test against expected state/submission outcomes

.. code-block:: scala
      for {
        client <- defaultLedgerClient()
        party <- allocateParty(client)
        (trigger, simulator) = getSimulator(
          client,
          QualifiedName.assertFromString("Cats:feedingTrigger"),
          packageId,
          applicationId,
          compiledPackages,
          timeProviderType,
          triggerRunnerConfiguration,
          party.unwrap,
        )
        converter = new Converter(compiledPackages, trigger)
        userStartState = "3"
        acs = Seq(createdEvent("Cats:Cat", s"<$party, 1>"))
        msg = TriggerMsg.Heartbeat
        startState = converter.fromTriggerUpdateState(
          acs,
          unsafeSValueFromLf(userStartState),
          TriggerParties(party, Set.empty),
          triggerRunnerConfiguration,
        )
       (submissions, _, state) <- simulator.updateStateLambda(startState, msg)
     } yield {
       val userEndState = unsafeSValueApp("""\(st: TriggerState @Int64) -> st.userState""", endState)
       val startACS = unsafeSValueApp("""\(st: TriggerState @Int64) -> st.acs.activeContracts""", startState)
       val endACS = unsafeSValueApp("""\(st: TriggerState @Int64) -> st.acs.activeContracts""", endState)

       assertEqual(userEndState, s"$userStartState - 1")
       submissions.map(numberOfExerciseCommands).sum should be(1)
       startACS should be(endACS)
     }

Using Generators and Iterators in Trigger Testing
-------------------------------------------------

TODO:
