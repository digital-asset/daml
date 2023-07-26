.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

How to Test Trigger Code
========================

This document describes how to use the trigger simulation library to unit test trigger Daml code.

Throughout, we assume that the :doc:`development instructions <development.rst>` have been followed to setup your development and simulation environment.

.. note::
  By default, we use the open source version of Canton in our Scalatest based simulation tests. This version of Canton performs:

  - sequential transaction processing
  - and, due to some hard coded resource limits, is more likely to rate limit ledger command submissions.

  The enterprise version of Canton does not have these limitations.

Our simulation testing will use the Daml ``Cat`` template:

.. code-block:: unused
  template Cat
    with
      owner : Party
      name : Int
    where
      signatory owner

and we use Daml triggers to create 25 instances of this contract every second.

.. note::
    A good understanding of `Translating Daml to Daml-LF <https://docs.daml.com/app-dev/daml-lf-translation.html>`_ is required when defining simulation tests. This is a known limitation.

The Scala simulation testing code is defined in :doc:`CatBreedingTriggerTest.scala <scala/CatBreedingTriggerTest.scala>`.

Trigger code can be viewed as operating using 2 lambda functions:

- an initialize lambda - given a starting active contract set, this function describes:

    - what the initial state will be
    - what side-effecting command submissions will be submitted to the ledger (using an internal ledger API client) when evaluating this initial state
- an update lambda - given a current state and a trigger message, this function describes:

    - how the current state will be modified to produce a next state
    - what side-effecting command submissions will be submitted to the ledger (using an internal ledger API client) when evaluating the next state.

Clearly then, when unit testing trigger code, we will need to make the following types of assertions:

- assertions regarding the trigger state
- assertions regarding the command submissions that are to be submitted.

The following code snippet shows the general format required when defining trigger unit tests:

.. code-block:: scala
    class CatBreedingTriggerTest extends AsyncWordSpec with TriggerSimulationTesting {

      import AbstractTriggerTest._
      import TriggerRuleSimulationLib._

      "Cat slow breeding trigger" should {
        "initialize lambda" in {
          // TODO: initial trigger state assertions
          // TODO: ledger submission assertions
        }

        "update lambda" in {
          // TODO: next trigger state assertions
          // TODO: ledger submission assertions
        }
      }
    }

Testing Trigger State
---------------------

For the purposes of unit testing, a trigger's state consists of the following components:

- a user state that is referenced and manipulated by the trigger initial lambda and update lambda
- an internal active contract store (ACS) that provides a party specific view of the ledger contracts that are currently active

    - received transaction create and archive events are used to update the internal ACS.

Asserting Properties of the User State
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Trigger starting states may be defined using a Daml-LF expression. To aid with generating these Daml-LF expressions, the method `unsafeSValueFromLf: String => SValue` is used.

Assertions of a given user state can be made using the Scala function `assertEqual: [SValue, String] => Assertion`.

The user state can be extracted from a trigger state using the Scala function `userState: SValue => SValue`.

So, for example, we can assert that a specific initial state is generated using:

.. code:: scala
    class CatBreedingTriggerTest extends AsyncWordSpec with TriggerSimulationTesting {

      import AbstractTriggerTest._
      import TriggerRuleSimulationLib._

      "Cat slow breeding trigger" should {
        "initialize lambda" in {
          val setup = (_: ApiTypes.Party) => Seq.empty

          initialStateLambdaAssertion(setup) { case (state, submissions) =>
            // Assert that the user state initializes to the Daml tuple (False, 0), expressed in Daml-LF
            assertEqual(userState(state), "< _1 = False, _2 = 0 >")

            // TODO: ledger submission assertions
          }
        }

        "update lambda" in {
          // ...
        }
      }
    }

or that, when processing heartbeat messages, the user state increases by (the breeding rate of) 25:

.. code:: scala
    class CatBreedingTriggerTest extends AsyncWordSpec with TriggerSimulationTesting {

      import AbstractTriggerTest._
      import TriggerRuleSimulationLib._

      "Cat slow breeding trigger" should {
        "initialize lambda" in {
          // ...
        }
      }

      "update lambda" should {
        val templateId = s"$packageId:Cats:Cat"
        val knownContractId = s"known-${UUID.randomUUID()}"
        val breedingRate = 25
        val userStartState = "3"
        val acsF = (party: ApiTypes.Party) => Seq(createdEvent(templateId, s"< _1 = \"$party\", _2 = 1 >", contractId = knownContractId))

        "for heartbeats" in {
          val setup = { (party: ApiTypes.Party) =>
              (
                unsafeSValueFromLf(s"< _1 = False, _2 = $userStartState >"),
                acsF(party),
                TriggerMsg.Heartbeat,
              )
          }

          updateStateLambdaAssertion(setup)  { case (startState, endState, submissions) =>
            val userEndState = unsafeSValueApp("""\(tuple: Tuple2 Bool Int64) -> (Tuple2 Bool Int64) {_2} tuple""", userState(endState))

            assertEqual(userEndState, s"$userStartState + $breedingRate")

            // ...
          }
        }

        // TODO: update lambda assertions for other trigger messages
     }
    }

Testing the Trigger ACS
^^^^^^^^^^^^^^^^^^^^^^^

TODO: show how to generate an arbitrary ACS

Each trigger has its own in-memory active contract store (ACS) and this ACS is a cached view of the ledger's ACS (see :doc:`Overview of Trigger Code Development <overview.rst>` for details).

As a trigger's ACS influences how its user code behaves, we need to understand how to define arbitrary ACS data structures for testing purposes. This may be achieved by defining the `CreatedEvent` events we want to use to populate our trigger internal ACS.

TODO: show how to assert against the trigger ACS - resulting ACS is presented as an SValue - add in code to evaluate Daml-LF expressions that interrogate the resulting SValue?

Defining Initial ACS
~~~~~~~~~~~~~~~~~~~~

Using

Asserting Properties of the ACS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

TODO:

Testing Trigger Command Submissions
-----------------------------------

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
