.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

TODO

Overview of Trigger Code Development
====================================

Triggers are pieces of Daml code that are packaged into Dar files and deployed to a trigger service. The trigger service is responsible for managing the deployed trigger's lifecycle.

When running a deployed trigger, the trigger service will unpack the uploaded trigger Dar file, evaluate Daml code within the package and, via a ledger API client, interact asynchronously with the ledger.

To help engineers develop trigger code, it is often useful to first use the Visual Studio IDE to write Daml code, with Daml script used to test against the IDE's in-memory ledger. However, when we wish to deploy the trigger code against a real Canton network, developers often hit a number of issues. In order to understand some of these issues, it is useful to first understand trigger/ledger interactions in greater detail.

Triggers interact with the ledger using a `CQRS <https://en.wikipedia.org/wiki/Command–query_separation#Command_Query_Responsibility_Separation>`_ pattern that utilises:

- a trigger in-memory contract store - the readable or queryable contract store (ACS)
- and a ledger contract store - the writable contract store (ACS).
  
These separate contract stores need to be kept in sync. This is accomplished using a `pub-sub <https://en.wikipedia.org/wiki/Publish–subscribe_pattern>`_ pattern where:

- the ledger publishes transaction create and archive events as its contract store is updated (i.e. written to)
- and the trigger registers to receive these events - so that its contract store may be updated.

Trigger Daml code queries the in-memory contract store without modifying that store. Triggers issue create and exercise commands to the ledger API - these commands are then queued by Canton network participants for asynchronous processing.

.. note::
  If create or archive events are not processed in a timely manner (e.g. due to high load) or are lost (e.g. due to Akka streaming `delivery failures <https://doc.akka.io/docs/akka/current/stream/stream-refs.html#delivery-guarantees>`_), then the trigger's view of the ledger contact store may lose coherence with the ledger's view of the contract store and so lead to stale or invalid data being used in ledger interactions.

  If the trigger's ACS becomes too large, then storing and querying the ACS may consume unnecessary system resources.

  Excessive command submissions to the ledger may result in back pressure being applied and so trigger command submissions will fail. These failures are automatically retried (for a fixed number of times with backoff), but they should generally be avoided (e.g. by controlling the number of command submissions) as they can be expensive for both the ledger and triggers.

When writing trigger Daml code, **it is the responsibility of the user to ensure that all potential ledger contention is managed.**

.. note::
  Ledger contention will lead to command submission failures (these are presented to the trigger as submission completion failures), which the trigger Daml code is responsible for retrying. Excessive numbers of ledger command failures can lead to the trigger ACS view being out of sync with the ledger ACS view and this in turn can result in stale or invalid data being used in ledger interactions.

To aid in managing these issues, we have written a trigger simulation library. The intent is that trigger Daml code should be validated against this simulation library prior to deploying that code against a Canton network.

Simulation Library Overview
===========================

The trigger simulation library is designed to help detect performance issues by collecting metrics for:

- time taken to process each trigger rule evaluation cycle
- JVM memory pressure and GC usage
- trigger ACS size
- number of ledger client submissions that are currently in-flight and are issued per trigger evaluation cycle
- difference between the trigger and ledger ACS views (by contract template) per trigger evaluation cycle.

These performance metrics may then be subjected to an offline analysis.

When architecting a collection of triggers and templates designed to interact with contracts on a ledger, a component based design is taken for describing the simulation. Each component models:

- a composable set of primitive trigger definitions - this allows:

  - complex trigger components to be modelled by composing individual abstraction layers (e.g. trigger initialization, scheduling of heartbeat messages, etc.)
  - non-standard and unsupported trigger definitions to be constructed, tested and experimented with (e.g. filtering of received transaction events, off-ledger trigger communication, etc.).
- the ledger
- and external components that may interact with the ledger, but be otherwise hidden to trigger code

An internal reporting component is responsible for collecting and storing simulation metric data for offline analysis. This data may be saved at the end of a simulation (in CSV files) for latter offline analysis.

Trigger simulations are `Scalatests <https://www.scalatest.org>`_ that extend the trait ``TriggerMultiProcessSimulation``. In extending this trait, the key method that a developer needs to implement is:

.. code-block:: scala
  protected def triggerMultiProcessSimulation: Behavior[Unit]

This method is used to define all the components that a given simulation is to take into account - and each component is defined as an Akka `Behavior <https://doc.akka.io/api/akka/current/akka/actor/typed/Behavior.html>`_. So, to define a simulation with no trigger components and no external components, one could write:

.. code-block:: scala
  class ExampleSimulation extends TriggerMultiProcessSimulation {
    override protected def triggerMultiProcessSimulation: Behavior[Unit] = {
      implicit def applicationId: ApiTypes.ApplicationId = this.applicationId

      withLedger { (client, ledger, actAs, controllerContext) =>
        // Trigger and external components could be defined here

        Behaviors.empty
      }
    }
  }

Trigger simulations may have their default configurations modified by overriding the inherited field:

.. code-block:: scala
  protected implicit lazy val simulationConfig: TriggerSimulationConfig

So, to have a simulation run for 42 seconds, one would override with:

.. code-block:: scala
  override protected implicit lazy val simulationConfig: TriggerSimulationConfig =
    TriggerSimulationConfig(simulationDuration = 42.seconds)

Under the hood, each simulation component is implemented in Scala code as an `Akka typed actor <https://doc.akka.io/docs/akka/current/typed/index.html>`_.

Ledger Process Component
------------------------

A ledger process provides trigger components with a strongly consistent data view of a participant ledger. Under the hood, this is achieved by wrapping a ledger API client with an Akka typed actor. The ledger API client then interacts with a Canton network participant on behalf of the trigger.

.. note::
  The ``LedgerProcess.scala`` file implements the underlying typed Akka actor as an instance of an Akka ``Behavior[LedgerProcess.Message]``.

  The ledger process accepts messages with Scala type ``LedgerProcess.Message``. These messages allow:

  - trigger processes to register with the ledger
  - the trigger ACS view to be compared against the ledger ACS view (for reporting purposes and use in an offline analysis)
  - external processes to interact with the ledger - e.g. to simulate external code (or ledger workloads) creating or archiving contracts.

  Ledger processes make no attempt at retrying failed command submissions. This is a known limitation.

Each trigger simulation can access the single ledger process using the inherited ``withLedger`` method.

Simulating External Ledger Interactions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

As external components may interact with a ledger (e.g. by creating or archiving contracts that a trigger registers an interest in), it is often necessary to model these within a given trigger simulation. This may be done by defining an Akka typed actor with type ``Behavior[Unit]`` and having this actor send ``LedgerProcess.ExternalAction`` messages to the ledger actor.

For example, to model an external component that randomly creates instances of a ``Cat`` contract once every second, we could write:

.. code-block:: scala
  def createRandomCat(
    ledger: ActorRef[LedgerProcess.Message],
    actAs: Party
  )(implicit materializer: Materializer): Behavior[Unit] =
    Behaviors.withTimers[Unit] { timer =>
      timer.startTimerWithFixedDelay((), 1.second, 1.second)

      Behaviors.receiveMessage { _ =>
        val randomCat =
          CreatedEvent(
            templateId = Some("Cats:Cat"),
            createArguments =
              Some(unsafeSValueFromLf(s"Cats:Cat { owner = ${actAs}, name = ${Random.nextLong()} }")),
        )
        val createEvent = LedgerProcess.ExternalAction(CreateContract(randomCat, actAs))

        ledger ! createEvent
        Behaviors.same
      }
    }

where a ``Cat`` template might be defined as:

.. code-block:: none
  template Cat
    with
      owner : Party
      name : Int
    where
      signatory owner

.. note::
  Currently, it is not possible to model external components that exercise choices on a contract. This is a known limitation.

  Currently, a good of understanding of `Daml-LF <https://github.com/digital-asset/daml/blob/main/daml-lf/spec/daml-lf-1.rst>`_ (which parses to a ``Value``) is required when defining create or archive events. This is a known limitation.

Trigger Process Component
-------------------------

A primitive trigger may be thought of as having state machine like behaviour defined by the following Daml code:

- its user defined Daml state - ``state`` say
- its ``updateState`` Daml function

  - crudely, we may think of this as defining a side effecting function with type ``TriggerMsg => state => state`` (side effects here being queries against the internal trigger ACS)
- and its ``rule`` Daml function

  - crudely, we may think of this as defining a side effecting function with type ``TriggerMsg => Party => state => Unit`` (side effects here being ledger command submissions).

More complex trigger behaviours may then be thought of as additional layers of code that encapsulate this primitive behaviour - e.g. user state initialization, scheduled messaging such as heartbeats, filtering of received transaction events, etc.

This layered or compositional approach is the basis for understanding how complex trigger processes may be defined from simpler pieces of code.

As many trigger instances can be defined from a single piece of trigger Daml code, primitive trigger processes are implemented using a `factory pattern <https://en.wikipedia.org/wiki/Factory_method_pattern>`_. Typically an instance of a trigger factory is first declared and then trigger instances (as Akka typed actors with type ``Behavior[TriggerProcess.Message]``) may then be created from that factory.

When creating a trigger instance, we need to declare the starting state for the trigger's internal ACS. For example, we could define a ``Cats:breedingTrigger`` trigger factory using:

.. code-block:: scala
  val breedingFactory: TriggerProcessFactory = triggerProcessFactory(client, ledger, "Cats:breedingTrigger", actAs)

and then define trigger instances (with an initial empty ACS) using:

.. code-block:: scala
  val trigger1: Behavior[TriggerProcess.Message] = breedingFactory.create(Seq.empty)
  val trigger2: Behavior[TriggerProcess.Message] = breedingFactory.create(Seq.empty)

Here, the associated Daml trigger code is:

.. code-block:: none
  template Cat
    with
      owner : Party
      name : Int
    where
      signatory owner

  breedingRate : Int
  breedingRate = 34

  breedingPeriod : RelTime
  breedingPeriod = seconds 1

  breedingTrigger : Trigger (Bool, Int)
  breedingTrigger = Trigger
    { initialize = pure (False, 0)
    , updateState = \msg -> case msg of
        MHeartbeat -> do
          (_, breedCount) <- get
          put (True, breedCount + breedingRate)
        _ -> do
          (_, breedCount) <- get
          put (False, breedCount)
    , rule = \party -> do
        (heartbeat, breedCount) <- get
        if heartbeat then
          forA_ [1..breedingRate] \offset -> do
            void $ emitCommands [createCmd (Cat party (breedCount + offset))] []
        else
          pure ()
    , registeredTemplates = RegisteredTemplates [ registeredTemplate @Cat ]
    , heartbeat = Some breedingPeriod
    }

Wrapping Trigger Processes
^^^^^^^^^^^^^^^^^^^^^^^^^^

Trigger processes have the Scala type ``Behavior[TriggerProcess.Message]`` and, once the Akka typed actor has been spawned, they will have the type ``ActorRef[TriggerProcess.Message]``.

Complex trigger process definitions may be defined by encapsulating instances of the spawned Akka typed actor ``ActorRef[TriggerProcess.Message]``. For example, given a Scala function ``transform: TriggerProcess.Message => TriggerProcess.Message`` we could write the following generic wrapper process:

.. code-block:: scala
  object TransformMessages {
    def apply(
      transform: TriggerProcess.Message => TriggerProcess.Message
    )(
      consumer: ActorRef[TriggerProcess.Message]
    ): Behavior[TriggerProcess.Message] = {
      Behaviors.receiveMessage { msg =>
        consumer ! transform(msg)
        Behaviors.same
      }
    }
  }

Alternatively, given a `Scalacheck <https://scalacheck.org>`_ generator ``Gen[TriggerProcess.Message]``, we could write the following wrapper process:

.. code-block:: scala
  object GeneratedMessages {
    def apply(
      msgGen: Gen[TriggerProcess.Message],
      duration: FiniteDuration,
    )(
      consumer: ActorRef[TriggerProcess.Message]
    ): Behavior[TriggerProcess.Message] = {
      Behaviors.withTimers[Unit] { timer =>
        timer.startTimerWithFixedDelay((), duration, duration)

        Behaviors.receive { case (context, _) =>
          msgGen.sample match {
            case Some(msg) =>
              consumer ! msg
              Behaviors.same

            case None =>
              context.log.warn("Scalacheck generator stopped producing messages")
              Behaviors.stopped
          }
        }
      }
    }
  }

In the following subsubsections, we present a number of pre-defined wrapper processes.

.. note::
  Wrapper processes allows engineers to define complex and potentially non-standard trigger behaviours, i.e. behaviours that are not easily definable in Daml code alone. This allows engineers to experiment with and research prototype trigger extensions.

Initializing Trigger User Defined State
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

So far, when we have defined trigger processes, we have only defined how the trigger ACS is initialized at startup. Trigger processes also have a user defined state, so how might that be initialized during startup?

By default, a trigger process with an uninitialized user state simply waits to receive a ``TriggerProcess.Initialize`` message. If the trigger process receives any other message, then it will log an error message and halt (causing the simulation to fail).

So, in order to initialize a trigger process, we simply need to send it an initialize message during the simulation setup. For example:

.. code-block:: scala
  override protected def triggerMultiProcessSimulation: Behavior[Unit] = {
    implicit def applicationId: ApiTypes.ApplicationId = this.applicationId

    withLedger { (client, ledger, actAs, controllerContext) =>
      val breedingTrigger: Behavior[TriggerProcess.Message] = breedingFactory.create(Seq.empty)
      val breedingProcess: ActorRef[TriggerProcess.Message] = controllerContext.spawn(breedingTrigger, "breedingTrigger")

      // Initialize the user state to be 0 (coded as an SValue) for the breeding trigger using a message
      breedingProcess ! TriggerProcess.Initialize(unsafeSValueFromLf("0"))

      Behaviors.empty
    }
  }  

Initializing trigger processes is a common use case, so an additional helper method has been defined that allows trigger processes to be initialized using code such as:

.. code-block:: scala
  override protected def triggerMultiProcessSimulation: Behavior[Unit] = {
    implicit def applicationId: ApiTypes.ApplicationId = this.applicationId

    withLedger { (client, ledger, actAs, controllerContext) =>
      // Initialize the user state to be 0 (coded as an SValue) for the breeding trigger at create time
      val breedingTrigger: Behavior[TriggerProcess.Message] = breedingFactory.create(unsafeSValueFromLf("0"), Seq.empty)
      
      controllerContext.spawn(breedingTrigger, "breedingTrigger")

      Behaviors.empty
    }
  }  

If a trigger fails at runtime, and we require the simulation to fail, then it is important to `watch <https://doc.akka.io/docs/akka/current/typed/actor-lifecycle.html#watching-actors>`_ the created trigger actor. This may be done using code such as:

.. code-block:: scala
  override protected def triggerMultiProcessSimulation: Behavior[Unit] = {
    implicit def applicationId: ApiTypes.ApplicationId = this.applicationId

    withLedger { (client, ledger, actAs, controllerContext) =>
      // Initialize the user state to be 0 (coded as an SValue) for the breeding trigger at create time
      val breedingTrigger: Behavior[TriggerProcess.Message] = breedingFactory.create(unsafeSValueFromLf("0"), Seq.empty)

      // Spawn the trigger actor and ensure the current (parent) actor watches it
      controllerContext.watch(controllerContext.spawn(breedingTrigger, "breedingTrigger"))

      Behaviors.empty
    }
  }

.. note::
  Currently, there is no support for extracting and using the Daml trigger ``initialize`` expression when initializing trigger processes. This is a known limitation.

  Currently, a good of understanding of `Daml-LF <https://github.com/digital-asset/daml/blob/main/daml-lf/spec/daml-lf-1.rst>`_ (which parses to a ``Value``) is required when initializing triggers. This is a known limitation.

Scheduling Heartbeat Messages
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

By default, trigger processes do not receive heartbeat messages - an explicit wrapper process (i.e. ``TriggerTimer.singleMessage`` or ``TriggerTimer.messageWithFixedDelay``) is required in order to schedule the sending of heartbeat messages.

For example, to have a trigger process receive heartbeat messages every second, we would use:

.. code-block:: scala
  val breedingTrigger: ActorRef[TriggerProcess.Message] =
    context.spawn(breedingFactory.create(SValue.SInt64(0), Seq.empty), "breedingTrigger")
  val regularTrigger: Behavior[TriggerProcess.Message] =
    TriggerTimer.singleMessage(1.second)(breedingTrigger)

or to have a trigger process receive heartbeat messages every 2 seconds (after a 5 second start up delay), we would use:

.. code-block:: scala
  val breedingTrigger: ActorRef[TriggerProcess.Message] =
    context.spawn(breedingFactory.create(unsafeSValueFromLf("0"), Seq.empty), "breedingTrigger")
  val delayedRegularTrigger: Behavior[TriggerProcess.Message] =
    TriggerTimer.singleMessage(5.seconds, 2.seconds)(breedingTrigger)

.. note::
  Currently, there is no support for extracting and using the Daml trigger ``heartbeat`` expression when scheduling heartbeat messages. This is a known limitation.

Filtering Ledger Transaction Messages
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

By default, trigger processes will receive all ledger create and archive events for the templates that they have registered for. Sometimes, it might be useful to have more granular control over which events a trigger process may receive - the ``TriggerFilter.apply`` wrapper function provides this control within a simulation.

For example, to have a trigger process ignore transaction messages with an effective date that is too old (e.g. older than a ``lifeTime: FiniteDuration``), we could use:

.. code-block:: scala
  object LifeTimeFilter {
    def apply(
      lifeTime: FiniteDuration
    )(
      consumer: ActorRef[TriggerProcess.Message]
    ): Behavior[TriggerProcess.Message] = {
      val lifeTimeFilter = { case TriggerMsg.Transaction(transaction) =>
  	    val effectiveAt = transaction.effectiveAt.seconds
  	    val now = System.currentTimeMillis / 1000

  	    now <= effectiveAt + lifeTime.toSeconds
      }

      TriggerFilter(lifeTimeFilter)(consumer)
    }
  }

.. note::
  Currently, trigger filtering can not be implemented in Daml in a manner that impacts the trigger view of the ACS.

Preserving Simulation Metrics for Offline Analysis
--------------------------------------------------

Reporting processes are implemented as Akka actors. They are (automatically) created as child processes of a ledger process and used to collect:

- trigger metric data
- trigger resource usage data
- information about the difference between the trigger and ledger contract store views.

Collected reporting data is saved into CSV files - the precise location of these files is configured by overriding the ``simulationConfig: TriggerSimulationConfig`` implicit. For example:

.. code-block:: scala
  class ExampleSimulation extends TriggerMultiProcessSimulation {

    override protected implicit lazy val simulationConfig: TriggerSimulationConfig =
      TriggerSimulationConfig(
        triggerDataFile = Paths.get("/data/trigger-simulation-metrics-data.csv"),
        acsDataFile = Paths.get("/data/trigger-simulation-acs-data.csv"),
      )

    override protected def triggerMultiProcessSimulation: Behavior[Unit] = ???
  }

If explicit file paths are configured for the reporting data, then a simple ``bazel test`` should be sufficient for running the simulation and saving the reporting data in the configured output files.

By default, however, all reporting data is stored within the bazel run directory and so, after a simulation test run has completed will be automatically deleted. To preserve the simulation reporting data then a ``bazel test --test_tempdir=/tmp`` or similar should be used.

Prior to starting and after running the trigger simulation, INFO logging records where data will be saved to - for example::

  Trigger simulation reporting data is located in /data
  16:48:37.516 [simulation-akka.actor.default-dispatcher-3] INFO  c.d.l.e.t.s.ExampleSimulation - Simulation will run for 42 seconds
  16:48:37.518 [simulation-akka.actor.default-dispatcher-3] DEBUG a.a.t.i.LogMessagesInterceptor - actor [akka://simulation/user] received message: StartSimulation
  ...
  16:49:07.534 [simulation-akka.actor.default-dispatcher-14] DEBUG a.a.t.i.LogMessagesInterceptor - actor [akka://simulation/user] received message: StopSimulation
  16:49:07.534 [simulation-akka.actor.default-dispatcher-14] INFO  c.d.l.e.t.s.TriggerMultiProcessSimulation - Simulation stopped after 30 seconds
  ...
  16:49:07.608 [simulation-akka.actor.default-dispatcher-6] INFO  c.d.l.e.t.s.ExampleSimulation - The temporary files are located in /data
  16:49:09.507 [ExampleSimulation-thread-pool-worker-3] INFO  akka.actor.CoordinatedShutdown - Running CoordinatedShutdown with reason [ActorSystemTerminateReason]

