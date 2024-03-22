.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _command-deduplication:

Command Deduplication
#####################

The interaction of a Daml application with the ledger is inherently asynchronous: applications send commands to the ledger, and some time later they see the effect of that command on the ledger.
Many things can fail during this time window:

- The application can crash.
- The participant node can crash.
- Messages can be lost on the network.
- The ledger may be slow to respond due to a high load.

If you want to make sure that an intended ledger change is not executed twice, your application needs to robustly handle all failure scenarios.
This guide covers the following topics:

- :ref:`How command deduplication works <command-dedup-workings>`.

- :ref:`How applications can effectively use the command deduplication <command-dedup-usage>`.

.. _command-dedup-workings:

How Command Deduplication Works
*******************************

The following fields in a command submissions are relevant for command deduplication.
The first three form the :ref:`change ID <change-id>` that identifies the intended ledger change.

- The union of :ref:`party <com.daml.ledger.api.v1.Commands.party>` and :ref:`act_as <com.daml.ledger.api.v1.Commands.act_as>` define the submitting parties.
  
- The :ref:`application ID <com.daml.ledger.api.v1.Commands.application_id>` identifies the application that submits the command.

- The :ref:`command ID <com.daml.ledger.api.v1.Commands.command_id>` is chosen by the application to identify the intended ledger change.

- The deduplication period specifies the period for which no earlier submissions with the same change ID should have been accepted, as witnessed by a completion event on the :ref:`command completion service <command-completion-service>`.
  If such a change has been accepted in that period, the current submission shall be rejected.
  The period is specified either as a :ref:`deduplication duration <com.daml.ledger.api.v1.Commands.deduplication_duration>` or as a :ref:`deduplication offset <com.daml.ledger.api.v1.Commands.deduplication_offset>` (inclusive).

- The :ref:`submission ID <com.daml.ledger.api.v1.Commands.submission_id>` is chosen by the application to identify a specific submission.
  It is included in the corresponding completion event so that the application can correlate specific submissions to specific completions.
  An application should never reuse a submission ID.

The ledger may arbitrarily extend the deduplication period specified in the submission, even beyond the maximum deduplication duration specified in the :ref:`ledger configuration <ledger-configuration-service>`.

.. note::
   The maximum deduplication duration is the length of the deduplication period guaranteed to be supported by the participant.
   
The deduplication period chosen by the ledger is the *effective deduplication period*.
The ledger may also convert a requested deduplication duration into an effective deduplication offset or vice versa.
The effective deduplication period is reported in the command completion event in the :ref:`deduplication duration <com.daml.ledger.api.v1.Completion.deduplication_duration>` or :ref:`deduplication offset <com.daml.ledger.api.v1.Completion.deduplication_offset>` fields.

A command submission is considered a **duplicate submission** if at least one of the following holds:

- The submitting participant's completion service contains a successful completion event for the same :ref:`change ID <change-id>` within the *effective* deduplication period.

- The participant or Daml ledger are aware of another command submission in-flight with the same :ref:`change ID <change-id>` when they perform command deduplication.

The outcome of command deduplication is communicated as follows:

- Command submissions via the :ref:`command service <command-service>` indicate the command deduplication outcome as a synchronous gRPC response unless the `gRPC deadline <https://grpc.io/blog/deadlines/>`_ was exceeded.

  .. note::
     The outcome MAY additionally appear as a completion event on the :ref:`command completion service <command-completion-service>`,
     but applications using the :ref:`command service <command-service>` typically need not process completion events.

- Command submissions via the :ref:`command submission service <command-submission-service>` can indicate the outcome as a synchronous gRPC response,
  or asynchronously through the :ref:`command completion service <command-completion-service>`.
  In particular, the submission may be a duplicate even if the command submission service acknowledges the submission with the gRPC status code ``OK``.

Independently of how the outcome is communicated, command deduplication generates the following outcomes of a command submission:

- If there is no conflicting submission with the same :ref:`change ID <change-id>` on the Daml ledger or in-flight, the completion event and possibly the response convey the result of the submission (success or a gRPC error; :doc:`/app-dev/grpc/error-codes` explains how errors are communicated).

- The gRPC status code ``ALREADY_EXISTS`` with error code ID :ref:`DUPLICATE_COMMAND <error_code_DUPLICATE_COMMAND>` indicates that there is an earlier command completion for the same :ref:`change ID <change-id>` within the effective deduplication period.

- The gRPC status code ``ABORTED`` with error code id :ref:`SUBMISSION_ALREADY_IN_FLIGHT <error_code_SUBMISSION_ALREADY_IN_FLIGHT>` indicates that another submission for the same :ref:`change ID <change-id>` was in flight when this submission was processed.

- The gRPC status code ``FAILED_PRECONDITION`` with error code id :ref:`INVALID_DEDUPLICATION_PERIOD <error_code_INVALID_DEDUPLICATION_PERIOD>` indicates that the specified deduplication period is not supported.
  The fields ``longest_duration`` or ``earliest_offset`` in the metadata specify the longest duration or earliest offset that is currently supported on the Ledger API endpoint.
  At least one of the two fields is present.

  Neither deduplication durations up to the :ref:`maximum deduplication duration <com.daml.ledger.api.v1.LedgerConfiguration.max_deduplication_duration>` nor deduplication offsets published within that duration SHOULD result in this error.
  Participants may accept longer periods at their discretion.

- The gRPC status code ``FAILED_PRECONDITION`` with error code id :ref:`PARTICIPANT_PRUNED_DATA_ACCESSED <error_code_PARTICIPANT_PRUNED_DATA_ACCESSED>`, when specifying a deduplication period represented by an offset, indicates that the specified deduplication offset has been pruned.
  The field ``earliest_offset`` in the metadata specifies the last pruned offset.

For deduplication to work as intended, all submissions for the same ledger change must be submitted via the same participant.
Whether a submission is considered a duplicate is determined by completion events, and by default a participant outputs only the completion events for submissions that were requested via the very same participant.
At this time, only `Daml driver for VMware Blockchain <https://www.digitalasset.com/daml-for-vmware-blockchain/>`__ supports command deduplication across participants.

.. _command-dedup-usage:

How to Use Command Deduplication
********************************

To effectuate a ledger change exactly once, the application must resubmit a command if an earlier submission was lost.
However, the application typically cannot distinguish a lost submission from slow submission processing by the ledger.
Command deduplication allows the application to resubmit the command until it is executed and reject all duplicate submissions thereafter.

Some ledger changes can be executed at most once, so no command deduplication is needed for them.
For example, if the submitted command exercises a consuming choice on a given contract ID, this command can be accepted at most once because every contract can be archived at most once.
All duplicate submissions of such a change will be rejected with :ref:`CONTRACT_NOT_ACTIVE <error_code_CONTRACT_NOT_ACTIVE>`.

In contrast, a :ref:`Create command <com.daml.ledger.api.v1.CreateCommand>` would create a fresh contract instance of the given :ref:`template <com.daml.ledger.api.v1.CreateCommand.template_id>` for each submission that reaches the ledger (unless other constraints such as the :ref:`template preconditions <daml-ref-preconditions>` or contract key uniqueness are violated).
Similarly, an :ref:`Exercise command <com.daml.ledger.api.v1.ExerciseCommand>` on a non-consuming choice or an :ref:`Exercise-By-Key command <com.daml.ledger.api.v1.ExercisebyKeyCommand>` may be executed multiple times if submitted multiple times.
With command deduplication, applications can ensure such intended ledger changes are executed only once within the deduplication period, even if the application resubmits, say because it considers the earlier submissions to be lost or forgot during a crash that it had already submitted the command.


Known Processing Time Bounds
============================

For this strategy, you must estimate a bound ``B`` on the processing time and forward clock drifts in the Daml ledger with respect to the application’s clock.
If processing measured across all retries takes longer than your estimate ``B``, the ledger change may take effect several times.
Under this caveat, the following strategy works for applications that use the :ref:`Command Service <command-service>` or the :ref:`Command Submission <command-submission-service>` and :ref:`Command Completion Service <command-completion-service>`.

.. note::
   The bound ``B`` should be at most the configured :ref:`maximum deduplication duration <com.daml.ledger.api.v1.LedgerConfiguration.max_deduplication_duration>`.
   Otherwise you rely on the ledger accepting longer deduplication durations.
   Such reliance makes your application harder to port to other Daml ledgers and fragile, as the ledger may stop accepting such extended durations at its own discretion.

.. _dedup-bounded-step-command-id:

#. Choose a command ID for the ledger change, in a way that makes sure the same ledger change is always assigned the same command ID.
   Either determine the command ID deterministically (e.g., if your contract payload contains a globally unique identifier, you can use that as your command ID), or choose the command ID randomly and persist it with the ledger change so that the application can use the same command ID in resubmissions after a crash and restart.

   .. note::
      Make sure that you assign the same command ID to all command (re-)submissions of the same ledger change.
      This is useful for the recovery procedure after an application crash/restart.
      After a crash, the application in general cannot know whether it has submitted a set of commands before the crash.
      If in doubt, resubmit the commands using the same command ID.
      If the commands had been submitted before the crash, command deduplication on the ledger will reject the resubmissions.

   .. _dedup-bounded-step-offset:

#. When you use the :ref:`Command Completion Service <command-submission-service>`, obtain a recent offset on the completion stream ``OFF1``, say the :ref:`current ledger end <com.daml.ledger.api.v1.CompletionEndRequest>`.

   .. _dedup-bounded-step-submit:
   
#. Submit the command with the following parameters:

   - Set the :ref:`command ID <com.daml.ledger.api.v1.Commands.command_id>` to the chosen command ID from :ref:`Step 1 <dedup-bounded-step-command-id>`.

   - Set the :ref:`deduplication duration <com.daml.ledger.api.v1.Commands.deduplication_duration>` to the bound ``B``.

     .. note::
        It is prudent to explicitly set the deduplication duration to the desired bound ``B``,
	to guard against the case where a ledger configuration update shortens the maximum deduplication duration.
	With the bound ``B``, you will be notified of such a problem via an :ref:`INVALID_DEDUPLICATION_PERIOD <error_code_INVALID_DEDUPLICATION_PERIOD>` error
	if the ledger does not support deduplication durations of length ``B`` any more.
	
	If you omitted the deduplication period, the currently valid maximum deduplication duration would be used.
	In this case, a ledger configuration update could silently shorten the deduplication period and thus invalidate your deduplication analysis.

   - Set the :ref:`submission ID <com.daml.ledger.api.v1.Commands.submission_id>` to a fresh value, e.g., a random UUID.

   - Set the timeout (gRPC deadline) to the expected submission processing time (Command Service) or submission hand-off time (Command Submission Service).

     The **submission processing time** is the time between when the application sends off a submission to the :ref:`Command Service <command-service>` and when it receives (synchronously, unless it times out) the acceptance or rejection.
     The **submission hand-off time** is the time between when the application sends off a submission to the :ref:`Command Submission Service <command-submission-service>` and when it obtains a synchronous response for this gRPC call.
     After the RPC timeout, the application considers the submission as lost and enters a retry loop.
     This timeout is typically much shorter than the deduplication duration.

   .. _dedup-bounded-step-await:
   
#. Wait until the RPC call returns a response.
   
   - Status codes other than ``OK`` should be handled according to :ref:`error handling <dedup-bounded-error-handling>`.

   - When you use the :ref:`Command Service <command-service>` and the response carries the status code ``OK``, the ledger change took place.
     You can report success.
     
   - When you use the :ref:`Command Submission Service <command-submission-service>`,
     subscribe with the :ref:`Command Completion Service <command-submission-service>` for completions for ``actAs`` from ``OFF1`` (exclusive) until you see a completion event for the change ID and the submission ID chosen in :ref:`Step 3 <dedup-bounded-step-submit>`.
     If the completion’s status is ``OK``, the ledger change took place and you can report success.
     Other status codes should be handled according to :ref:`error handling <dedup-bounded-error-handling>`.
   
     This step needs no timeout as the :ref:`Command Submission Service <command-submission-service>` acknowledges a submission only if there will eventually be a completion event, unless relevant parts of the system become permanently unavailable.


.. _dedup-bounded-error-handling:

Error Handling
--------------

Error handling is needed when the status code of the command submission RPC call or in the :ref:`completion event <com.daml.ledger.api.v1.Completion.status>` is not ``OK``.
The following table lists appropriate reactions by status code (written as ``STATUS_CODE``) and error code (written in capital letters with a link to the error code documentation).
Fields in the error metadata are written as ``field`` in lowercase letters.

.. list-table:: Command deduplication error handling with known processing time bound
   :widths: 10 50
   :header-rows: 1

   - * Error condition
     
     * Reaction

       
   - * ``DEADLINE_EXCEEDED``
     
     * Consider the submission lost.
       
       Retry from :ref:`Step 2 <dedup-bounded-step-offset>`, obtaining the completion offset ``OFF1``, and possibly increase the timeout.

       
   - * Application crashed
     
     * Retry from :ref:`Step 2 <dedup-bounded-step-offset>`, obtaining the completion offset ``OFF1``.


   - * ``ALREADY_EXISTS`` / :ref:`DUPLICATE_COMMAND <error_code_DUPLICATE_COMMAND>`
     
     * The change ID has already been accepted by the ledger within the reported deduplication period.
       The optional field ``completion_offset`` contains the precise offset.
       The optional field ``existing_submission_id`` contains the submission ID of the successful submission.
       Report success for the ledger change.
       
       
   - * ``FAILED_PRECONDITION`` / :ref:`INVALID_DEDUPLICATION_PERIOD <error_code_INVALID_DEDUPLICATION_PERIOD>`
     
     * The specified deduplication period is longer than what the Daml ledger supports or the ledger cannot handle the specified deduplication offset.
       ``earliest_offset`` contains the earliest deduplication offset or ``longest_duration`` contains the longest deduplication duration that can be used (at least one of the two must be provided).

       Options:

       - Negotiate support for longer deduplication periods with the ledger operator.

       - Set the deduplication offset to ``earliest_offset`` or the deduplication duration to ``longest_duration`` and retry from :ref:`Step 2 <dedup-bounded-step-offset>`,  obtaining the completion offset ``OFF1``.
	 This may lead to accepting the change twice within the originally intended deduplication period.


   - * ``FAILED_PRECONDITION`` / :ref:`PARTICIPANT_PRUNED_DATA_ACCESSED <error_code_PARTICIPANT_PRUNED_DATA_ACCESSED>`

     * The specified deduplication offset has been pruned by the participant.
       ``earliest_offset`` contains the last pruned offset.

        Use the :ref:`Command Completion Service <command-completion-service>` by asking for the :ref:`completions <com.daml.ledger.api.v1.CompletionStreamRequest>`, starting from the last pruned offset by setting :ref:`offset <com.daml.ledger.api.v1.CompletionStreamRequest.offset>` to the value of
        ``earliest_offset``, and use the first received :ref:`offset <com.daml.ledger.api.v1.Checkpoint.offset>` as a deduplication offset.


   - * ``ABORTED`` / :ref:`SUBMISSION_ALREADY_IN_FLIGHT <error_code_SUBMISSION_ALREADY_IN_FLIGHT>`
     
       This error occurs only as an RPC response, not inside a completion event.
       
     * There is already another submission in flight, with the submission ID in ``existing_submission_id``.

       - When you use the :ref:`Command Service <command-service>`, wait a bit and retry from :ref:`Step 3 <dedup-bounded-step-submit>`, submitting the command.

	 Since the in-flight submission might still be rejected, (repeated) resubmission ensures that you (eventually) learn the outcome:
         If an earlier submission was accepted, you will eventually receive a :ref:`DUPLICATE_COMMAND <error_code_DUPLICATE_COMMAND>` rejection.
	 Otherwise, you have a second chance to get the ledger change accepted on the ledger and learn the outcome.
	 

       - When you use the :ref:`Command Completion Service <command-completion-service>`, look for a completion for ``existing_submission_id`` instead of the chosen submission ID in :ref:`Step 4 <dedup-bounded-step-await>`.


   - * ``ABORTED`` / other error codes
     
     * Wait a bit and retry from :ref:`Step 2 <dedup-bounded-step-offset>`, obtaining the completion offset ``OFF1``.

       
   - * other error conditions

     * Use background knowledge about the business workflow and the current ledger state to decide whether earlier submissions might still get accepted.

       - If you conclude that it cannot be accepted any more, stop retrying and report that the ledger change failed.
       - Otherwise, retry from :ref:`Step 2 <dedup-bounded-step-offset>`, obtaining a completion offset ``OFF1``, or give up without knowing for sure that the ledger change will not happen.

       For example, if the ledger change only creates a contract instance of a template, you can never be sure, as any outstanding submission might still be accepted on the ledger.
       In particular, you must not draw any conclusions from not having received a :ref:`SUBMISSION_ALREADY_IN_FLIGHT <error_code_SUBMISSION_ALREADY_IN_FLIGHT>` error, because the outstanding submission may be queued somewhere and will reach the relevant processing point only later.


Failure Scenarios
-----------------

The above strategy can fail in the following scenarios:

#. The bound ``B`` is too low: The command can be executed multiple times.
   
   Possible causes:

   - You have retried for longer than the deduplication duration, but never got a meaningful answer, e.g., because the timeout (gRPC deadline) is too short.
     For example, this can happen due to long-running Daml interpretation when using the :ref:`Command Service <command-service>`.

   - The application clock drifts significantly from the participant's or ledger's clock.

   - There are unexpected network delays.

   - Submissions are retried internally in the participant or Daml ledger and those retries do not stop before ``B`` is over.
     Refer to the specific ledger's documentation for more information.

#. Unacceptable changes cause infinite retries

   You need business workflow knowledge to decide that retrying does not make sense any more.
   Of course, you can always stop retrying and accept that you do not know the outcome for sure.


Unknown Processing Time Bounds
==============================

Finding a good bound ``B`` on the processing time is hard, and there may still be unforeseen circumstances that delay processing beyond the chosen bound ``B``.
You can avoid these problems by using deduplication offsets instead of durations.
An offset defines a point in the history of the ledger and is thus not affected by clock skews and network delays.
Offsets are arguably less intuitive and require more effort by the application developer.
We recommend the following strategy for using deduplication offsets:

#. Choose a fresh command ID for the ledger change and the ``actAs`` parties, which (together with the application ID) determine the change ID.
   Remember the command ID across application crashes.
   (Analogous to :ref:`Step 1 above <dedup-bounded-step-command-id>`)

   .. _dedup-unbounded-step-dedup-offset:
   
#. Obtain a recent offset ``OFF0`` on the completion event stream and remember across crashes that you use ``OFF0`` with the chosen command ID. There are several ways to do so:

   - Use the :ref:`Command Completion Service <command-completion-service>` by asking for the :ref:`current ledger end <com.daml.ledger.api.v1.CompletionEndRequest>`.

     .. note::
	Some ledger implementations reject deduplication offsets that do not identify a command completion visible to the submitting parties with the error code id :ref:`INVALID_DEDUPLICATION_PERIOD <error_code_INVALID_DEDUPLICATION_PERIOD>`.
	In general, the ledger end need not identify a command completion that is visible to the submitting parties.
	When running on such a ledger, use the Command Service approach described next.
   
   - Use the :ref:`Command Service <command-service>` to obtain a recent offset by repeatedly submitting a dummy command, e.g., a :ref:`Create-And-Exercise command <com.daml.ledger.api.v1.CreateAndExerciseCommand>` of some single-signatory template with the :ref:`Archive <function-da-internal-template-functions-archive-2977>` choice, until you get a successful response.
     The response contains the :ref:`completion offset <com.daml.ledger.api.v1.SubmitAndWaitForTransactionIdResponse.completion_offset>`.


   .. _dedup-unbounded-step-offset:

#. When you use the :ref:`Command Completion Service <command-submission-service>`:
   
   - If you execute this step the first time, set ``OFF1 = OFF0``.
   - If you execute this step as part of :ref:`error handling <dedup-unbounded-error-handling>` retrying from Step 3, obtaining the completion offset ``OFF1``,
     obtain a recent offset on the completion stream ``OFF1``, say its current end.
     (Analogous to :ref:`step 2 above <dedup-bounded-step-offset>`)

#. Submit the command with the following parameters (analogous to :ref:`Step 3 above <dedup-bounded-step-submit>` except for the deduplication period):

   - Set the :ref:`command ID <com.daml.ledger.api.v1.Commands.command_id>` to the chosen command ID from :ref:`Step 1 <dedup-bounded-step-command-id>`.

   - Set the :ref:`deduplication offset <com.daml.ledger.api.v1.Commands.deduplication_offset>` to ``OFF0``.

   - Set the :ref:`submission ID <com.daml.ledger.api.v1.Commands.submission_id>` to a fresh value, e.g., a random UUID.

   - Set the timeout (gRPC deadline) to the expected submission processing time (Command Service) or submission hand-off time (Command Submission Service).

#. Wait until the RPC call returns a response.
   
   - Status codes other than ``OK`` should be handled according to :ref:`error handling <dedup-bounded-error-handling>`.

   - When you use the :ref:`Command Service <command-service>` and the response carries the status code ``OK``, the ledger change took place.
     You can report success.
     The response contains a :ref:`completion offset <com.daml.ledger.api.v1.SubmitAndWaitForTransactionIdResponse.completion_offset>` that you can use in :ref:`Step 2 <dedup-unbounded-step-dedup-offset>` of later submissions.
     
   - When you use the :ref:`Command Submission Service <command-submission-service>`,
     subscribe with the :ref:`Command Completion Service <command-submission-service>` for completions for ``actAs`` from ``OFF1`` (exclusive) until you see a completion event for the change ID and the submission ID chosen in :ref:`step 3 <dedup-bounded-step-submit>`.
     If the completion’s status is ``OK``, the ledger change took place and you can report success.
     Other status codes should be handled according to :ref:`error handling <dedup-bounded-error-handling>`.

.. _dedup-unbounded-error-handling:

Error Handling
--------------

The same as :ref:`for known bounds <dedup-bounded-error-handling>`, except that the former retry from :ref:`Step 2 <dedup-bounded-step-offset>` becomes retry from :ref:`Step 3 <dedup-unbounded-step-offset>`.


Failure Scenarios
-----------------

The above strategy can fail in the following scenarios:

#. No success within the supported deduplication period
   
   When the application receives a :ref:`INVALID_DEDUPLICATION_PERIOD <error_code_INVALID_DEDUPLICATION_PERIOD>` error, it cannot achieve exactly once execution any more within the originally intended deduplication period.


#. Unacceptable changes cause infinite retries

   You need business workflow knowledge to decide that retrying does not make sense any more.
   Of course, you can always stop retrying and accept that you do not know the outcome for sure.




..
  Command deduplication on the JSON API
  *************************************
