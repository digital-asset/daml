.. Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _command-deduplication:

Command deduplication
#####################

The interaction of a Daml application with the ledger is inherently asynchronous: applications send commands to the ledger, and some time later they see the effect of that command on the ledger.
Many things can fail during this time window: the application can crash, the participant node can crash, messages can be lost on the network, or the ledger may be just slow to respond due to a high load.
If you want to make sure that an intended ledger change is not executed twice, your application needs to robustly handle these failure scenarios.
This guide covers the following topics:

#. How command deduplication works.

#. How applications can effectively use the command deduplication.

.. _command-dedup-workings:

How command deduplication works
*******************************

The following fields in a command submissions are relevant for command deduplication.
The first three form the :ref:`change ID <change-id>` that identifies the intended ledger change.

#. The union of :ref:`party <com.daml.ledger.api.v1.Commands.party>` and :ref:`act_as <com.daml.ledger.api.v1.Commands.act_as>` define the submitting parties.
  
#. The :ref:`application ID <com.daml.ledger.api.v1.Commands.application_id>` identifies the application that submits the command.

#. The :ref:`command ID <com.daml.ledger.api.v1.Commands.command_id>` is chosen by the application to identify the intended ledger change.

#. The :ref:`deduplication period <com.daml.ledger.api.v1.Commands.deduplication_period>` specifies the period for which no earlier submissions with the same change ID should have been accepted, as witnessed by a completion event on the :ref:`command completion service <command-completion-service>`.
   Otherwise, the current submission shall be rejected.
   The period is specified either as a :ref:`deduplication duration <com.daml.ledger.api.v1.Commands.deduplication_duration>` or as a :ref:`deduplication offset <com.daml.ledger.api.v1.Commands.deduplication_offset>`.

#. The :ref:`submission ID <com.daml.ledger.api.v1.Commands.submission_id>` is returned to the submitting application and allows it to correlate specific submissions to specific completions.
   An application should never reuse a submission ID.

The ledger may arbitrarily extend the deduplication period specified in the submission, even beyond the maximum deduplication time specified in the :ref:`ledger configuration <ledger-configuration-service>`.
The deduplication period chosen by the ledger is the *effective deduplication period*.
The ledger may also convert a requested deduplication duration into an effective deduplication offset or vice versa.
The effective deduplication period is reported in the the command completion event in the :ref:`deduplication duration <com.daml.ledger.api.v1.Completion.deduplication_duration>` or :ref:`deduplication offset <com.daml.ledger.api.v1.Completion.deduplication_offset>` fields.

A command submission is considered a **duplicate submission** if at least one of the following holds:

- The submitting participant's completion service contains a successful completion event for the same :ref:`change ID <change-id>` within the *effective* deduplication period.

- The participant or Daml ledger are aware of another command submission in-flight with the same :ref:`change ID <change-id>` when they performs command deduplication.

Command submissions via the :ref:`command service <command-service>` indicate the command deduplication outcome as a synchronous gRPC response (except when the gRPC deadline was exceeded before).
Submissions via the :ref:`command submission service <command-submission-service>` can indicate the outcome synchronously or asynchronously in the event on the :ref:`command completion service <command-completion-service>`.
In particular, the submission may be a duplicate even if the command submission service acknowledges the submission with the gRPC status code ``OK``.
Command deduplication generates the following outcomes of a command submission:

- If no conflicting submission with the same :ref:`change ID <change-id>` was found, the completion event and the response convey the result of the submission (success or a gRPC error).

- The gRPC status code ``ALREADY_EXISTS`` with error code id :ref:`DUPLICATE_COMMAND <error_code_DUPLICATE_COMMAND>` if there is an earlier command completion for the same :ref:`change ID <change-id>` within the effective deduplication period.

- The gRPC status code ``ABORTED`` with error code id :ref:`SUBMISSION_ALREADY_IN_FLIGHT <error_code_SUBMISSION_ALREADY_IN_FLIGHT>` if another submission for the same :ref:`change ID <change-id>` was in flight when this submission was processed.

- The gRPC status code ``FAILED_PRECONDITION`` with error code id :ref:`INVALID_DEDUPLICATION_PERIOD <error_code_INVALID_DEDUPLICATION_PERIOD>` if the specified deduplication period is not supported.
  The fields ``longest_duration`` or ``earliest_offset`` in the metadata specify the longest duration or earliest offset that is currently supported on the Ledger API endpoint.
  At least one of the two fields is present.

  Deduplication periods that span at most the :ref:`maximum deduplication time <com.daml.ledger.api.v1.LedgerConfiguration.max_deduplication_time>` SHOULD not result in this error.
  Participants may accept longer periods at their discretion.

For deduplication to work as intended, all submissions for the same ledger change must be submitted via the same participants.
This is because a participant outputs by default only the completion events for submissions that were requested via this participant;
and whether a submission is considered a duplicate is determined by the completion events.

On some ledgers, every participant outputs the completion events for all the hosted parties, not just for the submissions that went through the participant.
In this case, command deduplication works across participants.
At this time, only `Daml Driver for VMware Blockchain <https://www.digitalasset.com/daml-for-vmware-blockchain/>`__ supports command deduplication across participants.

    

How to use command deduplication
********************************

When an application wants to effectuate a ledger change exactly once, the application must resubmit a command if an earlier submission was lost.
However, the application typically cannot distinguish a lost submission from a Daml ledger that is just slow in processing the submissions.
So the application should just resubmit the command until it is executed against the ledger, and rely on command deduplication to reject all duplicate submissions that do make it to the ledger.

Some ledger changes can be executed at most once anyway.
No command deduplication is needed for them.
For example, if the submitted command exercises a consuming choice on a given contract ID, this command can be accepted at most once because every contract can be archived at most once.
All duplicate submissions are rejected with :ref:`CONTRACT_NOT_ACTIVE <error_code_CONTRACT_NOT_ACTIVE>`.

In contrast, a :ref:`Create command <com.daml.ledger.api.v1.CreateCommand>` would create a fresh contract instance of the given :ref:`template <com.daml.ledger.api.v1.CreateCommand.template_id>` for each submission that reaches the ledger (unless other constraints such as the :ref:`template preconditions <daml-ref-preconditions>` or contract key uniqueness are violated).
Similarly, an :ref:`Exercise command <com.daml.ledger.api.v1.ExerciseCommand>` on a non-consuming choice or an :ref:`Exercise-By-Key command <com.daml.ledger.api.v1.ExercisebyKeyCommand>` may be executed multiple times if submitted multiple times.
With command deduplication, applications can ensure such intended ledger changes are executed only once, even if the application resubmits within the deduplication period, say because it considers the earlier submissions to be lost or forgot during a crash that it had already submitted the command.

As a typical example, the application wants to create exactly one contract instance of template ``T`` via the corresponding :ref:`Create command <com.daml.ledger.api.v1.CreateCommand>`.
We assume that the ledger is correctly set up so that the Create command should be accepted, i.e., Daml packages are uploaded and vetted, the parties are allocated, and the application has a valid access token for the submission.


Known processing time bounds
============================

For now, we assume that the application knows a bound ``B`` on the processing time and forward clock drifts (against the application’s clock) in the Daml ledger.
If processing measured across all retries takes longer than the bound ``B``, the ledger change may take effect several times.
Under this assumption, the following strategy works for applications that use the :ref:`Command Service <command-service>` or the :ref:`Command Submission <command-submission-service>` and :ref:`Command Completion Service <command-completion-service>`.

.. _dedup-bounded-step-command-id:

#. Choose a fresh command ID for the ledger change and the ``actAs`` parties, which (together with the application ID) determine the change ID.
   Remember the command ID across application crashes.

   .. note::
      Make sure that you set command ID deterministically, that is to say: the same ledger change must use the same command ID for all command submissions.
      This is useful for the recovery procedure after an application crash/restart, in which the application inspects the state of the ledger (e.g., via the :ref:`Active contracts service <active-contract-service>`) and sends commands to the ledger.
      When using deterministic command IDs, any commands that had been sent before the application restart will be discarded by the ledger to avoid duplicate submissions.

   .. _dedup-bounded-step-offset:

#. When you use the :ref:`Command Completion Service <command-submission-service>`, obtain a recent offset on the completion stream ``OFF1``, say the :ref:`current ledger end <com.daml.ledger.api.v1.CommandCompletionService.CompletionEnd>`.

   .. _dedup-bounded-step-submit:
   
#. Submit the command with the following parameters

   - Set the :ref:`command ID <<com.daml.ledger.api.v1.Commands.command_id>>` to the chosen command ID from :ref:`step 1 <dedup-bounded-step-command-id>`.

   - Set the :ref:`deduplication duration <com.daml.ledger.api.v1.Commands.deduplication_duration>` to the bound ``B``.

   - Set the :ref:`submission ID <com.daml.ledger.api.v1.Commands.submission_id>` to a fresh value, e.g., a random UUID.

   - Set the timeout (gRPC deadline) to the expected processing (Command Service) or submission (Command Submission Service) delay.

     The **processing delay** measures the time between when the application sends off a submission to the :ref:`Command Service <command-service>` and when it receives the acceptance or rejection.
     The **submission delay** measures the time between when the application sends off a submission to the :ref:`Command Submission Service <command-submission-service>` and when it obtains a synchronous response for this gRPC call.
     After the RPC timeout, the application considers the submission as lost and enters a retry loop.
     This timeout is typically much shorter than the deduplication duration.

   .. _dedup-bounded-step-await:
   
#. Wait until the RPC call returns a response.
   
   - Status codes other than ``OK`` trigger :ref:`error handling <dedup-bounded-error-handling>`.

   - When you use the :ref:`Command Service <command-service>` and the response carries the status code ``OK``, the ledger change took place.
     You can report success.
     
   - When you use the :ref:`Command Completion Service <command-completion-service>`,
     subscribe with the :ref:`Command Completion Service <command-submission-service>` for completions for ``actAs`` from ``OFF1`` exclusive until you see a completion event for the change ID and the submission ID chosen in :ref:`step 3 <dedup-bounded-step-submit>`.
     If the completion’s status is ``OK``, the ledger change took place and you can report success.
     Other status codes trigger :ref:`error handling <dedup-bounded-error-handling>`.
   
     This step needs no timeout as the :ref:`Command Submission Service <command-submission-service>` acknowledges a submission only if there will eventually be a completion event, unless relevant parts of the system become permanently unavailable.


.. _dedup-bounded-error-handling:

Error handling
--------------

Error handling is needed when the status code of the RPC call or in the :ref:`in the completion event <com.daml.ledger.api.v1Completion.status>` is not ``OK``.
The following table lists appropriate reactions by status code (written as ``STATUS_CODE``) and error code (written in capital letters with a link to the error code documentation).
Fields in the error metadata are written as ``field`` in lowercase letters.

.. list-table:: Command deduplication error handling with known processing time bound
   :widths: 10 50
   :header-rows: 1

   - * Error condition
     
     * Reaction

       
   - * ``DEADLINE_EXCEEDED``
     
     * Consider the submission as lost.
       
       Retry from obtaining a completion offset ``OFF1`` (:ref:`step 2 <dedup-bounded-step-offset>`) and possibly increase the timeout.

       
   - * Application crashed
     
     * Retry from obtaining a completion offset (:ref:`step 2 <dedup-bounded-step-offset>`).


   - * ``ALREADY_EXISTS`` / :ref:`DUPLICATE_COMMAND <error_code_DUPLICATE_COMMAND>`
     
     * The change ID has already been accepted on the ledger within the reported deduplication period.
       The optional field ``completion_offset`` contains the precise offset.
       Report success for the ledger change.
       
       If desired, query the ``completion_offset`` via the :ref:`Command Completion Service <command-submission-service>` to find out about the earlier outcome.

       
   - * ``FAILED_PRECONDITION`` / :ref:`INVALID_DEDUPLICATION_PERIOD <error_code_INVALID_DEDUPLICATION_PERIOD>`
     
     * The specified deduplication duration is longer than what the Daml ledger supports.
       ``earliest_offset`` contains the earliest deduplication offset or ``longest_duration`` contains the longest deduplication duration that can be used (at least one of the two must be provided).

       Options:

       - Negotiate support for longer deduplication periods with the ledger operator.

       - Set the deduplication offset to ``earliest_offset`` or the deduplication duration to ``longest_duration`` and retry from obtaining a completion offset (:ref:`step 2 <dedup-bounded-step-offset>`).
	 This may lead to accepting the change twice.

	 
   - * ``ABORTED`` / :ref:`SUBMISSION_ALREADY_IN_FLIGHT <error_code_SUBMISSION_ALREADY_IN_FLIGHT>`
     
       This error occurs only as an RPC response, not inside a completion event.
       
     * There is already another submission in flight, with the submission ID in ``existing_submission_id``.

       - When you use the :ref:`Command Service <command-service>`, wait a bit and retry from submitting the command (:ref:`step 3 <dedup-bounded-step-submit>`).

       - When you use the :ref:`Command Completion Service <command-completion-service>`, look for a completion for ``existing_submission_id`` instead of the chosen submission ID in :ref:`step 4 <dedup-bounded-step-await>`.


   - * ``ABORTED`` / other error codes
     
     * Wait a bit and retry from obtaining a completion offset ``OFF1`` (:ref:`step 2 <dedup-bounded-step-offset>`).
       Use the retryability information in the error details to decide how long to wait.

       
   - * other error conditions

     * You should use background knowledge about the workflow to decide whether earlier submissions might still get accepted.
       If not, you may stop retrying and report that the ledger change failed.
       If in doubt, retry from obtaining a completion offset ``OFF1`` (:ref:`step 2 <dedup-bounded-step-offset>`) or give up without knowing for sure that the ledger change will not happen.

       In the running example of creating a contract instance of ``T``, you can never be sure, as any outstanding submission might still be accepted on the ledger.
       In particular, you must not draw any conclusions from not having received a :ref:`SUBMISSION_ALREADY_IN_FLIGHT <error_code_SUBMISSION_ALREADY_IN_FLIGHT>` error, because the outstanding submission may be queued somewhere and will reach the relevant processing point only later.

Failure scenarios
-----------------

The above strategy can fail in the following scenarios:

#. The bound ``B`` is too low: The command can be executed multiple times.
   
   Possible causes:

   - You have retried for longer than the deduplication duration, but never got a meaningful answer, e.g., because the timeout (gRPC deadline) is too short.
     For example, this can happen due to long-running Daml interpretation when using the :ref:`Command Service <command-service>`.

   - The application clock drifts significantly from the participant's or ledger's clock.

   - There are unexpected network delays.

   - Submissions are retried internally in the participant or Daml ledger and those retries do not stop before ``B`` is over.

#. Unacceptable changes cause infinite retries

   You need business workflow knowledge to decide that retrying does not make sense any more.
   Of course, you can always stop retrying and accept that you do not know the outcome for sure.


Unknown processing time bounds
==============================

Finding a good bound ``B`` on the processing time is hard, and there may still be unforeseen circumstances that delay processing beyond the chosen bound ``B``.
You can avoid these problems by using deduplication offsets instead of durations.
An offset defines a point in the history of the ledger and is thus not affected by clock skews and network delays.
Offsets are arguably less intuitive and require more effort by the application developer.
We recommend the following strategy for using deduplication offsets:

#. Choose a fresh command ID for the ledger change and the ``actAs`` parties, which (together with the application ID) determine the change ID.
   Remember the command ID across application crashes.
   (Analogous to :ref:`step 1 above <dedup-bounded-step-command-id>`)

   .. _dedup-unbounded-step-dedup-offset:
   
#. Obtain a recent offset ``OFF0`` on the completion event stream and remember across crashes that you use ``OFF0`` with the chosen command ID.

   - When you use the :ref:`Command Service <command-service>`, to obtain a recent offset, repeatedly submit a dummy command, e.g., a :ref:`Create-And-Exercise command <com.daml.ledger.api.v1.CreateAndExerciseCommand>` of some single-signatory template with the :ref:`Archive <function-da-internal-template-functions-archive-52202>` choice, until you get a successful response.
     The response contains the :ref:`completion offset <com.daml.ledger.api.v1.SubmitAndWaitForTransactionIdResponse.completion_offset>`.

   - When you use the :ref:`Command Completion Service <command-completion-service>`, ask for the :ref:`current ledger end <com.daml.ledger.api.v1.CommandCompletionService.CompletionEnd>`.

   .. _dedup-unbounded-step-offset:

#. When you use the :ref:`Command Completion Service <command-submission-service>`, obtain a recent offset on the completion stream ``OFF1``, say its current end.
   (Analogous to :ref:`step 2 above <dedup-bounded-step-offset>`)

   .. note::
      Unless error handling retries from this step, you can use ``OFF1 = OFF0``.

#. Submit the command with the following parameters (analogous to :ref:`step 3 above <dedup-bounded-step-submit>` except for the deduplication period):

   - Set the :ref:`command ID <<com.daml.ledger.api.v1.Commands.command_id>>` to the chosen command ID from :ref:`step 1 <dedup-bounded-step-command-id>`.

   - Set the :ref:`deduplication offset <com.daml.ledger.api.v1.Commands.deduplication_offset>` to ``OFF0``.

   - Set the :ref:`submission ID <com.daml.ledger.api.v1.Commands.submission_id>` to a fresh value, e.g., a random UUID.

   - Set the timeout (gRPC deadline) to the expected processing (Command Service) or submission (Command Submission Service) delay.

#. Wait until the RPC call returns a response.
   
   - Status codes other than ``OK`` trigger :ref:`error handling <dedup-bounded-error-handling>`.

   - When you use the :ref:`Command Service <command-service>` and the response carries the status code ``OK``, the ledger change took place.
     You can report success.
     The response contains a :ref:`completion offset <com.daml.ledger.api.v1.SubmitAndWaitForTransactionIdResponse.completion_offset>` that you can use in :ref:`step 2 <dedup-unbounded-step-dedup-offset>` of later submissions.
     
   - When you use the :ref:`Command Completion Service <command-completion-service>`,
     subscribe with the :ref:`Command Completion Service <command-submission-service>` for completions for ``actAs`` from ``OFF1`` exclusive until you see a completion event for the change ID and the submission ID chosen in :ref:`step 3 <dedup-bounded-step-submit>`.
     If the completion’s status is ``OK``, the ledger change took place and you can report success.
     Other status codes trigger :ref:`error handling <dedup-bounded-error-handling>`.
   

Error handling
--------------

The same as :ref:`for known bounds <dedup-bounded-error-handling>`, except that the former retry from :ref:`step 2 <dedup-bounded-step-offset>` becomes retry from :ref:`step 3 <dedup-unbounded-step-offset>`.


Failure scenarios
-----------------

The above strategy can fail in the following scenarios:

#. No success within the supported deduplication period
   
   When the application receives a :ref:`INVALID_DEDUPLICATION_PERIOD <error_code_INVALID_DEDUPLICATION_PERIOD>` error, it cannot achieve exactly once execution any more.


#. Unacceptable changes cause infinite retries

   You need business workflow knowledge to decide that retrying does not make sense any more.
   Of course, you can always stop retrying and accept that you do not know the outcome for sure.




.. todo:: 
  Command deduplication on the JSON API
  *************************************




