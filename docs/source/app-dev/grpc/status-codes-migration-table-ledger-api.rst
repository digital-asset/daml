.. Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

ActiveContractsService.getActiveContracts
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

+------------------+--------------------+---------------------------------------------------------------------------------------------------------+---------------------------------+
|gRPC status code  |gRPC status code    |Remarks                                                                                                  |Ledger API                       |
|(before SDK 1.18) |(since SDK 1.18)    |                                                                                                         |error code id                    |
+==================+====================+=========================================================================================================+=================================+
|NOT_FOUND         |FAILED_PRECONDITION |FAILED_PRECONDITION can now be returned when attempting to access the data that has already been pruned. |PARTICIPANT_PRUNED_DATA_ACCESSED |
+------------------+--------------------+---------------------------------------------------------------------------------------------------------+---------------------------------+
|NOT_FOUND         |NOT_FOUND           |The ledger id from the request does match the participant's ledger id.                                   |LEDGER_ID_MISMATCH               |
+------------------+--------------------+---------------------------------------------------------------------------------------------------------+---------------------------------+


CommandCompletionService.completionStream
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

+------------------+--------------------+---------------------------------------------------------------------------------------------------------+---------------------------------+
|gRPC status code  |gRPC status code    |Remarks                                                                                                  |Ledger API                       |
|(before SDK 1.18) |(since SDK 1.18)    |                                                                                                         |error code id                    |
+==================+====================+=========================================================================================================+=================================+
|NOT_FOUND         |FAILED_PRECONDITION |FAILED_PRECONDITION can now be returned when attempting to access the data that has already been pruned. |PARTICIPANT_PRUNED_DATA_ACCESSED |
+------------------+--------------------+---------------------------------------------------------------------------------------------------------+---------------------------------+
|NOT_FOUND         |NOT_FOUND           |The ledger id from the request does match the participant's ledger id.                                   |LEDGER_ID_MISMATCH               |
+------------------+--------------------+---------------------------------------------------------------------------------------------------------+---------------------------------+


CommandService.submitAndWait
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

+-------------------+------------------+------------------------------------------------------------------------------+-------------------------------+
|gRPC status code   |gRPC status code  |Remarks                                                                       |Ledger API                     |
|(before SDK 1.18)  |(since SDK 1.18)  |                                                                              |error code id                  |
+===================+==================+==============================================================================+===============================+
|ABORTED            |DEADLINE_EXCEEDED |DEADLINE_EXCEEDED is now returned on Command Service submissions timeouts.    |REQUEST_TIME_OUT               |
+-------------------+------------------+------------------------------------------------------------------------------+-------------------------------+
|ABORTED            |INTERNAL          |INTERNAL is not returned on Command Service submissions on unexpected errors. |LEDGER_API_INTERNAL_ERROR      |
+-------------------+------------------+------------------------------------------------------------------------------+-------------------------------+
|ABORTED            |UNAVAILABLE       |UNAVAILABLE is now returned on Command Service submissions on backpressure.   |SERVICE_NOT_RUNNING            |
+-------------------+------------------+------------------------------------------------------------------------------+-------------------------------+
|RESOURCE_EXHAUSTED |ABORTED           |ABORTED is now returned on Command Service submissions on backpressure.       |PARTICIPANT_BACKPRESSURE       |
+-------------------+------------------+------------------------------------------------------------------------------+-------------------------------+
|UNAVAILABLE        |NOT_FOUND         |NOT_FOUND can now be returned when a ledger configuration was not found.      |LEDGER_CONFIGURATION_NOT_FOUND |
+-------------------+------------------+------------------------------------------------------------------------------+-------------------------------+
|UNAVAILABLE        |UNAVAILABLE       |A service is not running.                                                     |SERVICE_NOT_RUNNING            |
+-------------------+------------------+------------------------------------------------------------------------------+-------------------------------+


CommandService.submitAndWaitForTransaction
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

+-------------------+------------------+------------------------------------------------------------------------------+-------------------------------+
|gRPC status code   |gRPC status code  |Remarks                                                                       |Ledger API                     |
|(before SDK 1.18)  |(since SDK 1.18)  |                                                                              |error code id                  |
+===================+==================+==============================================================================+===============================+
|ABORTED            |DEADLINE_EXCEEDED |DEADLINE_EXCEEDED is now returned on Command Service submissions timeouts.    |REQUEST_TIME_OUT               |
+-------------------+------------------+------------------------------------------------------------------------------+-------------------------------+
|ABORTED            |INTERNAL          |INTERNAL is not returned on Command Service submissions on unexpected errors. |LEDGER_API_INTERNAL_ERROR      |
+-------------------+------------------+------------------------------------------------------------------------------+-------------------------------+
|ABORTED            |UNAVAILABLE       |UNAVAILABLE is now returned on Command Service submissions on backpressure.   |SERVICE_NOT_RUNNING            |
+-------------------+------------------+------------------------------------------------------------------------------+-------------------------------+
|RESOURCE_EXHAUSTED |ABORTED           |ABORTED is now returned on Command Service submissions on backpressure.       |PARTICIPANT_BACKPRESSURE       |
+-------------------+------------------+------------------------------------------------------------------------------+-------------------------------+
|UNAVAILABLE        |NOT_FOUND         |NOT_FOUND can now be returned when a ledger configuration was not found.      |LEDGER_CONFIGURATION_NOT_FOUND |
+-------------------+------------------+------------------------------------------------------------------------------+-------------------------------+
|UNAVAILABLE        |UNAVAILABLE       |A service is not running.                                                     |SERVICE_NOT_RUNNING            |
+-------------------+------------------+------------------------------------------------------------------------------+-------------------------------+


CommandService.submitAndWaitForTransactionId
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

+-------------------+------------------+------------------------------------------------------------------------------+-------------------------------+
|gRPC status code   |gRPC status code  |Remarks                                                                       |Ledger API                     |
|(before SDK 1.18)  |(since SDK 1.18)  |                                                                              |error code id                  |
+===================+==================+==============================================================================+===============================+
|ABORTED            |DEADLINE_EXCEEDED |DEADLINE_EXCEEDED is now returned on Command Service submissions timeouts.    |REQUEST_TIME_OUT               |
+-------------------+------------------+------------------------------------------------------------------------------+-------------------------------+
|ABORTED            |INTERNAL          |INTERNAL is not returned on Command Service submissions on unexpected errors. |LEDGER_API_INTERNAL_ERROR      |
+-------------------+------------------+------------------------------------------------------------------------------+-------------------------------+
|ABORTED            |UNAVAILABLE       |UNAVAILABLE is now returned on Command Service submissions on backpressure.   |SERVICE_NOT_RUNNING            |
+-------------------+------------------+------------------------------------------------------------------------------+-------------------------------+
|RESOURCE_EXHAUSTED |ABORTED           |ABORTED is now returned on Command Service submissions on backpressure.       |PARTICIPANT_BACKPRESSURE       |
+-------------------+------------------+------------------------------------------------------------------------------+-------------------------------+
|UNAVAILABLE        |NOT_FOUND         |NOT_FOUND can now be returned when a ledger configuration was not found.      |LEDGER_CONFIGURATION_NOT_FOUND |
+-------------------+------------------+------------------------------------------------------------------------------+-------------------------------+
|UNAVAILABLE        |UNAVAILABLE       |A service is not running.                                                     |SERVICE_NOT_RUNNING            |
+-------------------+------------------+------------------------------------------------------------------------------+-------------------------------+


CommandService.submitAndWaitForTransactionTree
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

+-------------------+------------------+------------------------------------------------------------------------------+-------------------------------+
|gRPC status code   |gRPC status code  |Remarks                                                                       |Ledger API                     |
|(before SDK 1.18)  |(since SDK 1.18)  |                                                                              |error code id                  |
+===================+==================+==============================================================================+===============================+
|ABORTED            |DEADLINE_EXCEEDED |DEADLINE_EXCEEDED is now returned on Command Service submissions timeouts.    |REQUEST_TIME_OUT               |
+-------------------+------------------+------------------------------------------------------------------------------+-------------------------------+
|ABORTED            |INTERNAL          |INTERNAL is not returned on Command Service submissions on unexpected errors. |LEDGER_API_INTERNAL_ERROR      |
+-------------------+------------------+------------------------------------------------------------------------------+-------------------------------+
|ABORTED            |UNAVAILABLE       |UNAVAILABLE is now returned on Command Service submissions on backpressure.   |SERVICE_NOT_RUNNING            |
+-------------------+------------------+------------------------------------------------------------------------------+-------------------------------+
|RESOURCE_EXHAUSTED |ABORTED           |ABORTED is now returned on Command Service submissions on backpressure.       |PARTICIPANT_BACKPRESSURE       |
+-------------------+------------------+------------------------------------------------------------------------------+-------------------------------+
|UNAVAILABLE        |NOT_FOUND         |NOT_FOUND can now be returned when a ledger configuration was not found.      |LEDGER_CONFIGURATION_NOT_FOUND |
+-------------------+------------------+------------------------------------------------------------------------------+-------------------------------+
|UNAVAILABLE        |UNAVAILABLE       |A service is not running.                                                     |SERVICE_NOT_RUNNING            |
+-------------------+------------------+------------------------------------------------------------------------------+-------------------------------+


CommandSubmissionService.submit
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

+------------------+-----------------+-------------------------------------------------------------------------+-------------------------------+
|gRPC status code  |gRPC status code |Remarks                                                                  |Ledger API                     |
|(before SDK 1.18) |(since SDK 1.18) |                                                                         |error code id                  |
+==================+=================+=========================================================================+===============================+
|UNAVAILABLE       |NOT_FOUND        |NOT_FOUND can now be returned when a ledger configuration was not found. |LEDGER_CONFIGURATION_NOT_FOUND |
+------------------+-----------------+-------------------------------------------------------------------------+-------------------------------+


ConfigManagementService.setTimeModel
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

+------------------+--------------------+----------------------------------------------------------------------------------+-------------------------------+
|gRPC status code  |gRPC status code    |Remarks                                                                           |Ledger API                     |
|(before SDK 1.18) |(since SDK 1.18)    |                                                                                  |error code id                  |
+==================+====================+==================================================================================+===============================+
|ABORTED           |DEADLINE_EXCEEDED   |DEADLINE_EXCEEDED can now be returned when a time out was reached.                |REQUEST_TIME_OUT               |
+------------------+--------------------+----------------------------------------------------------------------------------+-------------------------------+
|ABORTED           |FAILED_PRECONDITION |FAILED_PRECONDITION can now be returned when a configuration update was rejected. |CONFIGURATION_ENTRY_REJECTED   |
+------------------+--------------------+----------------------------------------------------------------------------------+-------------------------------+
|UNAVAILABLE       |NOT_FOUND           |NOT_FOUND can now be returned when a ledger configuration was not found.          |LEDGER_CONFIGURATION_NOT_FOUND |
+------------------+--------------------+----------------------------------------------------------------------------------+-------------------------------+


GrpcHealthService.check
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

+------------------+-----------------+----------------------------------------------------------------------------------------+-----------------+
|gRPC status code  |gRPC status code |Remarks                                                                                 |Ledger API       |
|(before SDK 1.18) |(since SDK 1.18) |                                                                                        |error code id    |
+==================+=================+========================================================================================+=================+
|NOT_FOUND         |INVALID_ARGUMENT |INVALID_ARGUMENT can now be returned when the received request contains invalid values. |INVALID_ARGUMENT |
+------------------+-----------------+----------------------------------------------------------------------------------------+-----------------+


GrpcHealthService.watch
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

+------------------+-----------------+----------------------------------------------------------------------------------------+-----------------+
|gRPC status code  |gRPC status code |Remarks                                                                                 |Ledger API       |
|(before SDK 1.18) |(since SDK 1.18) |                                                                                        |error code id    |
+==================+=================+========================================================================================+=================+
|NOT_FOUND         |INVALID_ARGUMENT |INVALID_ARGUMENT can now be returned when the received request contains invalid values. |INVALID_ARGUMENT |
+------------------+-----------------+----------------------------------------------------------------------------------------+-----------------+


PackageManagementService.uploadDarFile
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

+------------------+--------------------+----------------------------------------------------------------------------+------------------------+
|gRPC status code  |gRPC status code    |Remarks                                                                     |Ledger API              |
|(before SDK 1.18) |(since SDK 1.18)    |                                                                            |error code id           |
+==================+====================+============================================================================+========================+
|ABORTED           |DEADLINE_EXCEEDED   |DEADLINE_EXCEEDED can now be returned when a time out was reached.          |REQUEST_TIME_OUT        |
+------------------+--------------------+----------------------------------------------------------------------------+------------------------+
|INVALID_ARGUMENT  |FAILED_PRECONDITION |FAILED_PRECONDITION can now be returned when a package upload was rejected. |PACKAGE_UPLOAD_REJECTED |
+------------------+--------------------+----------------------------------------------------------------------------+------------------------+
|INVALID_ARGUMENT  |INVALID_ARGUMENT    |Generic error for invalid arguments in the request.                         |INVALID_ARGUMENT        |
+------------------+--------------------+----------------------------------------------------------------------------+------------------------+


ParticipantPruningService.prune
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

+------------------+--------------------+-------------------------------------------------------------------------------------------------------+-----------------------+
|gRPC status code  |gRPC status code    |Remarks                                                                                                |Ledger API             |
|(before SDK 1.18) |(since SDK 1.18)    |                                                                                                       |error code id          |
+==================+====================+=======================================================================================================+=======================+
|INVALID_ARGUMENT  |FAILED_PRECONDITION |FAILED_PRECONDITION can now be returned when the supplied pruning offset is not before the ledger end. |OFFSET_OUT_OF_RANGE    |
+------------------+--------------------+-------------------------------------------------------------------------------------------------------+-----------------------+
|INVALID_ARGUMENT  |INVALID_ARGUMENT    |Generic error for invalid arguments in the request.                                                    |INVALID_ARGUMENT       |
+------------------+--------------------+-------------------------------------------------------------------------------------------------------+-----------------------+
|INVALID_ARGUMENT  |INVALID_ARGUMENT    |The offset is not in hexadecimal format.                                                               |NON_HEXADECIMAL_OFFSET |
+------------------+--------------------+-------------------------------------------------------------------------------------------------------+-----------------------+


PartyManagementService.allocateParty
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

+------------------+------------------+-------------------------------------------------------------------+-----------------+
|gRPC status code  |gRPC status code  |Remarks                                                            |Ledger API       |
|(before SDK 1.18) |(since SDK 1.18)  |                                                                   |error code id    |
+==================+==================+===================================================================+=================+
|ABORTED           |DEADLINE_EXCEEDED |DEADLINE_EXCEEDED can now be returned when a time out was reached. |REQUEST_TIME_OUT |
+------------------+------------------+-------------------------------------------------------------------+-----------------+


SubmissionService.submit
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

+------------------+--------------------+-------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------+
|gRPC status code  |gRPC status code    |Remarks                                                                                                                  |Ledger API                                                                        |
|(before SDK 1.18) |(since SDK 1.18)    |                                                                                                                         |error code id                                                                     |
+==================+====================+=========================================================================================================================+==================================================================================+
|ABORTED           |ABORTED             |Failed to determine ledger time.                                                                                         |FAILED_TO_DETERMINE_LEDGER_TIME                                                   |
+------------------+--------------------+-------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------+
|ABORTED           |ALREADY_EXISTS      |ALREADY_EXISTS can now be returned when there was a duplicate contract key during interpretation.                        |DUPLICATE_CONTRACT_KEY_DURING_INTERPRETATION                                      |
+------------------+--------------------+-------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------+
|ABORTED           |INTERNAL            |INTERNAL can now be returned when validation fails on a mismatch during relay of the submitted transaction.              |LEDGER_API_INTERNAL_ERROR                                                         |
+------------------+--------------------+-------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------+
|ABORTED           |NOT_FOUND           |NOT_FOUND can now be returned when contract key was not found during interpretation.                                     |CONTRACT_NOT_FOUND                                                                |
+------------------+--------------------+-------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------+
|INVALID_ARGUMENT  |FAILED_PRECONDITION |FAILED_PRECONDITION can now be returned when a Daml transaction fails during interpretation.                             |DAML_INTERPRETATION_ERROR                                                         |
+------------------+--------------------+-------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------+
|INVALID_ARGUMENT  |INTERNAL            |INTERNAL can now be returned in case of internal errors.                                                                 |LEDGER_API_INTERNAL_ERROR                                                         |
+------------------+--------------------+-------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------+
|INVALID_ARGUMENT  |INVALID_ARGUMENT    |Invalid argument detected before command execution.                                                                      |ALLOWED_LANGUAGE_VERSIONS, COMMAND_PREPROCESSING_FAILED, DAML_AUTHORIZATION_ERROR |
+------------------+--------------------+-------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------+
|INVALID_ARGUMENT  |INVALID_ARGUMENT    |Invalid argument detected by the Daml interpreter.                                                                       |DAML_INTERPRETER_INVALID_ARGUMENT                                                 |
+------------------+--------------------+-------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------+
|INVALID_ARGUMENT  |NOT_FOUND           |NOT_FOUND can now be returned when a Daml interpreter can not resolve a contract key to an active contract.              |CONTRACT_KEY_NOT_FOUND                                                            |
+------------------+--------------------+-------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------+
|INVALID_ARGUMENT  |NOT_FOUND           |NOT_FOUND can now be returned when a Daml transaction was referring to a package which was not known to the participant. |MISSING_PACKAGE                                                                   |
+------------------+--------------------+-------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------+
|INVALID_ARGUMENT  |NOT_FOUND           |NOT_FOUND can now be returned when an exercise or fetch happens on a transaction-locally consumed contract.              |CONTRACT_NOT_ACTIVE                                                               |
+------------------+--------------------+-------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------+
|INVALID_ARGUMENT  |UNKNOWN             |UNKNOWN can now be returned when package validation fails.                                                               |PACKAGE_VALIDATION_FAILED                                                         |
+------------------+--------------------+-------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------+
|UNAVAILABLE       |NOT_FOUND           |NOT_FOUND can now be returned when a ledger configuration was not found.                                                 |LEDGER_CONFIGURATION_NOT_FOUND                                                    |
+------------------+--------------------+-------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------+


TransactionService.getFlatTransactionByEventId
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

+------------------+-----------------+----------------------------------------------------------------------------------------+-------------------+
|gRPC status code  |gRPC status code |Remarks                                                                                 |Ledger API         |
|(before SDK 1.18) |(since SDK 1.18) |                                                                                        |error code id      |
+==================+=================+========================================================================================+===================+
|NOT_FOUND         |INVALID_ARGUMENT |INVALID_ARGUMENT can now be returned when the received request contains invalid values. |INVALID_ARGUMENT   |
+------------------+-----------------+----------------------------------------------------------------------------------------+-------------------+
|NOT_FOUND         |NOT_FOUND        |The ledger id from the request does match the participant's ledger id.                  |LEDGER_ID_MISMATCH |
+------------------+-----------------+----------------------------------------------------------------------------------------+-------------------+


TransactionService.getTransactionByEventId
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

+------------------+-----------------+----------------------------------------------------------------------------------------+----------------------+
|gRPC status code  |gRPC status code |Remarks                                                                                 |Ledger API            |
|(before SDK 1.18) |(since SDK 1.18) |                                                                                        |error code id         |
+==================+=================+========================================================================================+======================+
|NOT_FOUND         |INVALID_ARGUMENT |INVALID_ARGUMENT can now be returned when the received request contains invalid values. |INVALID_ARGUMENT      |
+------------------+-----------------+----------------------------------------------------------------------------------------+----------------------+
|NOT_FOUND         |NOT_FOUND        |The ledger id from the request does match the participant's ledger id.                  |LEDGER_ID_MISMATCH    |
+------------------+-----------------+----------------------------------------------------------------------------------------+----------------------+
|NOT_FOUND         |NOT_FOUND        |Transaction was not found.                                                              |TRANSACTION_NOT_FOUND |
+------------------+-----------------+----------------------------------------------------------------------------------------+----------------------+


TransactionService.getTransactionTrees
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

+------------------+--------------------+---------------------------------------------------------------------------------------------------------+---------------------------------+
|gRPC status code  |gRPC status code    |Remarks                                                                                                  |Ledger API                       |
|(before SDK 1.18) |(since SDK 1.18)    |                                                                                                         |error code id                    |
+==================+====================+=========================================================================================================+=================================+
|INVALID_ARGUMENT  |FAILED_PRECONDITION |FAILED_PRECONDITION can now be returned when the supplied offset was out of range.                       |OFFSET_OUT_OF_RANGE              |
+------------------+--------------------+---------------------------------------------------------------------------------------------------------+---------------------------------+
|INVALID_ARGUMENT  |INVALID_ARGUMENT    |A field is missing in the request.                                                                       |MISSING_FIELD                    |
+------------------+--------------------+---------------------------------------------------------------------------------------------------------+---------------------------------+
|INVALID_ARGUMENT  |INVALID_ARGUMENT    |Generic error for invalid arguments in the request.                                                      |INVALID_ARGUMENT                 |
+------------------+--------------------+---------------------------------------------------------------------------------------------------------+---------------------------------+
|INVALID_ARGUMENT  |INVALID_ARGUMENT    |Invalid field detected in the request.                                                                   |INVALID_FIELD                    |
+------------------+--------------------+---------------------------------------------------------------------------------------------------------+---------------------------------+
|NOT_FOUND         |FAILED_PRECONDITION |FAILED_PRECONDITION can now be returned when attempting to access the data that has already been pruned. |PARTICIPANT_PRUNED_DATA_ACCESSED |
+------------------+--------------------+---------------------------------------------------------------------------------------------------------+---------------------------------+


TransactionService.getTransactions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

+------------------+--------------------+---------------------------------------------------------------------------------------------------------+---------------------------------+
|gRPC status code  |gRPC status code    |Remarks                                                                                                  |Ledger API                       |
|(before SDK 1.18) |(since SDK 1.18)    |                                                                                                         |error code id                    |
+==================+====================+=========================================================================================================+=================================+
|INVALID_ARGUMENT  |FAILED_PRECONDITION |FAILED_PRECONDITION can now be returned when the supplied offset was out of range.                       |OFFSET_OUT_OF_RANGE              |
+------------------+--------------------+---------------------------------------------------------------------------------------------------------+---------------------------------+
|INVALID_ARGUMENT  |INVALID_ARGUMENT    |A field is missing in the request.                                                                       |MISSING_FIELD                    |
+------------------+--------------------+---------------------------------------------------------------------------------------------------------+---------------------------------+
|INVALID_ARGUMENT  |INVALID_ARGUMENT    |Generic error for invalid arguments in the request.                                                      |INVALID_ARGUMENT                 |
+------------------+--------------------+---------------------------------------------------------------------------------------------------------+---------------------------------+
|INVALID_ARGUMENT  |INVALID_ARGUMENT    |Invalid field detected in the request.                                                                   |INVALID_FIELD                    |
+------------------+--------------------+---------------------------------------------------------------------------------------------------------+---------------------------------+
|NOT_FOUND         |FAILED_PRECONDITION |FAILED_PRECONDITION can now be returned when attempting to access the data that has already been pruned. |PARTICIPANT_PRUNED_DATA_ACCESSED |
+------------------+--------------------+---------------------------------------------------------------------------------------------------------+---------------------------------+
|NOT_FOUND         |NOT_FOUND           |The ledger id from the request does match the participant's ledger id.                                   |LEDGER_ID_MISMATCH               |
+------------------+--------------------+---------------------------------------------------------------------------------------------------------+---------------------------------+

