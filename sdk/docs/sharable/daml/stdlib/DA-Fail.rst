.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-da-fail-58029:

DA.Fail
=======

Fail, for FailureStatus

Data Types
----------

.. _type-da-internal-fail-failurecategory-78872:

**data** `FailureCategory <type-da-internal-fail-failurecategory-78872_>`_

  The category of the failure, which determines the status code and log
  level of the failure\. Maps 1\-1 to the Canton error categories documented
  here\: https\://docs\.daml\.com/canton/reference/error\_codes\.html\#error\-categories\-inventory

  If you are more familiar with gRPC error codes, you can use the synonyms referenced in the
  comments\.

  .. _constr-da-internal-fail-invalidindependentofsystemstate-48495:

  `InvalidIndependentOfSystemState <constr-da-internal-fail-invalidindependentofsystemstate-48495_>`_

    Use this to report errors that are independent of the current state of the ledger,
    and should thus not be retried\.

    Corresponds to the gRPC status code ``INVALID_ARGUMENT``\.

    See https\://docs\.daml\.com/canton/reference/error\_codes\.html\#invalidindependentofsystemstate
    for more information\.

  .. _constr-da-internal-fail-invalidgivencurrentsystemstateother-32308:

  `InvalidGivenCurrentSystemStateOther <constr-da-internal-fail-invalidgivencurrentsystemstateother-32308_>`_

    Use this to report errors that are due to the current state of the ledger,
    but might disappear if the ledger state changes\. Clients should retry these
    requests after reading updated state from the ledger\.

    Corresponds to the gRPC status code ``FAILED_PRECONDITION``\.

    See https\://docs\.daml\.com/canton/reference/error\_codes\.html\#invalidgivencurrentsystemstateother
    for more information\.

  **instance** :ref:`Eq <class-ghc-classes-eq-22713>` `FailureCategory <type-da-internal-fail-failurecategory-78872_>`_

  **instance** :ref:`Ord <class-ghc-classes-ord-6395>` `FailureCategory <type-da-internal-fail-failurecategory-78872_>`_

  **instance** :ref:`Show <class-ghc-show-show-65360>` `FailureCategory <type-da-internal-fail-failurecategory-78872_>`_

.. _type-da-internal-fail-failurestatus-69800:

**data** `FailureStatus <type-da-internal-fail-failurestatus-69800_>`_

  .. _constr-da-internal-fail-failurestatus-54193:

  `FailureStatus <constr-da-internal-fail-failurestatus-54193_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - errorId
         - :ref:`Text <type-ghc-types-text-51952>`
         - Unambiguous identifier of the error\. SHOULD be prefixed with the DNS name identifying the app provider or the API standard defining the error\. For example, ``splice.lfdecentralizedtrust.org/insufficient-funds`` could be used for reporting an out of funds error in the context of the CN token standards\.
       * - category
         - `FailureCategory <type-da-internal-fail-failurecategory-78872_>`_
         - Category of the failure, which determines how clients are expected to handle the error\.
       * - message
         - :ref:`Text <type-ghc-types-text-51952>`
         - Developer\-facing error message, which should be in English\.
       * - meta
         - \[(:ref:`Text <type-ghc-types-text-51952>`, :ref:`Text <type-ghc-types-text-51952>`)\]
         - Machine\-readable metadata about the error in a key\-value format\. Use this to provide extra context to clients for errors\.  SHOULD be less than \< 512 characters as it MAY be truncated otherwise\.

  **instance** :ref:`Eq <class-ghc-classes-eq-22713>` `FailureStatus <type-da-internal-fail-failurestatus-69800_>`_

  **instance** :ref:`Ord <class-ghc-classes-ord-6395>` `FailureStatus <type-da-internal-fail-failurestatus-69800_>`_

  **instance** :ref:`Show <class-ghc-show-show-65360>` `FailureStatus <type-da-internal-fail-failurestatus-69800_>`_
