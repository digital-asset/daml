.. Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-da-externalcall-24746:

DA.ExternalCall
===============

Functions
---------

.. _function-da-externalcall-externalcall-62225:

`externalCall <function-da-externalcall-externalcall-62225_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Update <type-da-internal-lf-update-68072>` :ref:`Text <type-ghc-types-text-51952>`

  Make an external call to a configured extension service\.

  The first two arguments identify the extension and function configured on the
  participant\. The config and input arguments are hex\-encoded byte strings; the
  empty string represents zero bytes\. Payloads are canonicalized to lowercase
  before the LF builtin is invoked\.

  The update records the external call in the transaction\. The participant
  handles communication with the configured extension service\. Malformed
  payloads, missing extension configuration, extension\-service failures, and
  invalid service output fail the update\. Runtime and service errors include
  extension and function context\.

  The result is the service response as a hex\-encoded byte string\.
