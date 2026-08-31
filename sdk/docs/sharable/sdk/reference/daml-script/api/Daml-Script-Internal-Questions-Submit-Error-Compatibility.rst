.. Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-daml-script-internal-questions-submit-error-compatibility-70159:

Daml.Script.Internal.Questions.Submit.Error.Compatibility
=========================================================

This module defines the compatibility Pattern Synonyms for the various SubmitError types
and the show instances that depend on them\.
Pattern synonyms are a way to provide a variant matching like syntax for types that are not variants\.
For example, the

.. code-block:: daml

    pattern UnsupportedContractId : Text -> AnySubmitError


Can be used to match on ``AnySubmitError`` as though it were a ``UnsupportedContractId``, even though
``AnySubmitError`` does not have a constructor named ``UnsupportedContractId``\.
This allows for backwards compatibility with code that was written against the old ``SubmitError`` type,
which did have a constructor named ``UnsupportedContractId``, but was not cross\-sdk compatible\.

Orphan Typeclass Instances
--------------------------

**instance** `Show <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-ghc-show-show-65360>`_ `UpgradeErrorType <type-daml-script-internal-questions-submit-error-compatibility-upgradeerrortype-58287_>`_

**instance** `Show <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-ghc-show-show-65360>`_ `SubmitError <type-daml-script-internal-questions-submit-error-compatibility-submiterror-33824_>`_

**instance** `Show <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-ghc-show-show-65360>`_ :ref:`AnyCryptoErrorType <type-daml-script-internal-questions-submit-error-stable-anycryptoerrortype-anycryptoerrortype-64150>`

**instance** `Show <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-ghc-show-show-65360>`_ :ref:`AnyExternalCallErrorType <type-daml-script-internal-questions-submit-error-stable-anyexternalcallerrortype-anyexternalcallerrortype-11122>`

**instance** `Show <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-ghc-show-show-65360>`_ :ref:`AnyDevErrorType <type-daml-script-internal-questions-submit-error-stable-anydeverrortype-anydeverrortype-93864>`

Data Types
----------

.. _type-daml-script-internal-questions-submit-error-compatibility-cryptoerrortype-2249:

**type** `CryptoErrorType <type-daml-script-internal-questions-submit-error-compatibility-cryptoerrortype-2249_>`_
  \= :ref:`AnyCryptoErrorType <type-daml-script-internal-questions-submit-error-stable-anycryptoerrortype-anycryptoerrortype-64150>`

.. _type-daml-script-internal-questions-submit-error-compatibility-deverrortype-27984:

**type** `DevErrorType <type-daml-script-internal-questions-submit-error-compatibility-deverrortype-27984_>`_
  \= :ref:`AnyDevErrorType <type-daml-script-internal-questions-submit-error-stable-anydeverrortype-anydeverrortype-93864>`

.. _type-daml-script-internal-questions-submit-error-compatibility-externalcallerrortype-11049:

**type** `ExternalCallErrorType <type-daml-script-internal-questions-submit-error-compatibility-externalcallerrortype-11049_>`_
  \= :ref:`AnyExternalCallErrorType <type-daml-script-internal-questions-submit-error-stable-anyexternalcallerrortype-anyexternalcallerrortype-11122>`

.. _type-daml-script-internal-questions-submit-error-compatibility-submiterror-33824:

**type** `SubmitError <type-daml-script-internal-questions-submit-error-compatibility-submiterror-33824_>`_
  \= :ref:`AnySubmitError <type-daml-script-internal-questions-submit-error-stable-anysubmiterror-anysubmiterror-96036>`

  **instance** `Show <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-ghc-show-show-65360>`_ `SubmitError <type-daml-script-internal-questions-submit-error-compatibility-submiterror-33824_>`_

.. _type-daml-script-internal-questions-submit-error-compatibility-upgradeerrortype-58287:

**type** `UpgradeErrorType <type-daml-script-internal-questions-submit-error-compatibility-upgradeerrortype-58287_>`_
  \= :ref:`AnyUpgradeErrorType <type-daml-script-internal-questions-submit-error-stable-anyupgradeerrortype-anyupgradeerrortype-9932>`

  **instance** `Show <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-ghc-show-show-65360>`_ `UpgradeErrorType <type-daml-script-internal-questions-submit-error-compatibility-upgradeerrortype-58287_>`_

