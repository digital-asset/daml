.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-daml-script-internal-questions-packages-8598:

Daml.Script.Internal.Questions.Packages
=======================================

Data Types
----------

.. _type-daml-script-internal-questions-packages-listallpackages-28931:

**data** `ListAllPackages <type-daml-script-internal-questions-packages-listallpackages-28931_>`_

  .. _constr-daml-script-internal-questions-packages-listallpackages-95684:

  `ListAllPackages <constr-daml-script-internal-questions-packages-listallpackages-95684_>`_

    (no fields)

  **instance** :ref:`IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227>` `ListAllPackages <type-daml-script-internal-questions-packages-listallpackages-28931_>`_ \[`PackageName <type-daml-script-internal-questions-packages-packagename-68696_>`_\]

.. _type-daml-script-internal-questions-packages-listvettedpackages-5133:

**data** `ListVettedPackages <type-daml-script-internal-questions-packages-listvettedpackages-5133_>`_

  .. _constr-daml-script-internal-questions-packages-listvettedpackages-55524:

  `ListVettedPackages <constr-daml-script-internal-questions-packages-listvettedpackages-55524_>`_

    (no fields)

  **instance** :ref:`IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227>` `ListVettedPackages <type-daml-script-internal-questions-packages-listvettedpackages-5133_>`_ \[`PackageName <type-daml-script-internal-questions-packages-packagename-68696_>`_\]

.. _type-daml-script-internal-questions-packages-packagename-68696:

**data** `PackageName <type-daml-script-internal-questions-packages-packagename-68696_>`_

  .. _constr-daml-script-internal-questions-packages-packagename-3807:

  `PackageName <constr-daml-script-internal-questions-packages-packagename-3807_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - name
         - `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -
       * - version
         - `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  **instance** :ref:`IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227>` `ListAllPackages <type-daml-script-internal-questions-packages-listallpackages-28931_>`_ \[`PackageName <type-daml-script-internal-questions-packages-packagename-68696_>`_\]

  **instance** :ref:`IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227>` `ListVettedPackages <type-daml-script-internal-questions-packages-listvettedpackages-5133_>`_ \[`PackageName <type-daml-script-internal-questions-packages-packagename-68696_>`_\]

  **instance** `Eq <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713>`_ `PackageName <type-daml-script-internal-questions-packages-packagename-68696_>`_

  **instance** `Ord <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-ord-6395>`_ `PackageName <type-daml-script-internal-questions-packages-packagename-68696_>`_

  **instance** `Show <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-show-show-65360>`_ `PackageName <type-daml-script-internal-questions-packages-packagename-68696_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"name\" `PackageName <type-daml-script-internal-questions-packages-packagename-68696_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"packages\" `UnvetPackages <type-daml-script-internal-questions-packages-unvetpackages-98510_>`_ \[`PackageName <type-daml-script-internal-questions-packages-packagename-68696_>`_\]

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"packages\" `VetPackages <type-daml-script-internal-questions-packages-vetpackages-30455_>`_ \[`PackageName <type-daml-script-internal-questions-packages-packagename-68696_>`_\]

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"version\" `PackageName <type-daml-script-internal-questions-packages-packagename-68696_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"name\" `PackageName <type-daml-script-internal-questions-packages-packagename-68696_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"packages\" `UnvetPackages <type-daml-script-internal-questions-packages-unvetpackages-98510_>`_ \[`PackageName <type-daml-script-internal-questions-packages-packagename-68696_>`_\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"packages\" `VetPackages <type-daml-script-internal-questions-packages-vetpackages-30455_>`_ \[`PackageName <type-daml-script-internal-questions-packages-packagename-68696_>`_\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"version\" `PackageName <type-daml-script-internal-questions-packages-packagename-68696_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

.. _type-daml-script-internal-questions-packages-unvetpackages-98510:

**data** `UnvetPackages <type-daml-script-internal-questions-packages-unvetpackages-98510_>`_

  .. _constr-daml-script-internal-questions-packages-unvetpackages-33113:

  `UnvetPackages <constr-daml-script-internal-questions-packages-unvetpackages-33113_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - packages
         - \[`PackageName <type-daml-script-internal-questions-packages-packagename-68696_>`_\]
         -
       * - participant
         - `Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  **instance** :ref:`IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227>` `UnvetPackages <type-daml-script-internal-questions-packages-unvetpackages-98510_>`_ ()

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"packages\" `UnvetPackages <type-daml-script-internal-questions-packages-unvetpackages-98510_>`_ \[`PackageName <type-daml-script-internal-questions-packages-packagename-68696_>`_\]

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"participant\" `UnvetPackages <type-daml-script-internal-questions-packages-unvetpackages-98510_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_)

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"packages\" `UnvetPackages <type-daml-script-internal-questions-packages-unvetpackages-98510_>`_ \[`PackageName <type-daml-script-internal-questions-packages-packagename-68696_>`_\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"participant\" `UnvetPackages <type-daml-script-internal-questions-packages-unvetpackages-98510_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_)

.. _type-daml-script-internal-questions-packages-vetpackages-30455:

**data** `VetPackages <type-daml-script-internal-questions-packages-vetpackages-30455_>`_

  .. _constr-daml-script-internal-questions-packages-vetpackages-80768:

  `VetPackages <constr-daml-script-internal-questions-packages-vetpackages-80768_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - packages
         - \[`PackageName <type-daml-script-internal-questions-packages-packagename-68696_>`_\]
         -
       * - participant
         - `Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  **instance** :ref:`IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227>` `VetPackages <type-daml-script-internal-questions-packages-vetpackages-30455_>`_ ()

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"packages\" `VetPackages <type-daml-script-internal-questions-packages-vetpackages-30455_>`_ \[`PackageName <type-daml-script-internal-questions-packages-packagename-68696_>`_\]

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"participant\" `VetPackages <type-daml-script-internal-questions-packages-vetpackages-30455_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_)

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"packages\" `VetPackages <type-daml-script-internal-questions-packages-vetpackages-30455_>`_ \[`PackageName <type-daml-script-internal-questions-packages-packagename-68696_>`_\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"participant\" `VetPackages <type-daml-script-internal-questions-packages-vetpackages-30455_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_)

Functions
---------

.. _function-daml-script-internal-questions-packages-vetpackages-16211:

`vetPackages <function-daml-script-internal-questions-packages-vetpackages-16211_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> \[`PackageName <type-daml-script-internal-questions-packages-packagename-68696_>`_\] \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` ()

.. _function-daml-script-internal-questions-packages-vetpackagesonparticipant-8324:

`vetPackagesOnParticipant <function-daml-script-internal-questions-packages-vetpackagesonparticipant-8324_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> \[`PackageName <type-daml-script-internal-questions-packages-packagename-68696_>`_\] \-\> :ref:`ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190>` \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` ()

.. _function-daml-script-internal-questions-packages-unvetpackages-80050:

`unvetPackages <function-daml-script-internal-questions-packages-unvetpackages-80050_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> \[`PackageName <type-daml-script-internal-questions-packages-packagename-68696_>`_\] \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` ()

.. _function-daml-script-internal-questions-packages-unvetpackagesonparticipant-47459:

`unvetPackagesOnParticipant <function-daml-script-internal-questions-packages-unvetpackagesonparticipant-47459_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> \[`PackageName <type-daml-script-internal-questions-packages-packagename-68696_>`_\] \-\> :ref:`ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190>` \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` ()

.. _function-daml-script-internal-questions-packages-unsafevetpackages-17811:

`unsafeVetPackages <function-daml-script-internal-questions-packages-unsafevetpackages-17811_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> \[`PackageName <type-daml-script-internal-questions-packages-packagename-68696_>`_\] \-\> `Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ :ref:`ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190>` \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` ()

.. _function-daml-script-internal-questions-packages-unsafeunvetpackages-73826:

`unsafeUnvetPackages <function-daml-script-internal-questions-packages-unsafeunvetpackages-73826_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> \[`PackageName <type-daml-script-internal-questions-packages-packagename-68696_>`_\] \-\> `Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ :ref:`ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190>` \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` ()

.. _function-daml-script-internal-questions-packages-listvettedpackages-3001:

`listVettedPackages <function-daml-script-internal-questions-packages-listvettedpackages-3001_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` \[`PackageName <type-daml-script-internal-questions-packages-packagename-68696_>`_\]

.. _function-daml-script-internal-questions-packages-listallpackages-50063:

`listAllPackages <function-daml-script-internal-questions-packages-listallpackages-50063_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` \[`PackageName <type-daml-script-internal-questions-packages-packagename-68696_>`_\]

.. _function-daml-script-internal-questions-packages-thenwait-34974:

`thenWait <function-daml-script-internal-questions-packages-thenwait-34974_>`_
  \: :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` a \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` a

