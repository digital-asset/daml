.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-daml-script-internal-questions-usermanagement-67373:

Daml.Script.Internal.Questions.UserManagement
=============================================

Data Types
----------

.. _type-daml-script-internal-questions-usermanagement-createuser-632:

**data** `CreateUser <type-daml-script-internal-questions-usermanagement-createuser-632_>`_

  .. _constr-daml-script-internal-questions-usermanagement-createuser-36813:

  `CreateUser <constr-daml-script-internal-questions-usermanagement-createuser-36813_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - user
         - `User <type-daml-script-internal-questions-usermanagement-user-21930_>`_
         -
       * - rights
         - \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\]
         -
       * - participant
         - `Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  **instance** :ref:`IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227>` `CreateUser <type-daml-script-internal-questions-usermanagement-createuser-632_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ ())

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"participant\" `CreateUser <type-daml-script-internal-questions-usermanagement-createuser-632_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_)

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"rights\" `CreateUser <type-daml-script-internal-questions-usermanagement-createuser-632_>`_ \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\]

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"user\" `CreateUser <type-daml-script-internal-questions-usermanagement-createuser-632_>`_ `User <type-daml-script-internal-questions-usermanagement-user-21930_>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"participant\" `CreateUser <type-daml-script-internal-questions-usermanagement-createuser-632_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_)

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"rights\" `CreateUser <type-daml-script-internal-questions-usermanagement-createuser-632_>`_ \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"user\" `CreateUser <type-daml-script-internal-questions-usermanagement-createuser-632_>`_ `User <type-daml-script-internal-questions-usermanagement-user-21930_>`_

.. _type-daml-script-internal-questions-usermanagement-deleteuser-32589:

**data** `DeleteUser <type-daml-script-internal-questions-usermanagement-deleteuser-32589_>`_

  .. _constr-daml-script-internal-questions-usermanagement-deleteuser-79196:

  `DeleteUser <constr-daml-script-internal-questions-usermanagement-deleteuser-79196_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - userId
         - `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_
         -
       * - participant
         - `Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  **instance** :ref:`IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227>` `DeleteUser <type-daml-script-internal-questions-usermanagement-deleteuser-32589_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ ())

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"participant\" `DeleteUser <type-daml-script-internal-questions-usermanagement-deleteuser-32589_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_)

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"userId\" `DeleteUser <type-daml-script-internal-questions-usermanagement-deleteuser-32589_>`_ `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"participant\" `DeleteUser <type-daml-script-internal-questions-usermanagement-deleteuser-32589_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_)

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"userId\" `DeleteUser <type-daml-script-internal-questions-usermanagement-deleteuser-32589_>`_ `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_

.. _type-daml-script-internal-questions-usermanagement-getuser-72497:

**data** `GetUser <type-daml-script-internal-questions-usermanagement-getuser-72497_>`_

  .. _constr-daml-script-internal-questions-usermanagement-getuser-47542:

  `GetUser <constr-daml-script-internal-questions-usermanagement-getuser-47542_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - userId
         - `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_
         -
       * - participant
         - `Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  **instance** :ref:`IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227>` `GetUser <type-daml-script-internal-questions-usermanagement-getuser-72497_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `User <type-daml-script-internal-questions-usermanagement-user-21930_>`_)

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"participant\" `GetUser <type-daml-script-internal-questions-usermanagement-getuser-72497_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_)

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"userId\" `GetUser <type-daml-script-internal-questions-usermanagement-getuser-72497_>`_ `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"participant\" `GetUser <type-daml-script-internal-questions-usermanagement-getuser-72497_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_)

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"userId\" `GetUser <type-daml-script-internal-questions-usermanagement-getuser-72497_>`_ `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_

.. _type-daml-script-internal-questions-usermanagement-grantuserrights-74210:

**data** `GrantUserRights <type-daml-script-internal-questions-usermanagement-grantuserrights-74210_>`_

  .. _constr-daml-script-internal-questions-usermanagement-grantuserrights-94797:

  `GrantUserRights <constr-daml-script-internal-questions-usermanagement-grantuserrights-94797_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - userId
         - `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_
         -
       * - rights
         - \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\]
         -
       * - participant
         - `Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  **instance** :ref:`IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227>` `GrantUserRights <type-daml-script-internal-questions-usermanagement-grantuserrights-74210_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\])

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"participant\" `GrantUserRights <type-daml-script-internal-questions-usermanagement-grantuserrights-74210_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_)

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"rights\" `GrantUserRights <type-daml-script-internal-questions-usermanagement-grantuserrights-74210_>`_ \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\]

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"userId\" `GrantUserRights <type-daml-script-internal-questions-usermanagement-grantuserrights-74210_>`_ `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"participant\" `GrantUserRights <type-daml-script-internal-questions-usermanagement-grantuserrights-74210_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_)

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"rights\" `GrantUserRights <type-daml-script-internal-questions-usermanagement-grantuserrights-74210_>`_ \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"userId\" `GrantUserRights <type-daml-script-internal-questions-usermanagement-grantuserrights-74210_>`_ `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_

.. _type-daml-script-internal-questions-usermanagement-invaliduserid-35585:

**data** `InvalidUserId <type-daml-script-internal-questions-usermanagement-invaliduserid-35585_>`_

  Thrown if text for a user identifier does not conform to the format restriction\.

  .. _constr-daml-script-internal-questions-usermanagement-invaliduserid-47622:

  `InvalidUserId <constr-daml-script-internal-questions-usermanagement-invaliduserid-47622_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - m
         - `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  **instance** `Eq <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713>`_ `InvalidUserId <type-daml-script-internal-questions-usermanagement-invaliduserid-35585_>`_

  **instance** `Show <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-show-show-65360>`_ `InvalidUserId <type-daml-script-internal-questions-usermanagement-invaliduserid-35585_>`_

  **instance** `HasFromAnyException <https://docs.daml.com/daml/stdlib/DA-Exception.html#class-da-internal-exception-hasfromanyexception-16788>`_ `InvalidUserId <type-daml-script-internal-questions-usermanagement-invaliduserid-35585_>`_

  **instance** `HasMessage <https://docs.daml.com/daml/stdlib/DA-Exception.html#class-da-internal-exception-hasmessage-3179>`_ `InvalidUserId <type-daml-script-internal-questions-usermanagement-invaliduserid-35585_>`_

  **instance** `HasThrow <https://docs.daml.com/daml/stdlib/DA-Exception.html#class-da-internal-exception-hasthrow-30284>`_ `InvalidUserId <type-daml-script-internal-questions-usermanagement-invaliduserid-35585_>`_

  **instance** `HasToAnyException <https://docs.daml.com/daml/stdlib/DA-Exception.html#class-da-internal-exception-hastoanyexception-55973>`_ `InvalidUserId <type-daml-script-internal-questions-usermanagement-invaliduserid-35585_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"m\" `InvalidUserId <type-daml-script-internal-questions-usermanagement-invaliduserid-35585_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"message\" `InvalidUserId <type-daml-script-internal-questions-usermanagement-invaliduserid-35585_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"m\" `InvalidUserId <type-daml-script-internal-questions-usermanagement-invaliduserid-35585_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

.. _type-daml-script-internal-questions-usermanagement-listallusers-79412:

**data** `ListAllUsers <type-daml-script-internal-questions-usermanagement-listallusers-79412_>`_

  .. _constr-daml-script-internal-questions-usermanagement-listallusers-3769:

  `ListAllUsers <constr-daml-script-internal-questions-usermanagement-listallusers-3769_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - participant
         - `Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  **instance** :ref:`IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227>` `ListAllUsers <type-daml-script-internal-questions-usermanagement-listallusers-79412_>`_ \[`User <type-daml-script-internal-questions-usermanagement-user-21930_>`_\]

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"participant\" `ListAllUsers <type-daml-script-internal-questions-usermanagement-listallusers-79412_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_)

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"participant\" `ListAllUsers <type-daml-script-internal-questions-usermanagement-listallusers-79412_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_)

.. _type-daml-script-internal-questions-usermanagement-listuserrights-88601:

**data** `ListUserRights <type-daml-script-internal-questions-usermanagement-listuserrights-88601_>`_

  .. _constr-daml-script-internal-questions-usermanagement-listuserrights-6724:

  `ListUserRights <constr-daml-script-internal-questions-usermanagement-listuserrights-6724_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - userId
         - `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_
         -
       * - participant
         - `Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  **instance** :ref:`IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227>` `ListUserRights <type-daml-script-internal-questions-usermanagement-listuserrights-88601_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\])

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"participant\" `ListUserRights <type-daml-script-internal-questions-usermanagement-listuserrights-88601_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_)

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"userId\" `ListUserRights <type-daml-script-internal-questions-usermanagement-listuserrights-88601_>`_ `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"participant\" `ListUserRights <type-daml-script-internal-questions-usermanagement-listuserrights-88601_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_)

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"userId\" `ListUserRights <type-daml-script-internal-questions-usermanagement-listuserrights-88601_>`_ `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_

.. _type-daml-script-internal-questions-usermanagement-revokeuserrights-41537:

**data** `RevokeUserRights <type-daml-script-internal-questions-usermanagement-revokeuserrights-41537_>`_

  .. _constr-daml-script-internal-questions-usermanagement-revokeuserrights-26624:

  `RevokeUserRights <constr-daml-script-internal-questions-usermanagement-revokeuserrights-26624_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - userId
         - `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_
         -
       * - rights
         - \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\]
         -
       * - participant
         - `Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  **instance** :ref:`IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227>` `RevokeUserRights <type-daml-script-internal-questions-usermanagement-revokeuserrights-41537_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\])

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"participant\" `RevokeUserRights <type-daml-script-internal-questions-usermanagement-revokeuserrights-41537_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_)

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"rights\" `RevokeUserRights <type-daml-script-internal-questions-usermanagement-revokeuserrights-41537_>`_ \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\]

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"userId\" `RevokeUserRights <type-daml-script-internal-questions-usermanagement-revokeuserrights-41537_>`_ `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"participant\" `RevokeUserRights <type-daml-script-internal-questions-usermanagement-revokeuserrights-41537_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_)

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"rights\" `RevokeUserRights <type-daml-script-internal-questions-usermanagement-revokeuserrights-41537_>`_ \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"userId\" `RevokeUserRights <type-daml-script-internal-questions-usermanagement-revokeuserrights-41537_>`_ `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_

.. _type-daml-script-internal-questions-usermanagement-user-21930:

**data** `User <type-daml-script-internal-questions-usermanagement-user-21930_>`_

  User\-info record for a user in the user management service\.

  .. _constr-daml-script-internal-questions-usermanagement-user-51383:

  `User <constr-daml-script-internal-questions-usermanagement-user-51383_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - userId
         - `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_
         -
       * - primaryParty
         - `Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_
         -

  **instance** :ref:`IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227>` `GetUser <type-daml-script-internal-questions-usermanagement-getuser-72497_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `User <type-daml-script-internal-questions-usermanagement-user-21930_>`_)

  **instance** :ref:`IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227>` `ListAllUsers <type-daml-script-internal-questions-usermanagement-listallusers-79412_>`_ \[`User <type-daml-script-internal-questions-usermanagement-user-21930_>`_\]

  **instance** `Eq <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713>`_ `User <type-daml-script-internal-questions-usermanagement-user-21930_>`_

  **instance** `Ord <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-ord-6395>`_ `User <type-daml-script-internal-questions-usermanagement-user-21930_>`_

  **instance** `Show <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-show-show-65360>`_ `User <type-daml-script-internal-questions-usermanagement-user-21930_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"primaryParty\" `User <type-daml-script-internal-questions-usermanagement-user-21930_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_)

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"user\" `CreateUser <type-daml-script-internal-questions-usermanagement-createuser-632_>`_ `User <type-daml-script-internal-questions-usermanagement-user-21930_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"userId\" `User <type-daml-script-internal-questions-usermanagement-user-21930_>`_ `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"primaryParty\" `User <type-daml-script-internal-questions-usermanagement-user-21930_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_)

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"user\" `CreateUser <type-daml-script-internal-questions-usermanagement-createuser-632_>`_ `User <type-daml-script-internal-questions-usermanagement-user-21930_>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"userId\" `User <type-daml-script-internal-questions-usermanagement-user-21930_>`_ `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_

.. _type-daml-script-internal-questions-usermanagement-useralreadyexists-98333:

**data** `UserAlreadyExists <type-daml-script-internal-questions-usermanagement-useralreadyexists-98333_>`_

  Thrown if a user to be created already exists\.

  .. _constr-daml-script-internal-questions-usermanagement-useralreadyexists-40670:

  `UserAlreadyExists <constr-daml-script-internal-questions-usermanagement-useralreadyexists-40670_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - userId
         - `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_
         -

  **instance** `Eq <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713>`_ `UserAlreadyExists <type-daml-script-internal-questions-usermanagement-useralreadyexists-98333_>`_

  **instance** `Show <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-show-show-65360>`_ `UserAlreadyExists <type-daml-script-internal-questions-usermanagement-useralreadyexists-98333_>`_

  **instance** `HasFromAnyException <https://docs.daml.com/daml/stdlib/DA-Exception.html#class-da-internal-exception-hasfromanyexception-16788>`_ `UserAlreadyExists <type-daml-script-internal-questions-usermanagement-useralreadyexists-98333_>`_

  **instance** `HasMessage <https://docs.daml.com/daml/stdlib/DA-Exception.html#class-da-internal-exception-hasmessage-3179>`_ `UserAlreadyExists <type-daml-script-internal-questions-usermanagement-useralreadyexists-98333_>`_

  **instance** `HasThrow <https://docs.daml.com/daml/stdlib/DA-Exception.html#class-da-internal-exception-hasthrow-30284>`_ `UserAlreadyExists <type-daml-script-internal-questions-usermanagement-useralreadyexists-98333_>`_

  **instance** `HasToAnyException <https://docs.daml.com/daml/stdlib/DA-Exception.html#class-da-internal-exception-hastoanyexception-55973>`_ `UserAlreadyExists <type-daml-script-internal-questions-usermanagement-useralreadyexists-98333_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"message\" `UserAlreadyExists <type-daml-script-internal-questions-usermanagement-useralreadyexists-98333_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"userId\" `UserAlreadyExists <type-daml-script-internal-questions-usermanagement-useralreadyexists-98333_>`_ `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"userId\" `UserAlreadyExists <type-daml-script-internal-questions-usermanagement-useralreadyexists-98333_>`_ `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_

.. _type-daml-script-internal-questions-usermanagement-userid-11123:

**data** `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_

  Identifier for a user in the user management service\.

  .. _constr-daml-script-internal-questions-usermanagement-userid-52094:

  `UserId <constr-daml-script-internal-questions-usermanagement-userid-52094_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_


  **instance** `Eq <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713>`_ `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_

  **instance** `Ord <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-ord-6395>`_ `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_

  **instance** `Show <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-show-show-65360>`_ `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"userId\" `DeleteUser <type-daml-script-internal-questions-usermanagement-deleteuser-32589_>`_ `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"userId\" `GetUser <type-daml-script-internal-questions-usermanagement-getuser-72497_>`_ `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"userId\" `GrantUserRights <type-daml-script-internal-questions-usermanagement-grantuserrights-74210_>`_ `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"userId\" `ListUserRights <type-daml-script-internal-questions-usermanagement-listuserrights-88601_>`_ `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"userId\" `RevokeUserRights <type-daml-script-internal-questions-usermanagement-revokeuserrights-41537_>`_ `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"userId\" `User <type-daml-script-internal-questions-usermanagement-user-21930_>`_ `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"userId\" `UserAlreadyExists <type-daml-script-internal-questions-usermanagement-useralreadyexists-98333_>`_ `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"userId\" `UserNotFound <type-daml-script-internal-questions-usermanagement-usernotfound-44479_>`_ `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"userId\" `DeleteUser <type-daml-script-internal-questions-usermanagement-deleteuser-32589_>`_ `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"userId\" `GetUser <type-daml-script-internal-questions-usermanagement-getuser-72497_>`_ `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"userId\" `GrantUserRights <type-daml-script-internal-questions-usermanagement-grantuserrights-74210_>`_ `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"userId\" `ListUserRights <type-daml-script-internal-questions-usermanagement-listuserrights-88601_>`_ `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"userId\" `RevokeUserRights <type-daml-script-internal-questions-usermanagement-revokeuserrights-41537_>`_ `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"userId\" `User <type-daml-script-internal-questions-usermanagement-user-21930_>`_ `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"userId\" `UserAlreadyExists <type-daml-script-internal-questions-usermanagement-useralreadyexists-98333_>`_ `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"userId\" `UserNotFound <type-daml-script-internal-questions-usermanagement-usernotfound-44479_>`_ `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_

.. _type-daml-script-internal-questions-usermanagement-usernotfound-44479:

**data** `UserNotFound <type-daml-script-internal-questions-usermanagement-usernotfound-44479_>`_

  Thrown if a user cannot be located for a given user identifier\.

  .. _constr-daml-script-internal-questions-usermanagement-usernotfound-26338:

  `UserNotFound <constr-daml-script-internal-questions-usermanagement-usernotfound-26338_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - userId
         - `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_
         -

  **instance** `Eq <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713>`_ `UserNotFound <type-daml-script-internal-questions-usermanagement-usernotfound-44479_>`_

  **instance** `Show <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-show-show-65360>`_ `UserNotFound <type-daml-script-internal-questions-usermanagement-usernotfound-44479_>`_

  **instance** `HasFromAnyException <https://docs.daml.com/daml/stdlib/DA-Exception.html#class-da-internal-exception-hasfromanyexception-16788>`_ `UserNotFound <type-daml-script-internal-questions-usermanagement-usernotfound-44479_>`_

  **instance** `HasMessage <https://docs.daml.com/daml/stdlib/DA-Exception.html#class-da-internal-exception-hasmessage-3179>`_ `UserNotFound <type-daml-script-internal-questions-usermanagement-usernotfound-44479_>`_

  **instance** `HasThrow <https://docs.daml.com/daml/stdlib/DA-Exception.html#class-da-internal-exception-hasthrow-30284>`_ `UserNotFound <type-daml-script-internal-questions-usermanagement-usernotfound-44479_>`_

  **instance** `HasToAnyException <https://docs.daml.com/daml/stdlib/DA-Exception.html#class-da-internal-exception-hastoanyexception-55973>`_ `UserNotFound <type-daml-script-internal-questions-usermanagement-usernotfound-44479_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"message\" `UserNotFound <type-daml-script-internal-questions-usermanagement-usernotfound-44479_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"userId\" `UserNotFound <type-daml-script-internal-questions-usermanagement-usernotfound-44479_>`_ `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"userId\" `UserNotFound <type-daml-script-internal-questions-usermanagement-usernotfound-44479_>`_ `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_

.. _type-daml-script-internal-questions-usermanagement-userright-13475:

**data** `UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_

  The rights of a user\.

  .. _constr-daml-script-internal-questions-usermanagement-participantadmin-36398:

  `ParticipantAdmin <constr-daml-script-internal-questions-usermanagement-participantadmin-36398_>`_


  .. _constr-daml-script-internal-questions-usermanagement-canactas-78256:

  `CanActAs <constr-daml-script-internal-questions-usermanagement-canactas-78256_>`_ `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_


  .. _constr-daml-script-internal-questions-usermanagement-canreadas-21035:

  `CanReadAs <constr-daml-script-internal-questions-usermanagement-canreadas-21035_>`_ `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_


  .. _constr-daml-script-internal-questions-usermanagement-canreadasanyparty-13813:

  `CanReadAsAnyParty <constr-daml-script-internal-questions-usermanagement-canreadasanyparty-13813_>`_


  **instance** :ref:`IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227>` `GrantUserRights <type-daml-script-internal-questions-usermanagement-grantuserrights-74210_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\])

  **instance** :ref:`IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227>` `ListUserRights <type-daml-script-internal-questions-usermanagement-listuserrights-88601_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\])

  **instance** :ref:`IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227>` `RevokeUserRights <type-daml-script-internal-questions-usermanagement-revokeuserrights-41537_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\])

  **instance** `Eq <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713>`_ `UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_

  **instance** `Show <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-show-show-65360>`_ `UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"rights\" `CreateUser <type-daml-script-internal-questions-usermanagement-createuser-632_>`_ \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\]

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"rights\" `GrantUserRights <type-daml-script-internal-questions-usermanagement-grantuserrights-74210_>`_ \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\]

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"rights\" `RevokeUserRights <type-daml-script-internal-questions-usermanagement-revokeuserrights-41537_>`_ \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"rights\" `CreateUser <type-daml-script-internal-questions-usermanagement-createuser-632_>`_ \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"rights\" `GrantUserRights <type-daml-script-internal-questions-usermanagement-grantuserrights-74210_>`_ \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"rights\" `RevokeUserRights <type-daml-script-internal-questions-usermanagement-revokeuserrights-41537_>`_ \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\]

.. _type-daml-script-internal-questions-usermanagement-validateuserid-7081:

**data** `ValidateUserId <type-daml-script-internal-questions-usermanagement-validateuserid-7081_>`_

  .. _constr-daml-script-internal-questions-usermanagement-validateuserid-90456:

  `ValidateUserId <constr-daml-script-internal-questions-usermanagement-validateuserid-90456_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - name
         - `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  **instance** :ref:`IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227>` `ValidateUserId <type-daml-script-internal-questions-usermanagement-validateuserid-7081_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_)

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"name\" `ValidateUserId <type-daml-script-internal-questions-usermanagement-validateuserid-7081_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"name\" `ValidateUserId <type-daml-script-internal-questions-usermanagement-validateuserid-7081_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

Functions
---------

.. _function-daml-script-internal-questions-usermanagement-useridtotext-75939:

`userIdToText <function-daml-script-internal-questions-usermanagement-useridtotext-75939_>`_
  \: `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_ \-\> `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  Extract the name\-text from a user identitifer\.

.. _function-daml-script-internal-questions-usermanagement-mkuserid-92157:

`mkUserId <function-daml-script-internal-questions-usermanagement-mkuserid-92157_>`_
  \: `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_ \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_) \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_

.. _function-daml-script-internal-questions-usermanagement-checkuseralreadyexists-70414:

`checkUserAlreadyExists <function-daml-script-internal-questions-usermanagement-checkuseralreadyexists-70414_>`_
  \: `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_ \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ ()) \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` ()

.. _function-daml-script-internal-questions-usermanagement-checkusernotfound-23198:

`checkUserNotFound <function-daml-script-internal-questions-usermanagement-checkusernotfound-23198_>`_
  \: `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_ \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ a) \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` a

.. _function-daml-script-internal-questions-usermanagement-validateuserid-51917:

`validateUserId <function-daml-script-internal-questions-usermanagement-validateuserid-51917_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_ \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_

  Construct a user identifer from text\. May throw InvalidUserId\.

.. _function-daml-script-internal-questions-usermanagement-createuser-37948:

`createUser <function-daml-script-internal-questions-usermanagement-createuser-37948_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `User <type-daml-script-internal-questions-usermanagement-user-21930_>`_ \-\> \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\] \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` ()

  Create a user with the given rights\. May throw UserAlreadyExists\.

.. _function-daml-script-internal-questions-usermanagement-createuseron-3905:

`createUserOn <function-daml-script-internal-questions-usermanagement-createuseron-3905_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `User <type-daml-script-internal-questions-usermanagement-user-21930_>`_ \-\> \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\] \-\> :ref:`ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190>` \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` ()

  Create a user with the given rights on the given participant\. May throw UserAlreadyExists\.

.. _function-daml-script-internal-questions-usermanagement-createusertick-71056:

`createUser' <function-daml-script-internal-questions-usermanagement-createusertick-71056_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `User <type-daml-script-internal-questions-usermanagement-user-21930_>`_ \-\> \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\] \-\> `Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ :ref:`ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190>` \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` ()

.. _function-daml-script-internal-questions-usermanagement-getuser-5077:

`getUser <function-daml-script-internal-questions-usermanagement-getuser-5077_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_ \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` `User <type-daml-script-internal-questions-usermanagement-user-21930_>`_

  Fetch a user record by user id\. May throw UserNotFound\.

.. _function-daml-script-internal-questions-usermanagement-getuseron-1968:

`getUserOn <function-daml-script-internal-questions-usermanagement-getuseron-1968_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_ \-\> :ref:`ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190>` \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` `User <type-daml-script-internal-questions-usermanagement-user-21930_>`_

  Fetch a user record by user id from the given participant\. May throw UserNotFound\.

.. _function-daml-script-internal-questions-usermanagement-getusertick-49223:

`getUser' <function-daml-script-internal-questions-usermanagement-getusertick-49223_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_ \-\> `Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ :ref:`ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190>` \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` `User <type-daml-script-internal-questions-usermanagement-user-21930_>`_

.. _function-daml-script-internal-questions-usermanagement-listallusers-63416:

`listAllUsers <function-daml-script-internal-questions-usermanagement-listallusers-63416_>`_
  \: :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` \[`User <type-daml-script-internal-questions-usermanagement-user-21930_>`_\]

  List all users\. This function may make multiple calls to underlying paginated ledger API\.

.. _function-daml-script-internal-questions-usermanagement-listalluserson-20857:

`listAllUsersOn <function-daml-script-internal-questions-usermanagement-listalluserson-20857_>`_
  \: :ref:`ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190>` \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` \[`User <type-daml-script-internal-questions-usermanagement-user-21930_>`_\]

  List all users on the given participant\. This function may make multiple calls to underlying paginated ledger API\.

.. _function-daml-script-internal-questions-usermanagement-listalluserstick-94172:

`listAllUsers' <function-daml-script-internal-questions-usermanagement-listalluserstick-94172_>`_
  \: `Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ :ref:`ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190>` \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` \[`User <type-daml-script-internal-questions-usermanagement-user-21930_>`_\]

.. _function-daml-script-internal-questions-usermanagement-grantuserrights-87478:

`grantUserRights <function-daml-script-internal-questions-usermanagement-grantuserrights-87478_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_ \-\> \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\] \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\]

  Grant rights to a user\. Returns the rights that have been newly granted\. May throw UserNotFound\.

.. _function-daml-script-internal-questions-usermanagement-grantuserrightson-91259:

`grantUserRightsOn <function-daml-script-internal-questions-usermanagement-grantuserrightson-91259_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_ \-\> \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\] \-\> :ref:`ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190>` \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\]

  Grant rights to a user on the given participant\. Returns the rights that have been newly granted\. May throw UserNotFound\.

.. _function-daml-script-internal-questions-usermanagement-grantuserrightstick-94714:

`grantUserRights' <function-daml-script-internal-questions-usermanagement-grantuserrightstick-94714_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_ \-\> \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\] \-\> `Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ :ref:`ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190>` \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\]

.. _function-daml-script-internal-questions-usermanagement-revokeuserrights-85325:

`revokeUserRights <function-daml-script-internal-questions-usermanagement-revokeuserrights-85325_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_ \-\> \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\] \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\]

  Revoke rights for a user\. Returns the revoked rights\. May throw UserNotFound\.

.. _function-daml-script-internal-questions-usermanagement-revokeuserrightson-21608:

`revokeUserRightsOn <function-daml-script-internal-questions-usermanagement-revokeuserrightson-21608_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_ \-\> \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\] \-\> :ref:`ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190>` \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\]

  Revoke rights for a user on the given participant\. Returns the revoked rights\. May throw UserNotFound\.

.. _function-daml-script-internal-questions-usermanagement-revokeuserrightstick-82827:

`revokeUserRights' <function-daml-script-internal-questions-usermanagement-revokeuserrightstick-82827_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_ \-\> \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\] \-\> `Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ :ref:`ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190>` \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\]

.. _function-daml-script-internal-questions-usermanagement-deleteuser-2585:

`deleteUser <function-daml-script-internal-questions-usermanagement-deleteuser-2585_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_ \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` ()

  Delete a user\. May throw UserNotFound\.

.. _function-daml-script-internal-questions-usermanagement-deleteuseron-74248:

`deleteUserOn <function-daml-script-internal-questions-usermanagement-deleteuseron-74248_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_ \-\> :ref:`ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190>` \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` ()

  Delete a user on the given participant\. May throw UserNotFound\.

.. _function-daml-script-internal-questions-usermanagement-deleteusertick-87955:

`deleteUser' <function-daml-script-internal-questions-usermanagement-deleteusertick-87955_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_ \-\> `Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ :ref:`ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190>` \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` ()

.. _function-daml-script-internal-questions-usermanagement-listuserrights-50525:

`listUserRights <function-daml-script-internal-questions-usermanagement-listuserrights-50525_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_ \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\]

  List the rights of a user\. May throw UserNotFound\.

.. _function-daml-script-internal-questions-usermanagement-listuserrightson-11796:

`listUserRightsOn <function-daml-script-internal-questions-usermanagement-listuserrightson-11796_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_ \-\> :ref:`ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190>` \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\]

  List the rights of a user on the given participant\. May throw UserNotFound\.

.. _function-daml-script-internal-questions-usermanagement-listuserrightstick-48903:

`listUserRights' <function-daml-script-internal-questions-usermanagement-listuserrightstick-48903_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_ \-\> `Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ :ref:`ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190>` \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\]

.. _function-daml-script-internal-questions-usermanagement-submituser-29476:

`submitUser <function-daml-script-internal-questions-usermanagement-submituser-29476_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_ \-\> :ref:`Commands <type-daml-script-internal-questions-commands-commands-79301>` a \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` a

  Submit the commands with the actAs and readAs claims granted to a user\. May throw UserNotFound\.

.. _function-daml-script-internal-questions-usermanagement-submituseron-39337:

`submitUserOn <function-daml-script-internal-questions-usermanagement-submituseron-39337_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_ \-\> :ref:`ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190>` \-\> :ref:`Commands <type-daml-script-internal-questions-commands-commands-79301>` a \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` a

  Submit the commands with the actAs and readAs claims granted to the user on the given participant\. May throw UserNotFound\.

.. _function-daml-script-internal-questions-usermanagement-submitusertick-41544:

`submitUser' <function-daml-script-internal-questions-usermanagement-submitusertick-41544_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_ \-\> `Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ :ref:`ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190>` \-\> :ref:`Commands <type-daml-script-internal-questions-commands-commands-79301>` a \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` a

