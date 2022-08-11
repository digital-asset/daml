Glossary
########

This page describes some of the terminology used throughout the Daml Finance library.

We strive to use descriptive names and stay as close as possible to the traditional financial meaning of the terms.

.. _account:

Account
-------

An Account contract is a relationship between a custodian (or account provider) and an asset owner.

An Account is referenced by `holdings <#holding>`__ and is used to limit the parties who are entitled to receive holding transfers.

.. _instrument:

Instrument
----------

An Instrument describes the economic terms (rights and obligations) of one unit of a financial contract.

An Instrument is referenced by `holdings <#holding>`__. It can be as simple as an ISIN code referencing some real-world (off-ledger) security, or it could encode specific on-ledger lifecycling logic.

.. _holding:

Holding
-------

A Holding contract represents the ownership of a certain amount of an `Instrument <#instrument>`__ by an owner at a custodian.

Custodian
---------

The party registering ownership for a given `Instrument <#instrument>`__.

Owner
-----

The party holding legal (or beneficial) ownership of an `Instrument <#instrument>`__.

Depository
----------

The party responsible to safekeep the legal form of an `Instrument <#instrument>`__ (e.g., paper certificates).

Issuer
------

The party issuing new units of an `Instrument <#instrument>`__.

.. _fungibility:

Fungibility
-----------

The ability of an `Instrument <#instrument>`__ to be interchanged with other individual instruments of the same type.

Only a fungible Instrument can be held for an amount other than ``1.0``.

.. _transferability:

Transferability
---------------

The ability to transfer ownership of units of an `Instrument <#instrument>`__ to a new owner at the same custodian.

.. _locking:

Locking
-------

Mechanism that adds a third-party authorization requirement to any interaction with a `Holding <#holding>`__ (archive, transfer, split, merge, etc.).

It is used to ensure that Holdings committed to a certain workflow are not consumed by other workflows.

Crediting / Debiting
--------------------

The process of creating new `Holdings <#holding>`__ for a given instrument (respectively, removing existing ones).

Disclosure
----------

The ability to disclose a contract to a third party by adding them as an observer.

Settlement
----------

The (possibly simultaneous) execution of ownership transfers according to predefined instructions.

Many financial transactions are traditionally settled a few days after execution.

.. _lifecycling:

Lifecycling
-----------

The evolution of `Instruments <#instrument>`__ over their lifetime.

Lifecycling can deal with intrinsic events, like contractual cash flows, as well as extrinsic events like corporate actions or elections.
