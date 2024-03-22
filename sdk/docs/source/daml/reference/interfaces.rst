.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _interfaces:

Reference: Interfaces
#####################

.. warning::
  This feature is under active development and not officially supported in
  production environments.

In Daml, an interface defines an abstract type which specifies the behavior
that a template must implement. This allows decoupling such behavior from its
implementation, so other developers can write applications in terms of the
interface instead of the concrete template.

Interface Declaration
*********************

An interface declaration is somewhat similar to a template declaration.

Interface Name
--------------

.. literalinclude:: ../code-snippets-dev/Interfaces.daml
   :language: daml
   :start-after: -- INTERFACE_NAME_BEGIN
   :end-before: -- INTERFACE_NAME_END

- This is the name of the interface.
- It's preceded by the keyword ``interface`` and followed by the keyword ``where``.
- It must begin with a capital letter, like any other type name.

Interface Methods
-----------------

.. literalinclude:: ../code-snippets-dev/Interfaces.daml
   :language: daml
   :start-after: -- INTERFACE_METHODS_BEGIN
   :end-before: -- INTERFACE_METHODS_END

- An interface may define any number of methods.
- Methods are in scope as functions at the top level, in the ensure clause, and
  in interface choices. These functions always take an unstated first argument
  corresponding to a contract that implements the interface:

  .. literalinclude:: ../code-snippets-dev/Interfaces.daml
     :language: daml
     :start-after: -- INTERFACE_METHODS_TOP_LEVEL_BEGIN
     :end-before: -- INTERFACE_METHODS_TOP_LEVEL_END

- Methods are also in scope in interface choices
  (see :ref:`interface-choices` below).

Interface Precondition
----------------------

.. literalinclude:: ../code-snippets-dev/Interfaces.daml
   :language: daml
   :start-after: -- INTERFACE_ENSURE_BEGIN
   :end-before: -- INTERFACE_ENSURE_END

- A precondition is introduced with the keyword ``ensure`` and must be a
  boolean expression.
- It is possible to define interfaces without an ``ensure`` clause, but writing
  more than one is a compilation error.
- ``this`` is in scope in the method with the type of the interface.
  ``self``, however, is not.
- The interface methods can be used as part of the expression (e.g. ``method1``).
- It is evaluated and checked right after the implementing template's
  precondition upon contract creation

.. _interface-choices:

Interface Choices
-----------------

.. literalinclude:: ../code-snippets-dev/Interfaces.daml
   :language: daml
   :start-after: -- INTERFACE_CHOICES_BEGIN
   :end-before: -- INTERFACE_CHOICES_END

- Interface choices work in a very similar way to template choices. Any contract
  of an implementing interface will grant the choice to the controlling party.
- Interface methods can be used to define the controller of a choice
  (e.g. ``method1``) as well as the actions that run when the choice is
  *exercised* (e.g. ``method2`` and ``method3``).
- As for template choices, the ``choice`` keyword can be optionally prefixed
  with the ``nonconsuming`` keyword to specify that the contract will not be
  consumed when the choice is exercised. If not specified, the choice will be
  ``consuming``. Note that the ``preconsuming`` and ``postconsuming`` qualifiers
  are not supported on interface choices.
- See :doc:`choices` for full reference information, but note that
  controller-first syntax is not supported for interface choices.

Empty Interfaces
----------------

.. literalinclude:: ../code-snippets-dev/Interfaces.daml
   :language: daml
   :start-after: -- EMPTY_INTERFACE_BEGIN
   :end-before: -- EMPTY_INTERFACE_END

- It is possible (though not necessarily useful) to define an interface without
  methods, precondition or choices. In such a case, the ``where`` keyword
  can be dropped.

Required Interfaces
-------------------

.. literalinclude:: ../code-snippets-dev/Interfaces.daml
   :language: daml
   :start-after: -- INTERFACE_REQUIRES_BEGIN
   :end-before: -- INTERFACE_REQUIRES_END

- An interface can depend on other interfaces. These are specified with the
  ``requires`` keyword after the interface name but before the
  ``where`` keyword, separated by commas.
- For a template's implementation of an interface to be valid, all its required
  interfaces must also be implemented by the template.
- If the interface doesn't have any methods, precondition or choices,
  the ``where`` keyword after the last required interface can be dropped:

  .. literalinclude:: ../code-snippets-dev/Interfaces.daml
     :language: daml
     :start-after: -- EMPTY_INTERFACE_REQUIRES_BEGIN
     :end-before: -- EMPTY_INTERFACE_REQUIRES_END

Interface Implementation
************************

For context, a simple template definition:

.. literalinclude:: ../code-snippets-dev/Interfaces.daml
   :language: daml
   :start-after: -- TEMPLATE_HEADER_BEGIN
   :end-before: -- TEMPLATE_HEADER_END

Implements Clause
-----------------

.. literalinclude:: ../code-snippets-dev/Interfaces.daml
   :language: daml
   :start-after: -- TEMPLATE_IMPLEMENTS_BEGIN
   :end-before: -- TEMPLATE_IMPLEMENTS_END

- To make a template implement an interface, an ``implements`` clause is added to
  the body of the template.

- The clause must start with the keyword ``implements``, followed by the name of
  the interface, followed by the keyword ``where``, which introduces a block
  where **all** the methods of the interface must be implemented.
- Methods can be defined using the same syntax as for top level functions,
  including pattern matches and guards (e.g. ``method3``).

Empty Implements Clause
-----------------------

.. literalinclude:: ../code-snippets-dev/Interfaces.daml
   :language: daml
   :start-after: -- TEMPLATE_EMPTY_IMPLEMENTS_BEGIN
   :end-before: -- TEMPLATE_EMPTY_IMPLEMENTS_END

- If the interface being implemented has no methods, the ``where`` keyword
  can be dropped.

Interface Functions
*******************

.. list-table::
   :header-rows: 1

   * - Function
     - Type
     - Instantiated type
     - Notes
   * - ``interfaceTypeRep``
     - ``HasInterfaceTypeRep i => i -> TemplateTypeRep``
     - ``MyInterface -> TemplateTypeRep``
     - The value of the resulting ``TemplateTypeRep`` indicates what template
       was used to construct the interface value.
   * - ``toInterface``
     - ``forall i t. HasToInterface t i => t -> i``
     - ``MyTemplate -> MyInterface``
     - Converts a template value into an interface value. Can also be used to
       convert an interface value to one of its required interfaces.
   * - ``fromInterface``
     - ``HasFromInterface t i => i -> Optional t``
     - ``MyInterface -> Optional MyTemplate``
     - Attempts to convert an interface value back into a template value.
       The result is ``None`` if the expected template type doesn't match the
       underlying template type used to construct the contract. Can also be
       used to convert a value of an interface type to one of its
       requiring interfaces.
   * - ``toInterfaceContractId``
     - ``forall i t. HasToInterface t i => ContractId t -> ContractId i``
     - ``ContractId MyTemplate -> ContractId MyInterface``
     - Convert a template contract id into an interface contract id. Can also
       be used to convert an interface contract id into a contract id of one of
       its required interfaces.
   * - ``fromInterfaceContractId``
     - ``forall t i. HasFromInterface t i => ContractId i -> ContractId t``
     - ``ContractId MyInterface -> ContractId MyTemplate``
     - Converts an interface contract id into a template contract id.
       Can also be used to convert an interface contract id into a contract id
       of a one of its requiring interfaces.
       This function does not verify that the given contract id actually points
       to a contract of the resulting type; if that is not the case, a
       subsequent ``fetch``, ``exercise`` or ``archive`` will fail.
       Therefore, this should only be used when the underlying contract is known
       to be of the resulting type, or when the result is immediately used by a
       ``fetch``, ``exercise`` or ``archive`` action and a transaction failure
       is the desired behavior in case of mismatch.
       In all other cases, consider using ``fetchFromInterface`` instead.
   * - ``fetchFromInterface``
     - ``forall t i. (HasFromInterface t i, HasFetch i) => ContractId i -> Update (Optional (ContractId t, t))``
     - ``ContractId MyInterface -> Update (Optional (ContractId MyTemplate, MyTemplate))``
     - Attempts to fetch and convert an interface contract id into a template,
       returning both the converted contract and its contract id if the
       conversion is successful, or ``None`` otherwise.
       Can also be used to fetch and convert an interface contract id into a
       contract and contract id of one of its requiring interfaces.
