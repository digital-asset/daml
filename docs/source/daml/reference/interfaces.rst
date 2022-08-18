.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _interfaces:

Reference: Interfaces
#####################

.. warning::
  This feature is under active development and not officially supported in
  production environments.

In Daml, an interface defines an abstract type together with a behavior
specified by its view type, method signatures, and choices. For a template to
conform to this interface, there must be a corresponding ``interface instance``
clause where all the methods of the interface (including the special ``view``
method) are implemented. This allows decoupling such behavior from its
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
- Methods are in scope as functions at the top level and in interface choices.
  These functions always take an unstated first argument corresponding to a
  contract of a template type that is an ``interface instance`` of the
  interface:

  .. literalinclude:: ../code-snippets-dev/Interfaces.daml
     :language: daml
     :start-after: -- INTERFACE_METHODS_TOP_LEVEL_BEGIN
     :end-before: -- INTERFACE_METHODS_TOP_LEVEL_END

- Methods are also in scope in interface choices
  (see :ref:`interface-choices` below).
- One special method, ``view``, must be defined for the viewtype.
  (see :ref:`interface-viewtype` below).

.. _interface-viewtype:

Interface View Type
-------------------

.. literalinclude:: ../code-snippets-dev/Interfaces.daml
   :language: daml
   :start-after: -- INTERFACE_VIEWTYPE_DATATYPE_BEGIN
   :end-before: -- INTERFACE_VIEWTYPE_DATATYPE_END

.. literalinclude:: ../code-snippets-dev/Interfaces.daml
   :language: daml
   :start-after: -- INTERFACE_VIEWTYPE_BEGIN
   :end-before: -- INTERFACE_VIEWTYPE_END

- All interface instances must implement a special ``view`` method which returns
  a value of the type declared by ``viewtype``.
- The type must be a record.
- This type is returned by subscriptions on interfaces.

.. _interface-choices:

Interface Choices
-----------------

.. literalinclude:: ../code-snippets-dev/Interfaces.daml
   :language: daml
   :start-after: -- INTERFACE_CHOICES_BEGIN
   :end-before: -- INTERFACE_CHOICES_END

- Interface choices work in a very similar way to template choices. Any contract
  of a template type for which an interface instance exists, will grant the
  choice to the controlling party.
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
  methods, precondition or choices. However, a view type must always be defined,
  though it can be set to unit.

Required Interfaces
-------------------

.. literalinclude:: ../code-snippets-dev/Interfaces.daml
   :language: daml
   :start-after: -- INTERFACE_REQUIRES_BEGIN
   :end-before: -- INTERFACE_REQUIRES_END

- An interface can depend on other interfaces. These are specified with the
  ``requires`` keyword after the interface name but before the
  ``where`` keyword, separated by commas.
- For an interface declaration to be valid, its list of required interfaces
  must be transitively closed. In other words, an interface ``I`` cannot
  require an interface ``J`` without also explicitly requiring all the
  interfaces required by ``J``. The order, however, is irrelevant.

  For example, given

  .. literalinclude:: ../code-snippets-dev/Interfaces.daml
     :language: daml
     :start-after: -- INTERFACE_TRANSITIVE_REQUIRES_GIVEN_BEGIN
     :end-before: -- INTERFACE_TRANSITIVE_REQUIRES_GIVEN_END

  This declaration for interface ``Square`` would cause a compiler error

  .. literalinclude:: ../code-snippets-dev/Interfaces.daml
     :language: daml
     :start-after: -- INTERFACE_TRANSITIVE_REQUIRES_INCORRECT_BEGIN
     :end-before: -- INTERFACE_TRANSITIVE_REQUIRES_INCORRECT_END

  Explicitly adding ``Shape`` to the required interfaces fixes the error

  .. literalinclude:: ../code-snippets-dev/Interfaces.daml
     :language: daml
     :start-after: -- INTERFACE_TRANSITIVE_REQUIRES_CORRECT_BEGIN
     :end-before: -- INTERFACE_TRANSITIVE_REQUIRES_CORRECT_END

- For a template ``T`` to be a valid ``interface instance`` of an interface
  ``I``, ``T`` must also be an ``interface instance`` of each of the interfaces
  required by ``I``.

Interface Instances
*******************

For context, a simple template definition:

.. literalinclude:: ../code-snippets-dev/Interfaces.daml
   :language: daml
   :start-after: -- TEMPLATE_HEADER_BEGIN
   :end-before: -- TEMPLATE_HEADER_END

``interface instance`` clause
-----------------------------

.. literalinclude:: ../code-snippets-dev/Interfaces.daml
   :language: daml
   :start-after: -- INTERFACE_INSTANCE_IN_TEMPLATE_BEGIN
   :end-before: -- INTERFACE_INSTANCE_IN_TEMPLATE_END

- To make a template an instance of an existing interface, an
  ``interface instance`` clause must be defined in the template declaration.
- The template of the clause must match the enclosing declaration. In other
  words, a template ``T`` declaration can only contain ``interface instance``
  clauses where the template is ``T``.
- The clause must start with the keywords ``interface instance``, followed by
  the name of the interface, then the keyword ``for`` and the name of the
  template, and finally the keyword ``where``, which introduces a block where
  **all** the methods of the interface must be implemented.
- Within the clause, there's an implicit local binding ``this`` referring to the
  current contract, which has the type of the template's data record. The
  template parameters of this contract are also in scope.
- The special ``view`` method must be implemented with the same return type as
  the interface's view type.
- Method implementations can be defined using the same syntax as for top level
  functions, including pattern matches and guards (e.g. ``method3``).

``interface instance`` clause in the interface
----------------------------------------------

.. literalinclude:: ../code-snippets-dev/Interfaces.daml
   :language: daml
   :start-after: -- INTERFACE_INSTANCE_IN_INTERFACE_BEGIN
   :end-before: -- INTERFACE_INSTANCE_IN_INTERFACE_END

- To make an *existing* template an instance of a new interface, the
  ``interface instance`` clause must be defined in the *interface* declaration.
- In this case, the *interface* of the clause must match the enclosing
  declaration. In other words, an interface ``I`` declaration can only contain
  ``interface instance`` clauses where the interface is ``I``.
- All other rules for ``interface instance`` clauses are the same whether the
  enclosing declaration is a template or an interface. In particular, the
  implicit local binding ``this`` always has the type of the *template*'s
  record.

Empty ``interface instance`` clause
-----------------------------------

.. literalinclude:: ../code-snippets-dev/Interfaces.daml
   :language: daml
   :start-after: -- EMPTY_INTERFACE_INSTANCE_BEGIN
   :end-before: -- EMPTY_INTERFACE_INSTANCE_END

- If the interface has no methods, the interface instance only needs to
  implement the ``view`` method.

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
