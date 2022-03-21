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

Interface declaration
*********************

An interface declaration is somewhat similar to a template declaration.

Interface name
--------------

.. literalinclude:: ../code-snippets-dev/Interfaces.daml
   :language: daml
   :start-after: -- INTERFACE_NAME_BEGIN
   :end-before: -- INTERFACE_NAME_END

- This is the name of the interface.
- It's preceded by the keyword ``interface`` and followed by the keyword ``where``.
- It must begin with a capital letter, like any other type name.

Interface methods
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

Interface precondition
----------------------

.. literalinclude:: ../code-snippets-dev/Interfaces.daml
   :language: daml
   :start-after: -- INTERFACE_ENSURE_BEGIN
   :end-before: -- INTERFACE_ENSURE_END

- Introduced with the keyword ``ensure``, must be a boolean expression.
- It is possible to define interfaces without an ``ensure`` clause, but writing
  more than one is a compilation error.
- ``this`` is in scope in the method with the type of the interface.
  ``self``, however, is not.
- The interface methods can be used as part of the expression (e.g. ``method1``).
- It is evaluated and checked together with the implementing template's
  precondition upon contract creation

.. _interface-choices:

Interface choices
-----------------

.. literalinclude:: ../code-snippets-dev/Interfaces.daml
   :language: daml
   :start-after: -- INTERFACE_CHOICE_BEGIN
   :end-before: -- INTERFACE_CHOICE_END

- Works in a very similar way to template choices. Any contract of an
  implementing interface will grant the choice to the controlling party.
- Interface methods can be used to define the controller of a choice
  (e.g. ``method1``) as well as the actions that run when the choice is
  *exercised* (e.g. ``method2`` and ``method3``).
- As for template choices, the ``choice`` keyword can be prefixed with one of
  ``preconsuming``, ``postconsuming`` or ``nonconsuming`` to further specify
  the behaviour of the choice when exercised.
- See :doc:`choices` for full reference information, but note that
  controller-first syntax is not supported for interface choices.

Empty interfaces
----------------

.. literalinclude:: ../code-snippets-dev/Interfaces.daml
   :language: daml
   :start-after: -- EMPTY_INTERFACE_BEGIN
   :end-before: -- EMPTY_INTERFACE_END

- It is possible (though not necessarily useful) to define an interface without
  methods, precondition or choices. In such a case, the ``where`` keyword
  can be dropped.

Required interfaces
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

Interface implementation
************************

For context, a simple template definition:

.. literalinclude:: ../code-snippets-dev/Interfaces.daml
   :language: daml
   :start-after: -- TEMPLATE_HEADER_BEGIN
   :end-before: -- TEMPLATE_HEADER_END

Implements clause
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

Empty implements clause
-----------------------

.. literalinclude:: ../code-snippets-dev/Interfaces.daml
   :language: daml
   :start-after: -- TEMPLATE_EMPTY_IMPLEMENTS_BEGIN
   :end-before: -- TEMPLATE_EMPTY_IMPLEMENTS_END

- If the interface being implemented has no methods, the ``where`` keyword
  can be dropped.
