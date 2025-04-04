.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Run Assistant Commands
######################

Terminal command completion
***************************

The ``daml`` assistant comes with support for ``bash`` and ``zsh`` completions. These are installed automatically on Linux and Mac when you install or upgrade the Daml assistant.

If you use the ``bash`` shell, and your ``bash`` supports completions, you can use the TAB key to complete many ``daml`` commands, such as ``daml install`` and ``daml version``.

For ``Zsh`` you first need to add ``~/.daml/zsh`` to your ``$fpath``,
e.g., by adding the following to the beginning of your ``~/.zshrc``
before you call ``compinit``: ``fpath=(~/.daml/zsh $fpath)``

You can override whether bash completions are installed for ``daml`` by
passing ``--bash-completions=yes`` or ``--bash-completions=no`` to ``daml install``.

.. _daml_project_dir:

Run commands outside of the project directory
*********************************************

In some cases, it can be convenient to run a command in a project
without changing directories. For that use case, you can set
the ``DAML_PROJECT`` environment variable to the path to the project:

.. code-block:: sh

    DAML_PROJECT=/path/to/my/project daml build

Note that while some commands, most notably, ``daml build``, accept a
``--project-root`` option, it can end up choosing the wrong SDK
version so you should prefer the environment variable instead.