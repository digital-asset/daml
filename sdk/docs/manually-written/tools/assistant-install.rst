.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _assistant-manual-managing-releases:

Managing SDK Versions
#####################

You can manage SDK versions manually by using ``daml install``.

To download and install the SDK of the latest stable Daml version::

  daml install latest

To download and install the latest snapshot release::

  daml install latest --snapshots=yes

Please note that snapshot releases are not intended for production usage.

To install the SDK version specified in the project config, run::

  daml install project

To install a specific SDK version, for example version ``2.0.0``, run::

  daml install 2.0.0

Rarely, you might need to install an SDK release from a downloaded SDK release tarball. **This is an advanced feature**: you should only ever perform this on an SDK release tarball that is released through the official ``digital-asset/daml`` github repository. Otherwise your ``daml`` installation may become inconsistent with everyone else's. To do this, run::

  daml install path-to-tarball.tar.gz

By default, ``daml install`` will update the assistant if the version being installed is newer. You can force the assistant to be updated with ``--install-assistant=yes`` and prevent the assistant from being updated with ``--install-assistant=no``.

See ``daml install --help`` for a full list of options.