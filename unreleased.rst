.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Release notes
#############

This page contains release notes for the SDK.

HEAD — ongoing
--------------

- [daml assistant] Fix VSCode path for use if not already in PATH on mac
- [DAML-LF] Fixed regression that produced an invalid daml-lf-archive artefact. See `#2058 <https://github.com/digital-asset/daml/issues/2058>`__.
- [daml assistant] Kill child processes on ``SIGTERM``. This means that killing
  ``daml sandbox`` will also kill the sandbox process.
