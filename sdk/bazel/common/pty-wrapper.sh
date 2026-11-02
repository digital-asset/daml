# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

exec python3 -c "import pty,sys; pty.spawn(sys.argv[1:])" "$@"
