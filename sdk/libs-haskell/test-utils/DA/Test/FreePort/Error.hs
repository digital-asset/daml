-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Test.FreePort.Error (FreePortError (..)) where

import Control.Exception (Exception)
import Type.Reflection (Typeable)

data FreePortError
  = DynamicRangeFileReadError IOError
  | DynamicRangeInvalidFormatError String
  | DynamicRangeShellFailure IOError
  | NoPortsAvailableError
  deriving (Show, Typeable)
instance Exception FreePortError

