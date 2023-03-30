module DA.Test.FreePort.Error where

import Control.Exception (Exception)
import Type.Reflection (Typeable)

data FreePortError
  = DynamicRangeFileReadError IOError
  | DynamicRangeInvalidFormatError String
  | NoPortsAvailableError
  deriving (Show, Typeable)
instance Exception FreePortError
