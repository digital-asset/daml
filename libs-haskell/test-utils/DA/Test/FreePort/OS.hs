{-# LANGUAGE MultiWayIf #-}

module DA.Test.FreePort.OS where

import System.Info.Extra

data OS
  = Windows
  | Linux
  | MacOS

-- TODO: Linux being a catchall is risky, consider alternate check
os :: OS
os = if
  | isWindows -> Windows
  | isMac -> MacOS
  | otherwise -> Linux
