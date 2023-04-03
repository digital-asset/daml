-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Test.FreePort.PortGen (getPortGen) where

import Control.Exception (mapException, throwIO)
import DA.Test.FreePort.Error (FreePortError (..))
import DA.Test.FreePort.OS (os, OS (..))
import Safe (tailMay)
import System.Process (readProcess)
import Test.QuickCheck(Gen, chooseInt)
import Text.Read (readMaybe)
import Text.Regex.TDFA

import qualified Bazel.Runfiles

newtype PortRange = PortRange (Int, Int) deriving Show -- The main port range
newtype DynamicPortRange = DynamicPortRange (Int, Int) deriving Show -- Port range to exclude from main port range

defPortRange :: PortRange
defPortRange = PortRange (1024, 65536)

-- | Clamps an integer within a given min max range
clampInt :: Int -> Int -> Int -> Int
clampInt minVal maxVal = min maxVal . max minVal

portGen :: PortRange -> DynamicPortRange -> Gen Int
portGen (PortRange (minPort, maxPort)) (DynamicPortRange (minExclPort, maxExclPort)) = do
  -- Exclude the dynamic port range (cropped to within the valid port range).
  -- E.g. 32768 60999 on most Linux systems.

  let -- Clamp the dyn range into the main port range
      minExcl = clampInt minPort maxPort minExclPort
      maxExcl = clampInt minPort maxPort maxExclPort

      -- Get count of ports before and after the excluded port range (where excluded is inclusive)
      numLowerPorts = minExcl - minPort
      numUpperPorts = maxPort - maxExcl
      numAvailablePorts = numLowerPorts + numUpperPorts

  n <- chooseInt (0, numAvailablePorts - 1)
  pure $ if n < numLowerPorts
    then n + minPort
    else n - numLowerPorts + maxExcl + 1

getPortGen :: IO (Gen Int)
getPortGen = portGen defPortRange <$> getDynamicPortRange

getDynamicPortRange :: IO DynamicPortRange
getDynamicPortRange = case os of
  Windows -> getWindowsDynamicPortRange
  Linux -> getLinuxDynamicPortRange
  MacOS -> getMacOSDynamicPortRange

getLinuxDynamicPortRange :: IO DynamicPortRange
getLinuxDynamicPortRange = do
  rangeStr <- mapException DynamicRangeFileReadError $ readFile "/proc/sys/net/ipv4/ip_local_port_range"
  let ports = traverse readMaybe $ words rangeStr
  case ports of
    Just [min, max] -> pure $ DynamicPortRange (min, max)
    _ -> throwIO $ DynamicRangeInvalidFormatError $ "Expected 2 ports, got " <> rangeStr

getWindowsDynamicPortRange :: IO DynamicPortRange
getWindowsDynamicPortRange = do
  portData <- mapException DynamicRangeShellFailure $ readProcess "netsh" ["int", "ipv4", "show", "dynamicport", "tcp"] ""
  let ports :: [String] = getAllTextSubmatches (portData =~ ("Start Port *: ([0-9]+)\nNumber of Ports *: ([0-9]+)" :: String))
  case tailMay ports >>= traverse readMaybe of
    Just [min, count] -> pure $ DynamicPortRange (min, min + count)
    _ -> throwIO $ DynamicRangeInvalidFormatError "Malformed response from netsh"

getMacOSDynamicPortRange :: IO DynamicPortRange
getMacOSDynamicPortRange = do
  runFiles <- Bazel.Runfiles.create
  let sysctl = Bazel.Runfiles.rlocation runFiles "external/sysctl_nix/bin/sysctl"
  portData <- mapException DynamicRangeShellFailure $ readProcess sysctl
    [ "net.inet.ip.portrange.first"
    , "net.inet.ip.portrange.last"
    ]
    ""
  error $ show portData

