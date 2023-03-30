module DA.Test.FreePort.PortGen (getPortGen) where

import Control.Exception (mapException, throwIO)
import DA.Test.FreePort.Error (FreePortError (..))
import DA.Test.FreePort.OS (os, OS (..))
import Test.QuickCheck(Gen, chooseInt)
import Text.Read (readMaybe)

newtype PortRange = PortRange (Int, Int) -- The main port range
newtype DynamicPortRange = DynamicPortRange (Int, Int) -- Port range to exclude from main port range

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
getWindowsDynamicPortRange = undefined -- TODO

getMacOSDynamicPortRange :: IO DynamicPortRange
getMacOSDynamicPortRange = undefined -- TODO
