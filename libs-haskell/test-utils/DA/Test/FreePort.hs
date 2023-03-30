module DA.Test.FreePort (withAvailablePort) where

import Control.Exception (bracket, throwIO)
import DA.Test.FreePort.Error
import DA.Test.FreePort.PortGen
import DA.Test.FreePort.PortLock
import Data.Either (isRight)
import Data.Foldable.Extra (firstJustM)
import UnliftIO.Exception (tryIO)
import Test.QuickCheck

import Network.Socket

maxAttempts :: Int
maxAttempts = 100

withAvailablePort :: (Int -> IO a) -> IO a
withAvailablePort k = do
  portGen <- getPortGen
  ports <- generate $ vectorOf maxAttempts portGen
  mRes <- firstJustM (tryPort k) ports
  maybe (throwIO NoPortsAvailableError) pure mRes

tryPort :: (Int -> IO a) -> Int -> IO (Maybe a)
tryPort k port = do
  available <- portAvailable port
  if available
    then withPortLock port (k port)
    else pure Nothing

portAvailable :: Int -> IO Bool
portAvailable port = do
  let hints = defaultHints { addrFlags = [AI_NUMERICHOST, AI_NUMERICSERV], addrSocketType = Stream }
      checkConnection addr =
        bracket
          (socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr))
          close
          (\s -> bind s (addrAddress addr))

  addr : _ <- getAddrInfo (Just hints) (Just "127.0.0.1") (Just $ show port)
  isRight <$> tryIO (checkConnection addr)
