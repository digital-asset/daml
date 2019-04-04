{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE BangPatterns #-}

import Arithmetic
import Network.GRPC.HighLevel.Generated

import Data.String (fromString)

handlers :: Arithmetic ServerRequest ServerResponse
handlers = Arithmetic { arithmeticAdd = addHandler
                      , arithmeticRunningSum = runningSumHandler
                      }

addHandler :: ServerRequest 'Normal TwoInts OneInt
              -> IO (ServerResponse 'Normal OneInt)
addHandler (ServerNormalRequest _metadata (TwoInts x y)) = do
  let answer = OneInt (x + y)
  return (ServerNormalResponse answer
                               [("metadata_key_one", "metadata_value")]
                               StatusOk
                               "addition is easy!")


runningSumHandler :: ServerRequest 'ClientStreaming OneInt OneInt
                     -> IO (ServerResponse 'ClientStreaming OneInt)
runningSumHandler (ServerReaderRequest _metadata recv) =
  loop 0
    where loop !i =
            do msg <- recv
               case msg of
                 Left err -> return (ServerReaderResponse
                                      Nothing
                                      []
                                      StatusUnknown
                                      (fromString (show err)))
                 Right (Just (OneInt x)) -> loop (i + x)
                 Right Nothing -> return (ServerReaderResponse
                                           (Just (OneInt i))
                                           []
                                           StatusOk
                                           "")

options :: ServiceOptions
options = defaultServiceOptions

main :: IO ()
main = arithmeticServer handlers options
