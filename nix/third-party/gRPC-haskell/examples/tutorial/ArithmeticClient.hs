{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedLists   #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

import           Arithmetic
import           Network.GRPC.HighLevel.Generated

clientConfig :: ClientConfig
clientConfig = ClientConfig { clientServerHost = "localhost"
                            , clientServerPort = 50051
                            , clientArgs = []
                            , clientSSLConfig = Nothing
                            }

main :: IO ()
main = withGRPCClient clientConfig $ \client -> do
  Arithmetic{..} <- arithmeticClient client

  -- Request for the Add RPC
  ClientNormalResponse (OneInt x) _meta1 _meta2 _status _details
    <- arithmeticAdd (ClientNormalRequest (TwoInts 2 2) 1 [])
  putStrLn ("2 + 2 = " ++ show x)

  -- Request for the RunningSum RPC
  ClientWriterResponse reply _streamMeta1 _streamMeta2 streamStatus streamDtls
    <- arithmeticRunningSum $ ClientWriterRequest 1 [] $ \send -> do
        eithers <- mapM send [OneInt 1, OneInt 2, OneInt 3]
                     :: IO [Either GRPCIOError ()]
        case sequence eithers of
          Left err -> error ("Error while streaming: " ++ show err)
          Right _  -> return ()

  case reply of
    Just (OneInt y) -> print ("1 + 2 + 3 = " ++ show y)
    Nothing -> putStrLn ("Client stream failed with status "
                         ++ show streamStatus
                         ++ " and details "
                         ++ show streamDtls)
  return ()
