{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedLists   #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE TypeOperators     #-}

import           Control.Monad
import           Data.ByteString                  (ByteString)
import           Data.Maybe                       (fromMaybe)
import qualified Data.Text.Lazy                   as TL
import           Echo
import           GHC.Generics                     (Generic)
import           Network.GRPC.HighLevel.Client
import           Network.GRPC.HighLevel.Generated
import           Network.GRPC.LowLevel
import           Options.Generic
import           Prelude                          hiding (FilePath)

data Args = Args
  { bind       :: Maybe ByteString <?> "grpc endpoint hostname (default \"localhost\")"
  , port       :: Maybe Int        <?> "grpc endpoint port (default 50051)"
  , payload    :: Maybe TL.Text    <?> "string to echo (default \"hullo!\")"
  } deriving (Generic, Show)
instance ParseRecord Args

main :: IO ()
main = do
  Args{..} <- getRecord "Runs the echo client"
  let
    pay      = fromMaybe "hullo!" . unHelpful $ payload
    rqt      = EchoRequest pay
    expected = EchoResponse pay
    cfg      = ClientConfig
                 (Host . fromMaybe "localhost" . unHelpful $ bind)
                 (Port . fromMaybe 50051       . unHelpful $ port)
                 [] Nothing
  withGRPC $ \g -> withClient g cfg $ \c -> do
    Echo{..} <- echoClient c
    echoDoEcho (ClientNormalRequest rqt 5 mempty) >>= \case
      ClientNormalResponse rsp _ _ StatusOk _
        | rsp == expected             -> return ()
        | otherwise                   -> fail $ "Got unexpected response: '" ++ show rsp ++ "', expected: '" ++ show expected ++ "'"
      ClientNormalResponse _ _ _ st _ -> fail $ "Got unexpected status " ++ show st ++ " from call, expecting StatusOk"
      ClientErrorResponse e           -> fail $ "Got client error: " ++ show e
  putStrLn $ "echo-client success: sent " ++ show pay ++ ", got " ++ show pay
