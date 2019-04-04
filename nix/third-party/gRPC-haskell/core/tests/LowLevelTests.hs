{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE OverloadedLists            #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE PatternSynonyms            #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TupleSections              #-}
{-# OPTIONS_GHC -fno-warn-orphans       #-}

module LowLevelTests where

import           Control.Concurrent                        (threadDelay)
import           Control.Concurrent.Async
import           Control.Monad
import           Control.Monad.Managed
import           Data.ByteString                           (ByteString,
                                                            isPrefixOf,
                                                            isSuffixOf)
import           Data.List                                 (find)
import qualified Data.Map.Strict                           as M
import qualified Data.Set                                  as S
import           GHC.Exts                                  (fromList, toList)
import           Network.GRPC.Unsafe.ChannelArgs           (Arg(..))
import           Network.GRPC.LowLevel
import qualified Network.GRPC.LowLevel.Call.Unregistered   as U
import qualified Network.GRPC.LowLevel.Client.Unregistered as U
import           Network.GRPC.LowLevel.GRPC                (threadDelaySecs)
import qualified Network.GRPC.LowLevel.Server.Unregistered as U
import qualified Pipes                                     as P
import           Test.Tasty
import           Test.Tasty.HUnit                          as HU (Assertion,
                                                                  assertBool,
                                                                  assertEqual,
                                                                  assertFailure,
                                                                  testCase,
                                                                  (@?=))

lowLevelTests :: TestTree
lowLevelTests = testGroup "Unit tests of low-level Haskell library"
  [ testGRPCBracket
  , testCompletionQueueCreateDestroy
  , testClientCreateDestroy
  , testClientCall
  , testClientTimeoutNoServer
  , testServerCreateDestroy
  , testMixRegisteredUnregistered
  , testPayload
  , testSSL
  , testAuthMetadataTransfer
  , testServerAuthProcessorCancel
  , testPayloadUnregistered
  , testServerCancel
  , testGoaway
  , testSlowServer
  , testServerCallExpirationCheck
  , testCustomUserAgent
  , testClientCompression
  , testClientServerCompression
  , testClientMaxReceiveMessageLengthChannelArg
  , testClientStreaming
  , testClientStreamingUnregistered
  , testServerStreaming
  , testServerStreamingUnregistered
  , testBiDiStreaming
  , testBiDiStreamingUnregistered
  ]

testGRPCBracket :: TestTree
testGRPCBracket =
  testCase "Start/stop GRPC" $ runManaged $ void mgdGRPC

testCompletionQueueCreateDestroy :: TestTree
testCompletionQueueCreateDestroy =
  testCase "Create/destroy CQ" $ runManaged $ do
    g <- mgdGRPC
    liftIO (withCompletionQueue g nop)

testClientCreateDestroy :: TestTree
testClientCreateDestroy =
  clientOnlyTest "start/stop" nop

testClientTimeoutNoServer :: TestTree
testClientTimeoutNoServer =
  clientOnlyTest "request timeout when server DNE" $ \c -> do
    rm <- clientRegisterMethodNormal c "/foo"
    r  <- clientRequest c rm 1 "Hello" mempty
    r @?= Left GRPCIOTimeout

testServerCreateDestroy :: TestTree
testServerCreateDestroy =
  serverOnlyTest "start/stop" (["/foo"],[],[],[]) nop

testMixRegisteredUnregistered :: TestTree
testMixRegisteredUnregistered =
  csTest "server uses unregistered calls to handle unknown endpoints"
         client
         server
         (["/foo"],[],[],[])
  where
    client c = do
      rm1 <- clientRegisterMethodNormal c "/foo"
      rm2 <- clientRegisterMethodNormal c "/bar"
      clientRequest c rm1 1 "Hello" mempty >>= do
        checkReqRslt $ \NormalRequestResult{..} -> do
          rspBody @?= "reply test"
          initMD @?= dummyMeta
          trailMD @?= dummyMeta
      clientRequest c rm2 1 "bad endpoint" mempty >>= do
        checkReqRslt $ \NormalRequestResult{..} -> do
          rspBody @?= ""
      return ()
    server s = do
       concurrently regThread unregThread
       return ()
       where regThread = do
               let rm = head (normalMethods s)
               _r <- serverHandleNormalCall s rm dummyMeta $ \c -> do
                 payload c @?= "Hello"
                 return ("reply test", dummyMeta, StatusOk, "")
               return ()
             unregThread = do
               U.serverHandleNormalCall s mempty $ \call _ -> do
                 U.callMethod call @?= "/bar"
                 return ("", mempty, StatusOk,
                         StatusDetails "Wrong endpoint")
               return ()

-- TODO: There seems to be a race here (and in other client/server pairs, of
-- course) about what gets reported when there is a failure. E.g., if one of the
-- Assertions fails in the request processing block for the server, we /may/ get
-- that error reported accurately as a call cancellation on the client, rather
-- than a useful error about the failure on the server. Maybe we'll need to
-- tweak EH behavior / async use.
testPayload :: TestTree
testPayload =
  csTest "registered normal request/response" client server (["/foo"],[],[],[])
  where
    clientMD = [ ("foo_key", "foo_val")
               , ("bar_key", "bar_val")
               , ("bar_key", "bar_repeated_val")]
    client c = do
      rm <- clientRegisterMethodNormal c "/foo"
      clientRequest c rm 10 "Hello!" clientMD >>= do
        checkReqRslt $ \NormalRequestResult{..} -> do
          rspCode @?= StatusOk
          rspBody @?= "reply test"
          details @?= "details string"
          initMD  @?= dummyMeta
          trailMD @?= dummyMeta
    server s = do
      let rm = head (normalMethods s)
      r <- serverHandleNormalCall s rm dummyMeta $ \c -> do
        payload c @?= "Hello!"
        checkMD "Server metadata mismatch" clientMD (metadata c)
        return ("reply test", dummyMeta, StatusOk, "details string")
      r @?= Right ()

testSSL :: TestTree
testSSL =
  csTest' "request/response using SSL" client server
  where
    clientConf = stdClientConf
                 {clientSSLConfig = Just (ClientSSLConfig
                                            (Just "tests/ssl/localhost.crt")
                                            Nothing
                                            Nothing)
                }
    client = TestClient clientConf $ \c -> do
      rm <- clientRegisterMethodNormal c "/foo"
      clientRequest c rm 10 "hi" mempty >>= do
        checkReqRslt $ \NormalRequestResult{..} -> do
          rspCode @?= StatusOk
          rspBody @?= "reply test"

    serverConf' = defServerConf
                  { sslConfig = Just (ServerSSLConfig
                                        Nothing
                                        "tests/ssl/localhost.key"
                                        "tests/ssl/localhost.crt"
                                        SslDontRequestClientCertificate
                                        Nothing)
                  }
    server = TestServer serverConf' $ \s -> do
      r <- U.serverHandleNormalCall s mempty $ \U.ServerCall{..} body -> do
        body @?= "hi"
        return ("reply test", mempty, StatusOk, "")
      r @?= Right ()

-- NOTE: With auth plugin tests, if an auth plugin decides someone isn't
-- authenticated, then the call never happens from the perspective of
-- the server, so the server will continue to block waiting for a call. So, if
-- these tests hang forever, it's because auth failed and the server is still
-- waiting for a successfully authenticated call to come in.

testServerAuthProcessorCancel :: TestTree
testServerAuthProcessorCancel =
  csTest' "request rejection by auth processor" client server
  where
    clientConf = stdClientConf
                 {clientSSLConfig = Just (ClientSSLConfig
                                            (Just "tests/ssl/localhost.crt")
                                            Nothing
                                            Nothing)
                }
    client = TestClient clientConf $ \c -> do
      rm <- clientRegisterMethodNormal c "/foo"
      r <- clientRequest c rm 10 "hi" mempty
      -- TODO: using checkReqRslt on this first result causes the test to hang!
      r @?= Left (GRPCIOBadStatusCode StatusUnauthenticated "denied!")
      clientRequest c rm 10 "hi" [("foo","bar")] >>= do
        checkReqRslt $ \NormalRequestResult{..} -> do
          rspCode @?= StatusOk
          rspBody @?= "reply test"

    serverProcessor = Just $ \_ m -> do
      let (status, details) = if M.member "foo" (unMap m)
                                 then (StatusOk, "")
                                 else (StatusUnauthenticated, "denied!")
      return $ AuthProcessorResult mempty mempty status details

    serverConf' = defServerConf
                  { sslConfig = Just (ServerSSLConfig
                                        Nothing
                                        "tests/ssl/localhost.key"
                                        "tests/ssl/localhost.crt"
                                        SslDontRequestClientCertificate
                                        serverProcessor)
                  }
    server = TestServer serverConf' $ \s -> do
      r <- U.serverHandleNormalCall s mempty $ \U.ServerCall{..} _body -> do
        checkMD "Handler only sees requests with good metadata"
                [("foo","bar")]
                metadata
        return ("reply test", mempty, StatusOk, "")
      r @?= Right ()

testAuthMetadataTransfer :: TestTree
testAuthMetadataTransfer =
  csTest' "Auth metadata changes sent from client to server" client server
  where
    plugin :: ClientMetadataCreate
    plugin authMetaCtx = do
      let authCtx = (channelAuthContext authMetaCtx)

      addAuthProperty authCtx (AuthProperty "foo1" "bar1")
      newProps <- getAuthProperties authCtx
      let addedProp = find ((== "foo1") . authPropName) newProps
      addedProp @?= Just (AuthProperty "foo1" "bar1")
      return $ ClientMetadataCreateResult [("foo","bar")] StatusOk ""
    clientConf = stdClientConf
                 {clientSSLConfig = Just (ClientSSLConfig
                                            (Just "tests/ssl/localhost.crt")
                                            Nothing
                                            (Just plugin))
                }
    client = TestClient clientConf $ \c -> do
      rm <- clientRegisterMethodNormal c "/foo"
      clientRequest c rm 10 "hi" mempty >>= do
        checkReqRslt $ \NormalRequestResult{..} -> do
          rspCode @?= StatusOk
          rspBody @?= "reply test"

    serverProcessor :: Maybe ProcessMeta
    serverProcessor = Just $ \authCtx m -> do
      let expected = fromList [("foo","bar")]

      props <- getAuthProperties authCtx
      let clientProp = find ((== "foo1") . authPropName) props
      assertBool "server plugin doesn't see auth properties set by client"
                 (clientProp == Nothing)
      checkMD "server plugin sees metadata added by client plugin" expected m
      return $ AuthProcessorResult mempty mempty StatusOk ""

    serverConf' = defServerConf
                  { sslConfig = Just (ServerSSLConfig
                                        Nothing
                                        "tests/ssl/localhost.key"
                                        "tests/ssl/localhost.crt"
                                        SslDontRequestClientCertificate
                                        serverProcessor)
                  }
    server = TestServer serverConf' $ \s -> do
      r <- U.serverHandleNormalCall s mempty $ \U.ServerCall{..} body -> do
        body @?= "hi"
        return ("reply test", mempty, StatusOk, "")
      r @?= Right ()

-- TODO: auth metadata doesn't propagate from parent calls to child calls.
-- Once we implement our own system for doing so, update this test and add it
-- to the tests list.
testAuthMetadataPropagate :: TestTree
testAuthMetadataPropagate = testCase "auth metadata inherited by children" $ do
  c <- async client
  s1 <- async server
  s2 <- async server2
  wait c
  wait s1
  wait s2
  return ()
  where
    clientPlugin _ =
      return $ ClientMetadataCreateResult [("foo","bar")] StatusOk ""
    clientConf = stdClientConf
                 {clientSSLConfig = Just (ClientSSLConfig
                                            (Just "tests/ssl/localhost.crt")
                                            Nothing
                                            (Just clientPlugin))
                }
    client = do
      threadDelaySecs 3
      withGRPC $ \g -> withClient g clientConf $ \c -> do
        rm <- clientRegisterMethodNormal c "/foo"
        clientRequest c rm 10 "hi" mempty >>= do
          checkReqRslt $ \NormalRequestResult{..} -> do
            rspCode @?= StatusOk
            rspBody @?= "reply test"

    server1ServerPlugin _ctx md = do
      checkMD "server1 sees client's auth metadata." [("foo","bar")] md
      -- TODO: add response meta to check, and consume meta to see what happens.
      return $ AuthProcessorResult mempty mempty StatusOk ""

    server1ServerConf = defServerConf
                 {sslConfig = Just (ServerSSLConfig
                                      Nothing
                                      "tests/ssl/localhost.key"
                                      "tests/ssl/localhost.crt"
                                      SslDontRequestClientCertificate
                                      (Just server1ServerPlugin)),
                  methodsToRegisterNormal = ["/foo"]
                }

    server1ClientPlugin _ =
      return $ ClientMetadataCreateResult [("foo1","bar1")] StatusOk ""

    server1ClientConf = stdClientConf
                 {clientSSLConfig = Just (ClientSSLConfig
                                            (Just "tests/ssl/localhost.crt")
                                            Nothing
                                            (Just server1ClientPlugin)),
                  clientServerPort = 50052
                }

    server = do
      threadDelaySecs 2
      withGRPC $ \g -> withServer g server1ServerConf $ \s ->
        withClient g server1ClientConf $ \c -> do
          let rm = head (normalMethods s)
          serverHandleNormalCall s rm mempty $ \call -> do
            rmc <- clientRegisterMethodNormal c "/foo"
            res <- clientRequestParent c (Just call) rmc 10 "hi" mempty
            case res of
              Left _ ->
                error "got bad result from server2"
              Right (NormalRequestResult{..}) ->
                return (rspBody, mempty, StatusOk, "")

    server2ServerPlugin _ctx md = do
      print md
      checkMD "server2 sees server1's auth metadata." [("foo1","bar1")] md
      --TODO: this assert fails
      checkMD "server2 sees client's auth metadata." [("foo","bar")] md
      return $ AuthProcessorResult mempty mempty StatusOk ""

    server2ServerConf = defServerConf
                 {sslConfig = Just (ServerSSLConfig
                                      Nothing
                                      "tests/ssl/localhost.key"
                                      "tests/ssl/localhost.crt"
                                      SslDontRequestClientCertificate
                                      (Just server2ServerPlugin)),
                  methodsToRegisterNormal = ["/foo"],
                  port = 50052
                }

    server2 = withGRPC $ \g -> withServer g server2ServerConf $ \s -> do
        let rm = head (normalMethods s)
        serverHandleNormalCall s rm mempty $ \_call -> do
          return ("server2 reply", mempty, StatusOk, "")

testServerCancel :: TestTree
testServerCancel =
  csTest "server cancel call" client server (["/foo"],[],[],[])
  where
    client c = do
      rm <- clientRegisterMethodNormal c "/foo"
      Left (GRPCIOBadStatusCode s _) <- clientRequest c rm 10 "" mempty
      s @?= StatusCancelled
    server s = do
      let rm = head (normalMethods s)
      r <- serverHandleNormalCall s rm mempty $ \c -> do
        serverCallCancel c StatusCancelled ""
        return (mempty, mempty, StatusCancelled, "")
      r @?= Right ()

testServerStreaming :: TestTree
testServerStreaming =
  csTest "server streaming" client server ([],[],["/feed"],[])
  where
    clientInitMD = [("client","initmd")]
    serverInitMD = [("server","initmd")]
    clientPay    = "FEED ME!"
    pays         = ["ONE", "TWO", "THREE", "FOUR"] :: [ByteString]

    client c = do
      rm <- clientRegisterMethodServerStreaming c "/feed"
      eea <- clientReader c rm 10 clientPay clientInitMD $ \initMD recv -> do
        checkMD "Server initial metadata mismatch" serverInitMD initMD
        forM_ pays $ \p -> recv `is` Right (Just p)
        recv `is` Right Nothing
      eea @?= Right (dummyMeta, StatusOk, "dtls")

    server s = do
      let rm = head (sstreamingMethods s)
      r <- serverWriter s rm serverInitMD $ \sc send -> do
        checkMD "Server request metadata mismatch" clientInitMD (metadata sc)
        payload sc @?= clientPay
        forM_ pays $ \p -> send p `is` Right ()
        return (dummyMeta, StatusOk, "dtls")
      r @?= Right ()

-- TODO: these unregistered streaming tests are basically the same as the
-- registered ones. Reduce duplication.
-- TODO: Once client-side unregistered streaming functions are added, switch
-- to using them in these tests.
testServerStreamingUnregistered :: TestTree
testServerStreamingUnregistered =
  csTest "unregistered server streaming" client server ([],[],[],[])
  where
    clientInitMD = [("client","initmd")]
    serverInitMD = [("server","initmd")]
    clientPay    = "FEED ME!"
    pays         = ["ONE", "TWO", "THREE", "FOUR"] :: [ByteString]

    client c = do
      rm <- clientRegisterMethodServerStreaming c "/feed"
      eea <- clientReader c rm 10 clientPay clientInitMD $ \initMD recv -> do
        checkMD "Server initial metadata mismatch" serverInitMD initMD
        forM_ pays $ \p -> recv `is` Right (Just p)
        recv `is` Right Nothing
      eea @?= Right (dummyMeta, StatusOk, "dtls")

    server s = U.withServerCallAsync s $ \call -> do
      r <- U.serverWriter s call serverInitMD $ \sc send -> do
        checkMD "Server request metadata mismatch" clientInitMD (metadata sc)
        payload sc @?= clientPay
        forM_ pays $ \p -> send p `is` Right ()
        return (dummyMeta, StatusOk, "dtls")
      r @?= Right ()

testClientStreaming :: TestTree
testClientStreaming =
  csTest "client streaming" client server ([],["/slurp"],[],[])
  where
    clientInitMD = [("a","b")]
    serverInitMD = [("x","y")]
    trailMD      = dummyMeta
    serverRsp    = "serverReader reply"
    serverDtls   = "deets"
    serverStatus = StatusOk
    pays         = ["P_ONE", "P_TWO", "P_THREE"] :: [ByteString]

    client c = do
      rm  <- clientRegisterMethodClientStreaming c "/slurp"
      eea <- clientWriter c rm 10 clientInitMD $ \send -> do
        -- liftIO $ checkMD "Server initial metadata mismatch" serverInitMD initMD
        forM_ pays $ \p -> send p `is` Right ()
      eea @?= Right (Just serverRsp, serverInitMD, trailMD, serverStatus, serverDtls)

    server s = do
      let rm = head (cstreamingMethods s)
      eea <- serverReader s rm serverInitMD $ \sc recv -> do
        checkMD "Client request metadata mismatch" clientInitMD (metadata sc)
        forM_ pays $ \p -> recv `is` Right (Just p)
        recv `is` Right Nothing
        return (Just serverRsp, trailMD, serverStatus, serverDtls)
      eea @?= Right ()

testClientStreamingUnregistered :: TestTree
testClientStreamingUnregistered =
  csTest "unregistered client streaming" client server ([],[],[],[])
  where
    clientInitMD = [("a","b")]
    serverInitMD = [("x","y")]
    trailMD      = dummyMeta
    serverRsp    = "serverReader reply"
    serverDtls   = "deets"
    serverStatus = StatusOk
    pays         = ["P_ONE", "P_TWO", "P_THREE"] :: [ByteString]

    client c = do
      rm  <- clientRegisterMethodClientStreaming c "/slurp"
      eea <- clientWriter c rm 10 clientInitMD $ \send -> do
        -- liftIO $ checkMD "Server initial metadata mismatch" serverInitMD initMD
        forM_ pays $ \p -> send p `is` Right ()
      eea @?= Right (Just serverRsp, serverInitMD, trailMD, serverStatus, serverDtls)

    server s = U.withServerCallAsync s $ \call -> do
      eea <- U.serverReader s call serverInitMD $ \sc recv -> do
        checkMD "Client request metadata mismatch" clientInitMD (metadata sc)
        forM_ pays $ \p -> recv `is` Right (Just p)
        recv `is` Right Nothing
        return (Just serverRsp, trailMD, serverStatus, serverDtls)
      eea @?= Right ()

testBiDiStreaming :: TestTree
testBiDiStreaming =
  csTest "bidirectional streaming" client server ([],[],[],["/bidi"])
  where
    clientInitMD = [("bidi-streaming","client")]
    serverInitMD = [("bidi-streaming","server")]
    trailMD      = dummyMeta
    serverStatus = StatusOk
    serverDtls   = "deets"

    client c = do
      rm  <- clientRegisterMethodBiDiStreaming c "/bidi"
      eea <- clientRW c rm 10 clientInitMD $ \getMD recv send writesDone -> do
        either clientFail (checkMD "Server rsp metadata mismatch" serverInitMD) =<< getMD
        send "cw0" `is` Right ()
        recv       `is` Right (Just "sw0")
        send "cw1" `is` Right ()
        recv       `is` Right (Just "sw1")
        recv       `is` Right (Just "sw2")
        writesDone `is` Right ()
        recv       `is` Right Nothing
      eea @?= Right (trailMD, serverStatus, serverDtls)

    server s = do
      let rm = head (bidiStreamingMethods s)
      eea <- serverRW s rm serverInitMD $ \sc recv send -> do
        checkMD "Client request metadata mismatch" clientInitMD (metadata sc)
        recv       `is` Right (Just "cw0")
        send "sw0" `is` Right ()
        recv       `is` Right (Just "cw1")
        send "sw1" `is` Right ()
        send "sw2" `is` Right ()
        recv       `is` Right Nothing
        return (trailMD, serverStatus, serverDtls)
      eea @?= Right ()

testBiDiStreamingUnregistered :: TestTree
testBiDiStreamingUnregistered =
  csTest "unregistered bidirectional streaming" client server ([],[],[],[])
  where
    clientInitMD = [("bidi-streaming","client")]
    serverInitMD = [("bidi-streaming","server")]
    trailMD      = dummyMeta
    serverStatus = StatusOk
    serverDtls   = "deets"

    client c = do
      rm  <- clientRegisterMethodBiDiStreaming c "/bidi"
      eea <- clientRW c rm 10 clientInitMD $ \getMD recv send writesDone -> do
        either clientFail (checkMD "Server rsp metadata mismatch" serverInitMD) =<< getMD
        send "cw0" `is` Right ()
        recv       `is` Right (Just "sw0")
        send "cw1" `is` Right ()
        recv       `is` Right (Just "sw1")
        recv       `is` Right (Just "sw2")
        writesDone `is` Right ()
        recv       `is` Right Nothing
      eea @?= Right (trailMD, serverStatus, serverDtls)

    server s = U.withServerCallAsync s $ \call -> do
      eea <- U.serverRW s call serverInitMD $ \sc recv send -> do
        checkMD "Client request metadata mismatch" clientInitMD (metadata sc)
        recv       `is` Right (Just "cw0")
        send "sw0" `is` Right ()
        recv       `is` Right (Just "cw1")
        send "sw1" `is` Right ()
        send "sw2" `is` Right ()
        recv       `is` Right Nothing
        return (trailMD, serverStatus, serverDtls)
      eea @?= Right ()

--------------------------------------------------------------------------------
-- Unregistered tests

testClientCall :: TestTree
testClientCall =
  clientOnlyTest "create/destroy call" $ \c -> do
    r <- U.withClientCall c "/foo" 10 $ const $ return $ Right ()
    r @?= Right ()

testServerCall :: TestTree
testServerCall =
  serverOnlyTest "create/destroy call" ([],[],[],[]) $ \s -> do
    r <- U.withServerCall s $ const $ return $ Right ()
    r @?= Left GRPCIOTimeout

testPayloadUnregistered :: TestTree
testPayloadUnregistered =
  csTest "unregistered normal request/response" client server ([],[],[],[])
  where
    client c =
      U.clientRequest c "/foo" 10 "Hello!" mempty >>= do
        checkReqRslt $ \NormalRequestResult{..} -> do
          rspCode @?= StatusOk
          rspBody @?= "reply test"
          details @?= "details string"
    server s = do
      r <- U.serverHandleNormalCall s mempty $ \U.ServerCall{..} body -> do
             body @?= "Hello!"
             callMethod @?= "/foo"
             return ("reply test", mempty, StatusOk, "details string")
      r @?= Right ()

testGoaway :: TestTree
testGoaway =
  csTest "Client handles server shutdown gracefully"
         client
         server
         (["/foo"],[],[],[])
  where
    client c = do
      rm <- clientRegisterMethodNormal c "/foo"
      clientRequest c rm 10 "" mempty
      clientRequest c rm 10 "" mempty
      eer <- clientRequest c rm 1 "" mempty
      assertBool "Client handles server shutdown gracefully" $ case eer of
        Left (GRPCIOBadStatusCode StatusUnavailable _)                        -> True
        Left (GRPCIOBadStatusCode StatusDeadlineExceeded "Deadline Exceeded") -> True
        Left GRPCIOTimeout                                                    -> True
        _                                                                     -> False

    server s = do
      let rm = head (normalMethods s)
      serverHandleNormalCall s rm mempty dummyHandler
      serverHandleNormalCall s rm mempty dummyHandler
      return ()

testSlowServer :: TestTree
testSlowServer =
  csTest "Client handles slow server response" client server (["/foo"],[],[],[])
  where
    client c = do
      rm <- clientRegisterMethodNormal c "/foo"
      result <- clientRequest c rm 1 "" mempty
      result @?= Left (GRPCIOBadStatusCode StatusDeadlineExceeded "Deadline Exceeded")
    server s = do
      let rm = head (normalMethods s)
      serverHandleNormalCall s rm mempty $ \_ -> do
        threadDelay (2*10^(6 :: Int))
        return dummyResp
      return ()

testServerCallExpirationCheck :: TestTree
testServerCallExpirationCheck =
  csTest "Check for call expiration" client server (["/foo"],[],[],[])
  where
    client c = do
      rm <- clientRegisterMethodNormal c "/foo"
      void $ clientRequest c rm 3 "" mempty
    server s = do
      let rm = head (normalMethods s)
      serverHandleNormalCall s rm mempty $ \c -> do
        exp1 <- serverCallIsExpired c
        assertBool "Call isn't expired when handler starts" $ not exp1
        threadDelaySecs 1
        exp2 <- serverCallIsExpired c
        assertBool "Call isn't expired after 1 second" $ not exp2
        threadDelaySecs 3
        exp3 <- serverCallIsExpired c
        assertBool "Call is expired after 4 seconds" exp3
        return dummyResp
      return ()

testCustomUserAgent :: TestTree
testCustomUserAgent =
  csTest' "Server sees custom user agent prefix/suffix" client server
  where
    clientArgs = [UserAgentPrefix "prefix!", UserAgentSuffix "suffix!"]
    client =
      TestClient (ClientConfig "localhost" 50051 clientArgs Nothing) $
        \c -> do rm <- clientRegisterMethodNormal c "/foo"
                 void $ clientRequest c rm 4 "" mempty
    server = TestServer (serverConf (["/foo"],[],[],[])) $ \s -> do
      let rm = head (normalMethods s)
      serverHandleNormalCall s rm mempty $ \c -> do
        ua <- case toList $ (unMap $ metadata c) M.! "user-agent" of
                []   -> fail "user-agent missing from metadata."
                [ua] -> return ua
                _    -> fail "multiple user-agent keys."
        assertBool "User agent prefix is present" $ isPrefixOf "prefix!" ua
        assertBool "User agent suffix is present" $ isSuffixOf "suffix!" ua
        return dummyResp
      return ()

testClientCompression :: TestTree
testClientCompression =
  csTest' "client-only compression: no errors" client server
  where
    client =
      TestClient (ClientConfig
                   "localhost"
                   50051
                   [CompressionAlgArg GrpcCompressDeflate]
                   Nothing) $ \c -> do
        rm <- clientRegisterMethodNormal c "/foo"
        void $ clientRequest c rm 1 "hello" mempty
    server = TestServer (serverConf (["/foo"],[],[],[])) $ \s -> do
      let rm = head (normalMethods s)
      serverHandleNormalCall s rm mempty $ \c -> do
        payload c @?= "hello"
        return dummyResp
      return ()

testClientServerCompression :: TestTree
testClientServerCompression =
  csTest' "client/server compression: no errors" client server
  where
    cconf = ClientConfig "localhost"
                         50051
                         [CompressionAlgArg GrpcCompressDeflate]
                         Nothing
    client = TestClient cconf $ \c -> do
      rm <- clientRegisterMethodNormal c "/foo"
      clientRequest c rm 1 "hello" mempty >>= do
        checkReqRslt $ \NormalRequestResult{..} -> do
          rspCode @?= StatusOk
          rspBody @?= "hello"
          details @?= ""
          initMD  @?= dummyMeta
          trailMD @?= dummyMeta
      return ()
    sconf = ServerConfig "localhost"
                         50051
                         ["/foo"] [] [] []
                         [CompressionAlgArg GrpcCompressDeflate]
                         Nothing
    server = TestServer sconf $ \s -> do
      let rm = head (normalMethods s)
      serverHandleNormalCall s rm dummyMeta $ \sc -> do
        payload sc @?= "hello"
        return ("hello", dummyMeta, StatusOk, StatusDetails "")
      return ()

testClientServerCompressionLvl :: TestTree
testClientServerCompressionLvl =
  csTest' "client/server compression: no errors" client server
  where
    cconf = ClientConfig "localhost"
                         50051
                         [CompressionLevelArg GrpcCompressLevelHigh]
                         Nothing
    client = TestClient cconf $ \c -> do
      rm <- clientRegisterMethodNormal c "/foo"
      clientRequest c rm 1 "hello" mempty >>= do
        checkReqRslt $ \NormalRequestResult{..} -> do
          rspCode @?= StatusOk
          rspBody @?= "hello"
          details @?= ""
          initMD  @?= dummyMeta
          trailMD @?= dummyMeta
      return ()
    sconf = ServerConfig "localhost"
                         50051
                         ["/foo"] [] [] []
                         [CompressionLevelArg GrpcCompressLevelLow]
                         Nothing
    server = TestServer sconf $ \s -> do
      let rm = head (normalMethods s)
      serverHandleNormalCall s rm dummyMeta $ \sc -> do
        payload sc @?= "hello"
        return ("hello", dummyMeta, StatusOk, StatusDetails "")
      return ()

testClientMaxReceiveMessageLengthChannelArg :: TestTree
testClientMaxReceiveMessageLengthChannelArg = do
  testGroup "max receive message length channel arg (client channel)"
    [ csTest' "payload size < small bound succeeds" shouldSucceed server
    , csTest' "payload size > small bound fails"    shouldFail    server
    ]
  where
    -- The server always sends a 4-byte payload
    pay    = "four"
    server = TestServer (ServerConfig "localhost" 50051 ["/foo"] [] [] [] [] Nothing) $ \s -> do
      let rm = head (normalMethods s)
      void $ serverHandleNormalCall s rm mempty $ \sc -> do
        payload sc @?= pay
        pure (pay, mempty, StatusOk, StatusDetails "")

    clientMax n k = TestClient conf $ \c -> do
      rm <- clientRegisterMethodNormal c "/foo"
      clientRequest c rm 1 pay mempty >>= k
      where
        conf = ClientConfig "localhost" 50051 [MaxReceiveMessageLength n] Nothing

    -- Expect success when the max recv payload size is set to 4 bytes, and we
    -- are sent 4.
    shouldSucceed = clientMax 4 $ checkReqRslt $ \NormalRequestResult{..} -> do
      rspCode @?= StatusOk
      rspBody @?= pay
      details @?= ""

    -- Expect failure when the max recv payload size is set to 3 bytes, and we
    -- are sent 4.
    shouldFail = clientMax 3 $ \case
        Left (GRPCIOBadStatusCode StatusResourceExhausted _)
          -> pure ()
        rsp
          -> clientFail ("Expected failure response, but got: " ++ show rsp)

--------------------------------------------------------------------------------
-- Utilities and helpers

is :: (Eq a, Show a, MonadIO m) => m a -> a -> m ()
is act x = act >>= liftIO . (@?= x)

dummyMeta :: MetadataMap
dummyMeta = [("foo","bar")]

dummyResp :: (ByteString, MetadataMap, StatusCode, StatusDetails)
dummyResp = ("", mempty, StatusOk, StatusDetails "")

dummyHandler :: ServerCall a
                -> IO (ByteString, MetadataMap, StatusCode, StatusDetails)
dummyHandler _ = return dummyResp

dummyResult' :: StatusDetails
             -> IO (ByteString, MetadataMap, StatusCode, StatusDetails)
dummyResult' = return . (mempty, mempty, StatusOk, )

nop :: Monad m => a -> m ()
nop = const (return ())

serverOnlyTest :: TestName
               -> ([MethodName],[MethodName],[MethodName],[MethodName])
               -> (Server -> IO ())
               -> TestTree
serverOnlyTest nm ms =
  testCase ("Server - " ++ nm) . runTestServer . TestServer (serverConf ms)

clientOnlyTest :: TestName -> (Client -> IO ()) -> TestTree
clientOnlyTest nm =
  testCase ("Client - " ++ nm) . runTestClient . stdTestClient

csTest :: TestName
       -> (Client -> IO ())
       -> (Server -> IO ())
       -> ([MethodName],[MethodName],[MethodName],[MethodName])
       -> TestTree
csTest nm c s ms =
  csTest' nm (stdTestClient c) (TestServer (serverConf ms) s)

csTest' :: TestName -> TestClient -> TestServer -> TestTree
csTest' nm tc ts =
  testCase ("Client/Server - " ++ nm)
  $ void (s `concurrently` c)
  where
    -- We use a small delay to give the server a head start
    c = threadDelay 100000 >> runTestClient tc
    s = runTestServer ts

-- | @checkMD msg expected actual@ fails when keys from @expected@ are not in
-- @actual@, or when values differ for matching keys.
checkMD :: String -> MetadataMap -> MetadataMap -> Assertion
checkMD desc expected actual =
    assertEqual desc expected' (actual' `S.intersection` expected')
  where
    expected' = fromList . toList $ expected
    actual' = fromList . toList $ actual

checkReqRslt :: Show a => (b -> Assertion) -> Either a b -> Assertion
checkReqRslt = either clientFail

-- | The consumer which asserts that the next value it consumes is equal to the
-- given value; string parameter used as in 'assertEqual'.
assertConsumeEq :: (Eq a, Show a) => String -> a -> P.Consumer a IO ()
assertConsumeEq s v = P.lift . assertEqual s v =<< P.await

clientFail :: Show a => a -> Assertion
clientFail = assertFailure . ("Client error: " ++). show

data TestClient = TestClient ClientConfig (Client -> IO ())

runTestClient :: TestClient -> IO ()
runTestClient (TestClient conf f) =
  runManaged $ mgdGRPC >>= mgdClient conf >>= liftIO . f

stdTestClient :: (Client -> IO ()) -> TestClient
stdTestClient = TestClient stdClientConf

stdClientConf :: ClientConfig
stdClientConf = ClientConfig "localhost" 50051 [] Nothing

data TestServer = TestServer ServerConfig (Server -> IO ())

runTestServer :: TestServer -> IO ()
runTestServer (TestServer conf f) =
  runManaged $ mgdGRPC >>= mgdServer conf >>= liftIO . f

defServerConf :: ServerConfig
defServerConf = ServerConfig "localhost" 50051 [] [] [] [] [] Nothing

serverConf :: ([MethodName],[MethodName],[MethodName],[MethodName])
              -> ServerConfig
serverConf (ns, cs, ss, bs) =
  defServerConf {methodsToRegisterNormal = ns,
                 methodsToRegisterClientStreaming = cs,
                 methodsToRegisterServerStreaming = ss,
                 methodsToRegisterBiDiStreaming = bs}

mgdGRPC :: Managed GRPC
mgdGRPC = managed withGRPC

mgdClient :: ClientConfig -> GRPC -> Managed Client
mgdClient conf g = managed $ withClient g conf

mgdServer :: ServerConfig -> GRPC -> Managed Server
mgdServer conf g = managed $ withServer g conf
