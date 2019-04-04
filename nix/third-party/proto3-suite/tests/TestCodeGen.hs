{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wwarn #-}

module TestCodeGen where

import           ArbitraryGeneratedTestTypes    ()
import           Control.Applicative
import           Control.Monad
import qualified Data.ByteString.Lazy           as LBS
import           Data.Monoid                    ((<>))
import           Data.String                    (IsString)
import qualified Data.Text                      as T
import           Prelude                        hiding (FilePath)
import           Proto3.Suite
import           Proto3.Suite.DotProto.Generate
import           Proto3.Suite.JSONPB            (FromJSONPB (..), Options (..),
                                                 ToJSONPB (..), eitherDecode,
                                                 encode, defaultOptions)
import           System.Exit
import           Test.Tasty
import           Test.Tasty.HUnit               (testCase, (@?=))
import           TestProto
import           TestProtoOneof
import           Turtle                         (FilePath)
import qualified Turtle
import           Turtle.Format                  ((%))
import qualified Turtle.Format                  as F

codeGenTests :: TestTree
codeGenTests = testGroup "Code generator unit tests"
  [ camelCaseMessageNames
  , camelCaseMessageFieldNames
  , don'tAlterEnumFieldNames

-- NOTE(JM): Not currently trivial to make these work with the Buck based
-- build.
-- , simpleEncodeDotProto
-- , simpleDecodeDotProto
  ]

camelCaseMessageNames :: TestTree
camelCaseMessageNames = testGroup "CamelCasing of message names"
  [ testCase "Capitalizes letters after underscores" (typeLikeName "protocol_analysis" @?= Right "ProtocolAnalysis")
  , testCase "Preserves casing of interior letters"  (typeLikeName "analyze_HTTP" @?= Right "AnalyzeHTTP")
  , testCase "Handles non-alphanumeric characters after underscore" (typeLikeName "analyze_http_2" @?= Right "AnalyzeHttp2")
  , testCase "Preserves one underscore in double underscore sequence" (typeLikeName "Analyze__HTTP" @?= Right "Analyze_HTTP")
  , testCase "Handles names prefixed with underscore" (typeLikeName "_message_name" @?= Right "XMessageName")
  , testCase "Preserves trailing underscore" (typeLikeName "message_name_" @?= Right "MessageName_") ]


camelCaseMessageFieldNames :: TestTree
camelCaseMessageFieldNames = testGroup "camelCasing of field names"
  [ testCase "Preserves capitalization patterns" (fieldLikeName "IP" @?= "ip")
  , testCase "Preserves underscores"             (fieldLikeName "IP_address" @?= "ip_address") ]

don'tAlterEnumFieldNames :: TestTree
don'tAlterEnumFieldNames
  = testGroup "Do not alter enumeration field names"
  $ tc <$> [ "fnord"
           , "FNORD"
           , "PascalCase"
           , "camelCase"
           , "VOCIFEROUS_SNAKE_CASE"
           , "snake_case"
           , "snake_case_"
           ]
  where
    enumName     = "MyEnum"
    tc fieldName = testCase fieldName
                 $ prefixedEnumFieldName enumName fieldName
                     @?= Right (enumName <> fieldName)

setPythonPath :: IO ()
setPythonPath = Turtle.export "PYTHONPATH" =<< do
  maybe pyTmpDir ((pyTmpDir <> ":") <>) <$> Turtle.need "PYTHONPATH"

{-
simpleEncodeDotProto :: TestTree
simpleEncodeDotProto =
    testCase "generate code for a simple .proto and then use it to encode messages" $
    do compileTestDotProtos
       -- Compile our generated encoder
       (@?= ExitSuccess) =<< Turtle.proc "tests/encode.sh" [hsTmpDir] empty

       -- The python encoder test exits with a special error code to indicate
       -- all tests were successful
       setPythonPath
       let cmd = hsTmpDir <> "/simpleEncodeDotProto | python tests/check_simple_dot_proto.py"
       (@?= ExitFailure 12) =<< Turtle.shell cmd empty

       -- Not using bracket so that we can inspect the output to fix the tests
       Turtle.rmtree hsTmpDir
       Turtle.rmtree pyTmpDir

simpleDecodeDotProto :: TestTree
simpleDecodeDotProto =
    testCase "generate code for a simple .proto and then use it to decode messages" $
    do compileTestDotProtos
       -- Compile our generated decoder
       (@?= ExitSuccess) =<< Turtle.proc "tests/decode.sh" [hsTmpDir] empty

       setPythonPath
       let cmd = "python tests/send_simple_dot_proto.py | " <> hsTmpDir <> "/simpleDecodeDotProto "
       (@?= ExitSuccess) =<< Turtle.shell cmd empty

       -- Not using bracket so that we can inspect the output to fix the tests
       Turtle.rmtree hsTmpDir
       Turtle.rmtree pyTmpDir
-}

-- * Helpers

-- E.g. dumpAST ["test-files"] "test_proto.proto"
dumpAST :: [FilePath] -> FilePath -> IO ()
dumpAST incs fp = do
  Right (dp, tc) <- readDotProtoWithContext incs fp
  let Right src = renderHsModuleForDotProto mempty dp tc
  putStrLn src

hsTmpDir, pyTmpDir :: IsString a => a
hsTmpDir = "test-files/hs-tmp"
pyTmpDir = "test-files/py-tmp"

compileTestDotProtos :: IO ()
compileTestDotProtos = do
  Turtle.mktree hsTmpDir
  Turtle.mktree pyTmpDir
  forM_ protoFiles $ \protoFile -> do
    compileDotProtoFileOrDie [] hsTmpDir ["test-files"] protoFile
    (@?= ExitSuccess) =<< Turtle.shell (T.concat [ "protoc --python_out="
                                                 , pyTmpDir
                                                 , " --proto_path=test-files"
                                                 , " test-files/" <> Turtle.format F.fp protoFile
                                                 ])
                                       empty
  Turtle.touch (pyTmpDir Turtle.</> "__init__.py")
  where
    protoFiles =
      [ "test_proto.proto"
      , "test_proto_import.proto"
      , "test_proto_oneof.proto"
      , "test_proto_oneof_import.proto"
      ]

-- * Doctests for JSONPB

-- $setup
-- >>> import qualified Data.Text.Lazy as TL
-- >>> import qualified Data.Vector    as V
-- >>> import Proto3.Suite.JSONPB (defaultOptions)
-- >>> :set -XOverloadedStrings
-- >>> :set -XOverloadedLists
-- >>> let omitDefaults = defaultOptions
-- >>> let emitDefaults = defaultOptions{ optEmitDefaultValuedFields = True }

-- | Round-trip tests
-- prop> roundTrip (x :: Trivial)
-- prop> roundTrip (x :: MultipleFields)
-- prop> roundTrip (x :: SignedInts)
-- prop> roundTrip (SignedInts minBound minBound)
-- prop> roundTrip (SignedInts maxBound maxBound)
-- prop> roundTrip (WithEnum (Enumerated (Right x)))
-- prop> roundTrip (x :: WithNesting)
-- prop> roundTrip (x :: WithNestingRepeated)
-- prop> roundTrip (x :: WithNestingRepeatedInts)
-- prop> roundTrip (x :: WithBytes)
-- prop> roundTrip (x :: OutOfOrderFields)
-- prop> roundTrip (x :: UsingImported)
-- prop> roundTrip (x :: Wrapped)
-- prop> roundTrip (x :: Something)
-- prop> roundTrip (x :: WithImported)

-- | Specific encoding tests
-- prop> encodesAs omitDefaults (MultipleFields 0 0 0 0 "" False)                                                         "{}"
-- prop> encodesAs emitDefaults (MultipleFields 0 2.0 0 0 "" True)                                                        "{\"multiFieldDouble\":0.0,\"multiFieldFloat\":2.0,\"multiFieldInt32\":0,\"multiFieldInt64\":\"0\",\"multiFieldString\":\"\",\"multiFieldBool\":true}"
-- prop> encodesAs omitDefaults (SignedInts minBound minBound)                                                            "{\"signed32\":-2147483648,\"signed64\":\"-9223372036854775808\"}"
-- prop> encodesAs omitDefaults (SignedInts maxBound maxBound)                                                            "{\"signed32\":2147483647,\"signed64\":\"9223372036854775807\"}"
-- prop> encodesAs omitDefaults (WithEnum (Enumerated (Right WithEnum_TestEnumENUM1)))                                    "{}"
-- prop> encodesAs emitDefaults (WithEnum (Enumerated (Right WithEnum_TestEnumENUM1)))                                    "{\"enumField\":\"ENUM1\"}"
-- prop> encodesAs omitDefaults (WithEnum (Enumerated (Right WithEnum_TestEnumENUM3)))                                    "{\"enumField\":\"ENUM3\"}"
-- prop> encodesAs omitDefaults (WithNesting $ Just $ WithNesting_Nested "" 0 [1,2] [66,99])                              "{\"nestedMessage\":{\"nestedPacked\":[1,2],\"nestedUnpacked\":[66,99]}}"
-- prop> encodesAs omitDefaults (Something 42 99 (Just (SomethingPickOneName "")))                                        "{\"value\":\"42\",\"another\":99,\"name\":\"\"}"
-- prop> encodesAs omitDefaults (Something 42 99 (Just (SomethingPickOneSomeid 0)))                                       "{\"value\":\"42\",\"another\":99,\"someid\":0}"
-- prop> encodesAs omitDefaults (Something 42 99 (Just (SomethingPickOneDummyMsg1 (DummyMsg 66))))                        "{\"value\":\"42\",\"another\":99,\"dummyMsg1\":{\"dummy\":66}}"
-- prop> encodesAs omitDefaults (Something 42 99 (Just (SomethingPickOneDummyMsg2 (DummyMsg 67))))                        "{\"value\":\"42\",\"another\":99,\"dummyMsg2\":{\"dummy\":67}}"
-- prop> encodesAs omitDefaults (Something 42 99 (Just (SomethingPickOneDummyEnum (Enumerated (Right DummyEnumDUMMY0))))) "{\"value\":\"42\",\"another\":99,\"dummyEnum\":\"DUMMY0\"}"

-- | Specific decoding tests
-- prop> decodesAs "{\"signed32\":2147483647,\"signed64\":\"9223372036854775807\"}"   (SignedInts 2147483647 9223372036854775807)
-- prop> decodesAs "{\"enumField\":\"ENUM3\"}"                                        (WithEnum (Enumerated (Right WithEnum_TestEnumENUM3)))
-- prop> decodesAs "{\"enumField\":null}"                                             (WithEnum (Enumerated (Right WithEnum_TestEnumENUM1)))
-- prop> decodesAs "{}"                                                               (WithEnum (Enumerated (Right WithEnum_TestEnumENUM1)))
-- prop> decodesAs "{\"nestedMessage\":{}}"                                           (WithNesting $ Just $ WithNesting_Nested "" 0 [] [])
-- prop> decodesAs "{\"value\":\"42\",\"another\":99,\"someid\":66}"                  (Something 42 99 (Just (SomethingPickOneSomeid 66)))
-- prop> decodesAs "{\"value\":\"42\",\"another\":99,\"name\":\"foo\"}"               (Something 42 99 (Just (SomethingPickOneName "foo")))
-- prop> decodesAs "{\"value\":\"42\",\"another\":99,\"dummyMsg1\":{\"dummy\":41}}"   (Something 42 99 (Just (SomethingPickOneDummyMsg1 (DummyMsg 41))))
-- prop> decodesAs "{\"value\":\"42\",\"another\":99,\"dummyMsg2\":{\"dummy\":43}}"   (Something 42 99 (Just (SomethingPickOneDummyMsg2 (DummyMsg 43))))
-- prop> decodesAs "{\"value\":\"42\",\"another\":99,\"dummyEnum\":\"DUMMY0\"}"       (Something 42 99 (Just (SomethingPickOneDummyEnum (Enumerated (Right DummyEnumDUMMY0)))))
-- prop> decodesAs "{\"value\":\"42\",\"another\":99}"                                (Something 42 99 Nothing)

-- * Helper quickcheck props

roundTrip :: (ToJSONPB a, FromJSONPB a, Eq a)
          => a -> Bool
roundTrip x = roundTrip' False && roundTrip' True
  where
    roundTrip' emitDefaults =
      eitherDecode (encode defaultOptions{ optEmitDefaultValuedFields = emitDefaults} x)
      ==
      Right x

encodesAs :: (ToJSONPB a)
          => Options -> a -> LBS.ByteString -> Bool
encodesAs opts x bs = encode opts x == bs

decodesAs :: (Eq a, FromJSONPB a)
          => LBS.ByteString -> a -> Bool
decodesAs bs x = eitherDecode bs == Right x
