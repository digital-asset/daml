{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module UnsafeTests (unsafeTests, unsafeProperties) where

import           Control.Exception               (bracket_)
import           Control.Monad
import qualified Data.ByteString                 as B
import           Foreign.Marshal.Alloc
import           Foreign.Storable
import           GHC.Exts
import           Network.GRPC.LowLevel.GRPC      (threadDelaySecs)
import           Network.GRPC.Unsafe
import           Network.GRPC.Unsafe.ByteBuffer
import           Network.GRPC.Unsafe.ChannelArgs
import           Network.GRPC.Unsafe.Metadata
import           Network.GRPC.Unsafe.Security
import           Network.GRPC.Unsafe.Slice
import           Network.GRPC.Unsafe.Time
import           System.Clock
import           Test.QuickCheck.Gen             as QC
import           Test.QuickCheck.Property        as QC hiding (testCase)
import           Test.Tasty
import           Test.Tasty.HUnit                as HU (testCase, (@?=))
import           Test.Tasty.QuickCheck           as QC

unsafeTests :: TestTree
unsafeTests = testGroup "Unit tests for unsafe C bindings"
  [ roundtripSliceUnit "\NULabc\NUL"
  , roundtripSliceUnit largeByteString
  , roundtripByteBufferUnit largeByteString
  , roundtripTimeSpec (TimeSpec 123 123)
  , testMetadata
  , testNow
  , testCreateDestroyMetadata
  , testCreateDestroyMetadataKeyVals
  , testCreateDestroyDeadline
  , testCreateDestroyChannelArgs
  , testCreateDestroyClientCreds
  , testCreateDestroyServerCreds
  ]

unsafeProperties :: TestTree
unsafeProperties = testGroup "QuickCheck properties for unsafe C bindings"
  [ roundtripSliceQC
  , roundtripByteBufferQC
  , roundtripMetadataQC
  , metadataIsList
  ]

instance Arbitrary B.ByteString where
  arbitrary = B.pack <$> arbitrary

instance Arbitrary MetadataMap where
  arbitrary = do
    --keys are not allowed to contain \NUL, but values are.
    ks <- arbitrary `suchThat` all (B.notElem 0)
    let l = length ks
    vs <- vector l
    return $ fromList (zip ks vs)

roundtripMetadataKeyVals :: MetadataMap -> IO MetadataMap
roundtripMetadataKeyVals m = do
  (kvPtr, l) <- createMetadata m
  m' <- getAllMetadata kvPtr l
  metadataFree kvPtr
  return m'

roundtripMetadataQC :: TestTree
roundtripMetadataQC = QC.testProperty "Metadata roundtrip" $
                      \m -> QC.ioProperty $ do m' <- roundtripMetadataKeyVals m
                                               return $ m === m'

metadataIsList :: TestTree
metadataIsList = QC.testProperty "Metadata IsList instance" $
                   \(md :: MetadataMap) -> md == (fromList $ toList md)

largeByteString :: B.ByteString
largeByteString = B.pack $ take (32*1024*1024) $ cycle [97..99]

roundtripSlice :: B.ByteString -> IO B.ByteString
roundtripSlice bs = do
  slice <- byteStringToSlice bs
  unslice <- sliceToByteString slice
  freeSlice slice
  return unslice

roundtripSliceQC :: TestTree
roundtripSliceQC = QC.testProperty "Slice roundtrip: QuickCheck" $
                   \bs -> QC.ioProperty $ do bs' <- roundtripSlice bs
                                             return $ bs == bs'

roundtripSliceUnit :: B.ByteString -> TestTree
roundtripSliceUnit bs = testCase "ByteString slice roundtrip" $ do
  unslice <- roundtripSlice bs
  unslice HU.@?= bs

roundtripByteBuffer :: B.ByteString -> IO B.ByteString
roundtripByteBuffer bs = do
  slice <- byteStringToSlice bs
  buffer <- grpcRawByteBufferCreate slice 1
  reader <- byteBufferReaderCreate buffer
  readSlice <- grpcByteBufferReaderReadall reader
  bs' <- sliceToByteString readSlice
  freeSlice slice
  byteBufferReaderDestroy reader
  grpcByteBufferDestroy buffer
  freeSlice readSlice
  return bs'

roundtripByteBufferQC :: TestTree
roundtripByteBufferQC = QC.testProperty "ByteBuffer roundtrip: QuickCheck" $
                        \bs -> QC.ioProperty $ do bs' <- roundtripByteBuffer bs
                                                  return $ bs == bs'

roundtripByteBufferUnit :: B.ByteString -> TestTree
roundtripByteBufferUnit bs = testCase "ByteBuffer roundtrip" $ do
  bs' <- roundtripByteBuffer bs
  bs' HU.@?= bs

roundtripTimeSpec :: TimeSpec -> TestTree
roundtripTimeSpec t = testCase "CTimeSpec roundtrip" $ do
  p <- malloc
  let c = CTimeSpec t
  poke p c
  c' <- peek p
  c' @?= c
  free p

testMetadata :: TestTree
testMetadata = testCase "Metadata setter/getter roundtrip" $ do
  m <- metadataAlloc 3
  setMetadataKeyVal "hello" "world" m 0
  setMetadataKeyVal "foo" "bar" m 1
  setMetadataKeyVal "Haskell" "Curry" m 2
  k0 <- getMetadataKey m 0
  v0 <- getMetadataVal m 0
  k1 <- getMetadataKey m 1
  v1 <- getMetadataVal m 1
  k2 <- getMetadataKey m 2
  v2 <- getMetadataVal m 2
  k0 HU.@?= "hello"
  v0 HU.@?= "world"
  k1 HU.@?= "foo"
  v1 HU.@?= "bar"
  k2 HU.@?= "Haskell"
  v2 HU.@?= "Curry"
  metadataFree m

currTimeMillis :: ClockType -> IO Int
currTimeMillis t = do
  gprT <- gprNow t
  tMillis <- gprTimeToMillis gprT
  timespecDestroy gprT
  return tMillis

testNow :: TestTree
testNow = testCase "Create/destroy various clock types" $ do
  _ <- currTimeMillis GprClockMonotonic
  _ <- currTimeMillis GprClockRealtime
  _ <- currTimeMillis GprClockPrecise
  return ()

testCreateDestroyMetadata :: TestTree
testCreateDestroyMetadata = testCase "Create/destroy metadataArrayPtr" $ do
  grpc $ withMetadataArrayPtr $ const $ return ()

testCreateDestroyMetadataKeyVals :: TestTree
testCreateDestroyMetadataKeyVals = testCase "Create/destroy metadata key/values" $ do
  grpc $ withMetadataKeyValPtr 10 $ const $ return ()

testCreateDestroyDeadline :: TestTree
testCreateDestroyDeadline = testCase "Create/destroy deadline" $ do
  grpc $ withDeadlineSeconds 10 $ const $ return ()

testCreateDestroyChannelArgs :: TestTree
testCreateDestroyChannelArgs = testCase "Create/destroy channel args" $
  grpc $ withChannelArgs [CompressionAlgArg GrpcCompressDeflate] $
  const $ return ()

testCreateDestroyClientCreds :: TestTree
testCreateDestroyClientCreds = testCase "Create/destroy client credentials" $
  grpc $ withChannelCredentials Nothing Nothing Nothing $ const $ return ()

testCreateDestroyServerCreds :: TestTree
testCreateDestroyServerCreds = testCase "Create/destroy server credentials" $
  grpc $ withServerCredentials Nothing
                               "tests/ssl/testServerKey.pem"
                               "tests/ssl/testServerCert.pem"
                               SslDontRequestClientCertificate
                               $ const $ return ()

assertCqEventComplete :: Event -> IO ()
assertCqEventComplete e = do
  eventCompletionType e HU.@?= OpComplete
  eventSuccess e HU.@?= True

grpc :: IO a -> IO ()
grpc = bracket_ grpcInit grpcShutdown . void

_nowarnUnused :: a
_nowarnUnused = assertCqEventComplete `undefined` threadDelaySecs
