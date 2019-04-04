{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE RecordWildCards #-}

module Main where


import Test.Tasty
import Test.Tasty.HUnit (Assertion, (@?=), (@=?), testCase)
import Control.Applicative
import Control.Monad
import Proto3.Suite
import qualified Data.ByteString.Char8 as BC
import System.IO
import System.Exit

import TestProto
import qualified TestProtoImport
import qualified TestProtoOneof
import qualified TestProtoOneofImport

main :: IO ()
main = do putStr "\n"
          defaultMain tests

tests, testCase1, testCase2, testCase3, testCase4, testCase5,
    testCase6, testCase8, testCase9, testCase10, testCase11,
    testCase12, testCase13, testCase14, testCase15, testCase16,
    testCase17 :: TestTree
tests = testGroup "Decode protobuf messages from Python"
          [  testCase1,  testCase2, testCaseSignedInts
          ,  testCase3,  testCase4,  testCase5,  testCase6
          ,  testCase7,  testCase8,  testCase9, testCase10
          , testCase11, testCase12, testCase13, testCase14
          , testCase15, testCase16, testCase17, testCase18
          , allTestsDone -- this should always run last
          ]

readProto :: Message a => IO a
readProto = do length <- readLn
               res <- fromByteString <$> BC.hGet stdin length
               case res of
                 Left err -> fail ("readProto: " ++ show err)
                 Right  x -> pure x

expect :: (Eq a, Message a, Show a) => a -> Assertion
expect v = (v @=?) =<< readProto

testCase1  = testCase "Trivial message" $
    do Trivial { .. } <- readProto
       trivialTrivialField @?= 0x7BADBEEF

testCase2  = testCase "Multi-field message" $
    do MultipleFields { .. } <- readProto

       multipleFieldsMultiFieldDouble @?= 1.125
       multipleFieldsMultiFieldFloat  @?= 1e9
       multipleFieldsMultiFieldInt32  @?= 0x1135
       multipleFieldsMultiFieldInt64  @?= 0x7FFAFABADDEAFFA0
       multipleFieldsMultiFieldString @?= "Goodnight moon"
       multipleFieldsMultiFieldBool   @?= False

testCaseSignedInts = testCase "Signed integer types" $
    do expect (SignedInts 0 0)
       expect (SignedInts 42 84)
       expect (SignedInts (-42) (-84))
       expect (SignedInts minBound minBound)
       expect (SignedInts maxBound maxBound)

testCase3  = testCase "Nested enumeration" $
    do WithEnum { withEnumEnumField = Enumerated a } <- readProto
       a @?= Right WithEnum_TestEnumENUM1

       WithEnum { withEnumEnumField = Enumerated b } <- readProto
       b @?= Right WithEnum_TestEnumENUM2

       WithEnum { withEnumEnumField = Enumerated c } <- readProto
       c @?= Right WithEnum_TestEnumENUM3

       WithEnum { withEnumEnumField = Enumerated d } <- readProto
       d @?= Left 0xBEEF

testCase4  = testCase "Nested message" $
    do WithNesting { withNestingNestedMessage = a } <- readProto
       a @?= Just (WithNesting_Nested "testCase4 nestedField1" 0xABCD [] [])

       WithNesting { withNestingNestedMessage = b } <- readProto
       b @?= Nothing

testCase5  = testCase "Nested repeated message" $
    do WithNestingRepeated { withNestingRepeatedNestedMessages = a } <- readProto
       length a @?= 3
       let [a1, a2, a3] = a

       a1 @?= WithNestingRepeated_Nested "testCase5 nestedField1" 0xDCBA [1, 1, 2, 3, 5] [0xB, 0xABCD, 0xBADBEEF, 0x10203040]
       a2 @?= WithNestingRepeated_Nested "Hello world" 0x7FFFFFFF [0, 0, 0] []
       a3 @?= WithNestingRepeated_Nested "" 0 [] []

       WithNestingRepeated { withNestingRepeatedNestedMessages = b } <- readProto
       b @?= []

testCase6  = testCase "Nested repeated int message" $
    do WithNestingRepeatedInts { withNestingRepeatedIntsNestedInts = a } <- readProto
       a @?= [ NestedInts 636513 619021 ]

       WithNestingRepeatedInts { withNestingRepeatedIntsNestedInts = b } <- readProto
       b @?= []

       WithNestingRepeatedInts { withNestingRepeatedIntsNestedInts = c } <- readProto
       c @?= [ NestedInts 636513 619021
             , NestedInts 423549 687069
             , NestedInts 545506 143731
             , NestedInts 193605 385360 ]

testCase7  = testCase "Repeated int32 field" $
    do WithRepetition { withRepetitionRepeatedField1 = a } <- readProto
       a @?= []

       WithRepetition { withRepetitionRepeatedField1 = b } <- readProto
       b @?= [1..10000]

testCase8  = testCase "Fixed-width integer types" $
    do WithFixed { .. } <- readProto
       withFixedFixed1 @?= 0
       withFixedFixed2 @?= 0
       withFixedFixed3 @?= 0
       withFixedFixed4 @?= 0

       WithFixed { .. } <- readProto
       withFixedFixed1 @?= maxBound
       withFixedFixed2 @?= maxBound
       withFixedFixed3 @?= maxBound
       withFixedFixed4 @?= maxBound

       WithFixed { .. } <- readProto
       withFixedFixed1 @?= minBound
       withFixedFixed2 @?= minBound
       withFixedFixed3 @?= minBound
       withFixedFixed4 @?= minBound

testCase9  = testCase "Bytes fields" $
    do WithBytes { .. } <- readProto
       withBytesBytes1 @?= "\x00\x00\x00\x01\x02\x03\xFF\xFF\x00\x01"
       withBytesBytes2 @?= ["", "\x01", "\xAB\xBAhello", "\xBB"]

       WithBytes { .. } <- readProto
       withBytesBytes1 @?= "Hello world"
       withBytesBytes2 @?= []

       WithBytes { .. } <- readProto
       withBytesBytes1 @?= ""
       withBytesBytes2 @?= ["Hello", "\x00world", "\x00\x00"]

       WithBytes { .. } <- readProto
       withBytesBytes1 @?= ""
       withBytesBytes2 @?= []

testCase10 = testCase "Packed and unpacked repeated types" $
    do WithPacking { .. } <- readProto
       withPackingPacking1 @?= []
       withPackingPacking2 @?= []

       WithPacking { .. } <- readProto
       withPackingPacking1 @?= [100, 2000, 300, 4000, 500, 60000, 7000]
       withPackingPacking2 @?= []

       WithPacking { .. } <- readProto
       withPackingPacking1 @?= []
       withPackingPacking2 @?= [100, 2000, 300, 4000, 500, 60000, 7000]

       WithPacking { .. } <- readProto
       withPackingPacking1 @?= [1, 2, 3, 4, 5]
       withPackingPacking2 @?= [5, 4, 3, 2, 1]

testCase11 = testCase "All possible packed types" $
    do a <- readProto
       a @?= AllPackedTypes [] [] [] [] [] [] [] [] [] [] [] [] []

       b <- readProto
       b @?= AllPackedTypes [1] [2] [3] [4] [5] [6] [7] [8] [9] [10] [False] [efld0] [efld0]

       c <- readProto
       c @?= AllPackedTypes [1] [2] [-3] [-4] [5] [6] [-7] [-8] [-9] [-10] [True] [efld1] [efld1]

       d <- readProto
       d @?= AllPackedTypes [1..10000] [1..10000]
                            [1..10000] [1..10000]
                            [1..10000] [1..10000]
                            [1,1.125..10000] [1,1.125..10000]
                            [1..10000] [1..10000]
                            [False,True]
                            [efld0,efld1]
                            [efld0,efld1]
    where
      efld0 = Enumerated (Right EFLD0)
      efld1 = Enumerated (Right EFLD1)


testCase12 = testCase "Message with out of order field numbers" $
    do OutOfOrderFields { .. } <- readProto
       outOfOrderFieldsField1 @?= []
       outOfOrderFieldsField2 @?= ""
       outOfOrderFieldsField3 @?= maxBound
       outOfOrderFieldsField4 @?= []

       OutOfOrderFields { .. } <- readProto
       outOfOrderFieldsField1 @?= [1,7..100]
       outOfOrderFieldsField2 @?= "This is a test"
       outOfOrderFieldsField3 @?= minBound
       outOfOrderFieldsField4 @?= ["This", "is", "a", "test"]

testCase13 = testCase "Nested message with the same name as another package-level message" $
    do ShadowedMessage { .. } <- readProto
       shadowedMessageName  @?= "name"
       shadowedMessageValue @?= 0x7DADBEEF

       MessageShadower { .. } <- readProto
       messageShadowerName @?= "another name"
       messageShadowerShadowedMessage @?= Just (MessageShadower_ShadowedMessage "name" "string value")

       MessageShadower_ShadowedMessage { .. } <- readProto
       messageShadower_ShadowedMessageName  @?= "another name"
       messageShadower_ShadowedMessageValue @?= "another string"

testCase14 = testCase "Qualified name resolution" $
    do WithQualifiedName { .. } <- readProto
       withQualifiedNameQname1 @?= Just (ShadowedMessage "int value" 42)
       withQualifiedNameQname2 @?= Just (MessageShadower_ShadowedMessage "string value" "hello world")

testCase15 = testCase "Imported message resolution" $
    do TestProtoImport.WithNesting { .. } <- readProto
       withNestingNestedMessage1 @?= Just (TestProtoImport.WithNesting_Nested 1 2)
       withNestingNestedMessage2 @?= Nothing

testCase16 = testCase "Proper resolution of shadowed message names" $
    do UsingImported { .. } <- readProto
       usingImportedImportedNesting @?=
         Just (TestProtoImport.WithNesting
                 (Just (TestProtoImport.WithNesting_Nested 1 2))
                 (Just (TestProtoImport.WithNesting_Nested 3 4)))
       usingImportedLocalNesting @?= Just (WithNesting (Just (WithNesting_Nested "field" 0xBEEF [] [])))

testCase17 = testCase "Oneof" $ do
    -- Read default values for oneof subfields
    do TestProtoOneof.Something{ .. } <- readProto
       somethingValue   @?= 1
       somethingAnother @?= 2
       somethingPickOne @?= Just (TestProtoOneof.SomethingPickOneName "")
    do TestProtoOneof.Something { .. } <- readProto
       somethingValue   @?= 3
       somethingAnother @?= 4
       somethingPickOne @?= Just (TestProtoOneof.SomethingPickOneSomeid 0)
    do TestProtoOneof.Something { .. } <- readProto
       somethingValue   @?= 5
       somethingAnother @?= 6
       somethingPickOne @?= Just (TestProtoOneof.SomethingPickOneDummyMsg1
                                    (TestProtoOneof.DummyMsg 0))

    do TestProtoOneof.Something { .. } <- readProto
       somethingValue   @?= 7
       somethingAnother @?= 8
       somethingPickOne @?= Just (TestProtoOneof.SomethingPickOneDummyMsg2
                                    (TestProtoOneof.DummyMsg 0))

    do TestProtoOneof.Something { .. } <- readProto
       somethingValue   @?= 9
       somethingAnother @?= 10
       somethingPickOne @?= Just (TestProtoOneof.SomethingPickOneDummyEnum
                                    (Enumerated (Right TestProtoOneof.DummyEnumDUMMY0)))
    -- Read non-default values for oneof subfields
    do TestProtoOneof.Something{ .. } <- readProto
       somethingValue   @?= 1
       somethingAnother @?= 2
       somethingPickOne @?= Just (TestProtoOneof.SomethingPickOneName "hello world")
    do TestProtoOneof.Something { .. } <- readProto
       somethingValue   @?= 3
       somethingAnother @?= 4
       somethingPickOne @?= Just (TestProtoOneof.SomethingPickOneSomeid 42)
    do TestProtoOneof.Something { .. } <- readProto
       somethingValue   @?= 5
       somethingAnother @?= 6
       somethingPickOne @?= Just (TestProtoOneof.SomethingPickOneDummyMsg1
                                    (TestProtoOneof.DummyMsg 66))
    do TestProtoOneof.Something { .. } <- readProto
       somethingValue   @?= 7
       somethingAnother @?= 8
       somethingPickOne @?= Just (TestProtoOneof.SomethingPickOneDummyMsg2
                                    (TestProtoOneof.DummyMsg 67))
    do TestProtoOneof.Something { .. } <- readProto
       somethingValue   @?= 9
       somethingAnother @?= 10
       somethingPickOne @?= Just (TestProtoOneof.SomethingPickOneDummyEnum
                                    (Enumerated (Right TestProtoOneof.DummyEnumDUMMY1)))
    -- Read with oneof not set
    do TestProtoOneof.Something { .. } <- readProto
       somethingValue   @?= 11
       somethingAnother @?= 12
       somethingPickOne @?= Nothing

testCase18 = testCase "Imported Oneof" $ do
  do TestProtoOneof.WithImported{ .. } <- readProto
     withImportedPickOne @?= Just (TestProtoOneof.WithImportedPickOneDummyMsg1
                                     (TestProtoOneof.DummyMsg 0))
  do TestProtoOneof.WithImported{ .. } <- readProto
     withImportedPickOne @?= Just (TestProtoOneof.WithImportedPickOneDummyMsg1
                                     (TestProtoOneof.DummyMsg 68))
  do TestProtoOneof.WithImported{ .. } <- readProto
     withImportedPickOne @?= Just (TestProtoOneof.WithImportedPickOneWithOneof
                                     (TestProtoOneofImport.WithOneof Nothing))
  do TestProtoOneof.WithImported{ .. } <- readProto
     withImportedPickOne @?= Just (TestProtoOneof.WithImportedPickOneWithOneof
                                     (TestProtoOneofImport.WithOneof
                                        (Just (TestProtoOneofImport.WithOneofPickOneA ""))))
  do TestProtoOneof.WithImported{ .. } <- readProto
     withImportedPickOne @?= Just (TestProtoOneof.WithImportedPickOneWithOneof
                                     (TestProtoOneofImport.WithOneof
                                        (Just (TestProtoOneofImport.WithOneofPickOneB 0))))
  do TestProtoOneof.WithImported{ .. } <- readProto
     withImportedPickOne @?= Just (TestProtoOneof.WithImportedPickOneWithOneof
                                     (TestProtoOneofImport.WithOneof
                                        (Just (TestProtoOneofImport.WithOneofPickOneA "foo"))))
  do TestProtoOneof.WithImported{ .. } <- readProto
     withImportedPickOne @?= Just (TestProtoOneof.WithImportedPickOneWithOneof
                                     (TestProtoOneofImport.WithOneof
                                        (Just (TestProtoOneofImport.WithOneofPickOneB 19))))
  do TestProtoOneof.WithImported{ .. } <- readProto
     withImportedPickOne @?= Nothing

allTestsDone = testCase "Receive end of test suite sentinel message" $
   do MultipleFields{..} <- readProto
      multipleFieldsMultiFieldString @?= "All tests complete"
