{-# OPTIONS_GHC -fno-warn-orphans #-}

module ArbitraryGeneratedTestTypes where

import qualified Data.ByteString       as BS
import qualified Data.Text.Lazy        as T
import qualified Data.Vector           as V
import qualified Proto3.Suite.Types as DotProto
import           Test.QuickCheck       (Arbitrary, arbitrary,
                                        arbitraryBoundedEnum, listOf)
import qualified Test.QuickCheck       as QC
import           TestProto
import qualified TestProtoImport
import qualified TestProtoOneof
import qualified TestProtoOneofImport

instance Arbitrary a => Arbitrary (V.Vector a) where
  arbitrary = V.fromList <$> listOf arbitrary

instance Arbitrary Trivial where
  arbitrary = Trivial <$> arbitrary

instance Arbitrary MultipleFields where
  arbitrary =
    MultipleFields
    <$> arbitrary
    <*> arbitrary
    <*> arbitrary
    <*> arbitrary
    <*> fmap T.pack arbitrary
    <*> arbitrary

instance Arbitrary WithEnum_TestEnum where
  arbitrary = arbitraryBoundedEnum

instance Arbitrary WithEnum where
  arbitrary = WithEnum <$> arbitrary

instance Arbitrary WithNesting_Nested where
  arbitrary =
    WithNesting_Nested
    <$> fmap T.pack arbitrary
    <*> arbitrary
    <*> arbitrary
    <*> arbitrary

instance Arbitrary WithNesting where
  arbitrary = WithNesting <$> arbitrary

instance Arbitrary WithRepetition where
  arbitrary = WithRepetition <$> arbitrary

instance Arbitrary WithFixed where
  arbitrary = WithFixed <$> arbitrary <*> arbitrary <*> arbitrary <*> arbitrary

instance Arbitrary WithBytes where
  arbitrary = WithBytes <$> arbitrary <*> arbitrary

instance Arbitrary BS.ByteString where
  arbitrary = BS.pack <$> arbitrary

instance Arbitrary AllPackedTypes where
  arbitrary = do
    AllPackedTypes
      <$> arbitrary <*> arbitrary <*> arbitrary <*> arbitrary <*> arbitrary
      <*> arbitrary <*> arbitrary <*> arbitrary <*> arbitrary <*> arbitrary
      <*> arbitrary <*> arbEnums  <*> arbEnums
    where
      arbEnums = V.fromList <$> listOf (DotProto.Enumerated . Right <$> arbitraryBoundedEnum)

instance Arbitrary SignedInts where
  arbitrary = SignedInts <$> arbitrary <*> arbitrary

instance Arbitrary WithNestingRepeated where
  arbitrary = WithNestingRepeated <$> arbitrary

instance Arbitrary WithNestingRepeated_Nested where
  arbitrary =
    WithNestingRepeated_Nested
    <$> fmap T.pack arbitrary
    <*> arbitrary
    <*> arbitrary
    <*> arbitrary

instance Arbitrary WithNestingRepeatedInts where
  arbitrary = WithNestingRepeatedInts <$> arbitrary

instance Arbitrary NestedInts where
  arbitrary = NestedInts <$> arbitrary <*> arbitrary

instance Arbitrary OutOfOrderFields where
  arbitrary =
    OutOfOrderFields
    <$> arbitrary
    <*> fmap T.pack arbitrary
    <*> arbitrary
    <*> fmap (fmap T.pack) arbitrary

instance Arbitrary UsingImported where
  arbitrary =
    UsingImported
    <$> arbitrary
    <*> arbitrary

instance Arbitrary TestProtoImport.WithNesting where
  arbitrary =
    TestProtoImport.WithNesting
    <$> arbitrary
    <*> arbitrary

instance Arbitrary TestProtoImport.WithNesting_Nested where
  arbitrary =
    TestProtoImport.WithNesting_Nested
    <$> arbitrary
    <*> arbitrary

instance Arbitrary Wrapped where
  arbitrary = Wrapped <$> arbitrary

instance Arbitrary TestProtoOneof.DummyMsg where
  arbitrary =
    TestProtoOneof.DummyMsg
    <$> arbitrary

instance Arbitrary TestProtoOneof.Something where
  arbitrary =
    TestProtoOneof.Something
    <$> arbitrary
    <*> arbitrary
    <*> arbitrary

instance Arbitrary TestProtoOneof.SomethingPickOne where
  arbitrary =
    QC.oneof
      [ TestProtoOneof.SomethingPickOneName       <$> fmap T.pack arbitrary
      , TestProtoOneof.SomethingPickOneSomeid     <$> arbitrary
      , TestProtoOneof.SomethingPickOneDummyMsg1  <$> arbitrary
      , TestProtoOneof.SomethingPickOneDummyMsg2  <$> arbitrary
      , TestProtoOneof.SomethingPickOneDummyEnum . DotProto.Enumerated . Right
        <$> arbitraryBoundedEnum
      ]

instance Arbitrary TestProtoOneof.WithImported where
  arbitrary =
    TestProtoOneof.WithImported
    <$> arbitrary

instance Arbitrary TestProtoOneof.WithImportedPickOne where
  arbitrary =
    QC.oneof
      [ TestProtoOneof.WithImportedPickOneDummyMsg1 <$> arbitrary
      , TestProtoOneof.WithImportedPickOneWithOneof <$> arbitrary
      ]

instance Arbitrary TestProtoOneofImport.WithOneof where
  arbitrary =
    TestProtoOneofImport.WithOneof
    <$> arbitrary

instance Arbitrary TestProtoOneofImport.WithOneofPickOne where
  arbitrary =
    QC.oneof
      [ TestProtoOneofImport.WithOneofPickOneA <$> fmap T.pack arbitrary
      , TestProtoOneofImport.WithOneofPickOneB <$> arbitrary
      ]
