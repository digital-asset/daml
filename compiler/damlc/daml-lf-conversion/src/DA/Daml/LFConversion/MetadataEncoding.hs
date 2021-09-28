-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE PatternSynonyms #-}

-- | Encoding/decoding of metadata (i.e. non-semantically-relevant bindings) in LF,
-- such as functional dependencies and typeclass instance overlap modes. These are
-- added in during LF conversion, and then decoded during data-dependencies to
-- improve the reconstructed module interface.
module DA.Daml.LFConversion.MetadataEncoding
    ( funDepName
    , encodeFunDeps
    , decodeFunDeps
    , mapFunDep
    , mapFunDepM
    , minimalName
    , encodeLBooleanFormula
    , decodeLBooleanFormula
    , encodeBooleanFormula
    , decodeBooleanFormula
    , overlapModeName
    , encodeOverlapMode
    , decodeOverlapMode
    , mkMetadataStub
    , moduleImportsName
    , encodeModuleImports
    , decodeModuleImports
    ) where

import Safe (readMay)
import Control.Monad (guard, liftM2)
import Data.List (sortOn)
import qualified Data.Set as S
import qualified Data.Text as T

import qualified "ghc-lib-parser" BasicTypes as GHC
import qualified "ghc-lib-parser" Class as GHC
import qualified "ghc-lib-parser" SrcLoc as GHC
import qualified "ghc-lib-parser" BooleanFormula as BF

import qualified DA.Daml.LF.Ast as LF

-----------------------------
-- FUNCTIONAL DEPENDENCIES --
-----------------------------

funDepName :: LF.TypeSynName -> LF.ExprValName
funDepName (LF.TypeSynName xs) = LF.ExprValName ("$$fd" <> T.concat xs)

-- | Encode a list of functional dependencies as an LF type.
encodeFunDeps :: [GHC.FunDep LF.TypeVarName] -> LF.Type
encodeFunDeps = encodeTypeList $ \(xs, ys) ->
    encodeTypeList LF.TVar xs LF.:->
    encodeTypeList LF.TVar ys

-- | Encode a list as an LF type. Given @'map' f xs == [y1, y2, ..., yn]@
-- then @'encodeTypeList' f xs == { _1: y1, _2: y2, ..., _n: yn }@.
encodeTypeList :: (t -> LF.Type) -> [t] -> LF.Type
encodeTypeList _ [] = LF.TUnit
encodeTypeList f xs =
    LF.TStruct $ zipWith
        (\i x -> (LF.FieldName (T.pack ('_' : show @Int i)), f x))
        [1..] xs

decodeFunDeps :: LF.Type -> Maybe [GHC.FunDep LF.TypeVarName]
decodeFunDeps = decodeTypeList decodeFunDep

decodeFunDep :: LF.Type -> Maybe (GHC.FunDep LF.TypeVarName)
decodeFunDep ty = do
    (left LF.:-> right) <- pure ty
    left' <- decodeTypeList decodeTypeVar left
    right' <- decodeTypeList decodeTypeVar right
    pure (left', right')

decodeTypeVar :: LF.Type -> Maybe LF.TypeVarName
decodeTypeVar = \case
    LF.TVar x -> Just x
    _ -> Nothing

decodeTypeList :: (LF.Type -> Maybe t) -> LF.Type -> Maybe [t]
decodeTypeList _ LF.TUnit = Just []
decodeTypeList f ty = do
    LF.TStruct fields <- pure ty
    pairs <- sortOn fst <$> mapM (decodeTypeListField f) fields
    guard (map fst pairs == [1 .. length pairs])
    pure (map snd pairs)

decodeTypeListField :: (LF.Type -> Maybe t) -> (LF.FieldName, LF.Type) -> Maybe (Int, t)
decodeTypeListField f (LF.FieldName fieldName, x) = do
    suffix <- T.stripPrefix "_" fieldName
    i <- readMay (T.unpack suffix)
    y <- f x
    pure (i, y)

mapFunDep :: (a -> b) -> (GHC.FunDep a -> GHC.FunDep b)
mapFunDep f (a, b) = (map f a, map f b)

mapFunDepM :: Monad m => (a -> m b) -> (GHC.FunDep a -> m (GHC.FunDep b))
mapFunDepM f (a, b) = liftM2 (,) (mapM f a) (mapM f b)

---------------------
-- MINIMAL PRAGMAS --
---------------------

minimalName :: LF.TypeSynName -> LF.ExprValName
minimalName (LF.TypeSynName xs) = LF.ExprValName ("$$minimal" <> T.concat xs)

pattern TEncodedStr :: T.Text -> LF.Type
pattern TEncodedStr x = LF.TStruct [(LF.FieldName x, LF.TUnit)]

decodeText :: LF.Type -> Maybe T.Text
decodeText (TEncodedStr x) = Just x
decodeText _ = Nothing

pattern TEncodedCon :: T.Text -> LF.Type -> LF.Type
pattern TEncodedCon a b = LF.TStruct [(LF.FieldName a, b)]

encodeLBooleanFormula :: BF.LBooleanFormula T.Text -> LF.Type
encodeLBooleanFormula = encodeBooleanFormula . GHC.unLoc

decodeLBooleanFormula :: LF.Type -> Maybe (BF.LBooleanFormula T.Text)
decodeLBooleanFormula = fmap GHC.noLoc . decodeBooleanFormula

encodeBooleanFormula :: BF.BooleanFormula T.Text -> LF.Type
encodeBooleanFormula = \case
    BF.Var x -> TEncodedCon "Var" (TEncodedStr x)
    BF.And xs -> TEncodedCon "And" (encodeTypeList encodeLBooleanFormula xs)
    BF.Or xs -> TEncodedCon "Or" (encodeTypeList encodeLBooleanFormula xs)
    BF.Parens x -> TEncodedCon "Parens" (encodeLBooleanFormula x)

decodeBooleanFormula :: LF.Type -> Maybe (BF.BooleanFormula T.Text)
decodeBooleanFormula = \case
    TEncodedCon "Var" (TEncodedStr x) -> Just (BF.Var x)
    TEncodedCon "And" xs -> BF.And <$> decodeTypeList decodeLBooleanFormula xs
    TEncodedCon "Or" xs -> BF.Or <$> decodeTypeList decodeLBooleanFormula xs
    TEncodedCon "Parens" x -> BF.Parens <$> decodeLBooleanFormula x
    _ -> Nothing

-------------------
-- OVERLAP MODES --
-------------------

overlapModeName :: LF.ExprValName -> LF.ExprValName
overlapModeName (LF.ExprValName x) = LF.ExprValName ("$$om" <> x)

encodeOverlapMode :: GHC.OverlapMode -> Maybe LF.Type
encodeOverlapMode = \case
    GHC.NoOverlap _ -> Nothing
    GHC.Overlappable _ -> Just (TEncodedStr "OVERLAPPABLE")
    GHC.Overlapping _ -> Just (TEncodedStr "OVERLAPPING")
    GHC.Overlaps _ -> Just (TEncodedStr "OVERLAPS")
    GHC.Incoherent _ -> Just (TEncodedStr "INCOHERENT")

decodeOverlapMode :: LF.Type -> Maybe GHC.OverlapMode
decodeOverlapMode = \case
    TEncodedStr mode -> lookup mode
        [ ("OVERLAPPING", GHC.Overlapping GHC.NoSourceText)
        , ("OVERLAPPABLE", GHC.Overlappable GHC.NoSourceText)
        , ("OVERLAPS", GHC.Overlaps GHC.NoSourceText)
        , ("INCOHERENT", GHC.Incoherent GHC.NoSourceText)
        ]
    _ -> Nothing

--------------------------
-- INSTANCE PROPAGATION --
--------------------------
moduleImportsName :: LF.ExprValName
moduleImportsName = LF.ExprValName "$$imports"

encodeModuleImports :: S.Set (LF.Qualified ()) -> LF.Type
encodeModuleImports = encodeTypeList encodeModuleImport . S.toList

encodeModuleImport :: LF.Qualified () -> LF.Type
encodeModuleImport q =
    encodeTypeList id
        [ encodePackageRef (LF.qualPackage q)
        , encodeModuleName (LF.qualModule q)
        ]

encodePackageRef :: LF.PackageRef -> LF.Type
encodePackageRef = \case
  LF.PRSelf -> LF.TUnit
  LF.PRImport (LF.PackageId packageId) -> TEncodedStr packageId

encodeModuleName :: LF.ModuleName -> LF.Type
encodeModuleName (LF.ModuleName components) =
    encodeTypeList TEncodedStr components

decodeModuleImports :: LF.Type -> Maybe (S.Set (LF.Qualified ()))
decodeModuleImports = fmap S.fromList . decodeTypeList decodeModuleImport

decodeModuleImport :: LF.Type -> Maybe (LF.Qualified ())
decodeModuleImport x = do
    [p, m] <- decodeTypeList Just x
    packageRef <- decodePackageRef p
    moduleName <- decodeModuleName m
    pure (LF.Qualified packageRef moduleName ())

decodePackageRef :: LF.Type -> Maybe LF.PackageRef
decodePackageRef = \case
    LF.TUnit -> pure LF.PRSelf
    TEncodedStr packageId -> pure (LF.PRImport (LF.PackageId packageId))
    _ -> Nothing

decodeModuleName :: LF.Type -> Maybe LF.ModuleName
decodeModuleName = fmap LF.ModuleName . decodeTypeList decodeText

---------------------
-- STUB GENERATION --
---------------------

mkMetadataStub :: LF.ExprValName -> LF.Type -> LF.DefValue
mkMetadataStub n t = LF.DefValue
    { dvalLocation = Nothing
    , dvalBinder = (n,t)
    , dvalBody = LF.EBuiltin LF.BEError `LF.ETyApp` t
        `LF.ETmApp` LF.EBuiltin (LF.BEText "undefined")
    , dvalNoPartyLiterals = LF.HasNoPartyLiterals True
    , dvalIsTest = LF.IsTest False
    }
