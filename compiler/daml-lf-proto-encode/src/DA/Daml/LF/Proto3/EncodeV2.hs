-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}
-- | Encoding of the LF package into LF version 1 format.
module DA.Daml.LF.Proto3.EncodeV2
  ( encodeScenarioModule
  , encodePackage
  ) where

import           Control.Lens ((^.), matching)
import           Control.Lens.Ast (rightSpine)
import           Control.Monad.State.Strict

import           Data.Coerce
import           Data.Functor.Identity
import qualified Data.HashMap.Strict as HMS
import qualified Data.List as L
import qualified Data.Set as S
import qualified Data.Map.Strict as Map
import           Data.Maybe (fromMaybe)
import qualified Data.NameMap as NM
import qualified Data.Text           as T
import qualified Data.Text.Lazy      as TL
import qualified Data.Vector         as V
import           Data.Int

import           DA.Pretty
import           DA.Daml.LF.Ast
import           DA.Daml.LF.Mangling
import qualified DA.Daml.LF.Proto3.Util as Util
import qualified Com.Daml.DamlLfDev.DamlLf2 as P

import qualified Proto3.Suite as P (Enumerated (..))

-- NOTE(MH): Type synonym for a `Maybe` that is always known to be a `Just`.
-- Some functions always return `Just x` instead of `x` since they would
-- otherwise always be wrapped in `Just` at their call sites.
type Just a = Maybe a

type Encode a = State EncodeEnv a

data EncodeEnv = EncodeEnv
    { version :: !Version
    , internedStrings :: !(HMS.HashMap T.Text Int32)
    , nextInternedStringId :: !Int32
      -- ^ We track the size of `internedStrings` explicitly since `HMS.size` is `O(n)`.
    , internedDottedNames :: !(HMS.HashMap [Int32] Int32)
    , nextInternedDottedNameId :: !Int32
      -- ^ We track the size of `internedDottedNames` explicitly since `HMS.size` is `O(n)`.
    , internedTypes :: !(Map.Map P.TypeSum Int32)
    , nextInternedTypeId :: !Int32
    }

initEncodeEnv :: Version -> EncodeEnv
initEncodeEnv version =
    EncodeEnv
    { nextInternedStringId = 0
    , internedStrings = HMS.empty
    , internedDottedNames = HMS.empty
    , nextInternedDottedNameId = 0
    , internedTypes = Map.empty
    , nextInternedTypeId = 0
    , ..
    }

-- | Find or allocate a string in the interning table. Return the index of
-- the string in the resulting interning table.
allocString :: T.Text -> Encode Int32
allocString t = do
    env@EncodeEnv{internedStrings, nextInternedStringId = n} <- get
    case t `HMS.lookup` internedStrings of
        Just n -> pure n
        Nothing -> do
            when (n == maxBound) $
                error "String interning table grew too large"
            put $! env
                { internedStrings = HMS.insert t n internedStrings
                , nextInternedStringId = n + 1
                }
            pure n

allocDottedName :: [Int32] -> Encode Int32
allocDottedName ids = do
    env@EncodeEnv{internedDottedNames, nextInternedDottedNameId = n} <- get
    case ids `HMS.lookup` internedDottedNames of
        Just n -> pure n
        Nothing -> do
            when (n == maxBound) $
                error "Dotted name interning table grew too large"
            put $! env
                { internedDottedNames = HMS.insert ids n internedDottedNames
                , nextInternedDottedNameId = n + 1
                }
            pure n

------------------------------------------------------------------------
-- Encodings of things related to string interning
------------------------------------------------------------------------

-- | Encode of a string that cannot be interned, e.g, the entries of the
-- interning table itself.
encodeString :: T.Text -> TL.Text
encodeString = TL.fromStrict

-- | Encode a string that will be interned
encodeInternableString :: T.Text -> Encode Int32
encodeInternableString = coerce (encodeInternableStrings @Identity)

-- | Encode a string that will be interned
encodeInternableStrings :: Traversable t => t T.Text -> Encode (t Int32)
encodeInternableStrings strs = mapM allocString strs

-- TODO(https://github.com/digital-asset/daml/issues/18240): change the proto
--  to only use interned names. Then we don't need this EitherLike variant of
--  encodeName anymore.
-- | Encode the name of a syntactic object, e.g., a variable or a data
-- constructor. These strings are mangled to escape special characters. All
-- names are interned.
encodeName
    :: Util.EitherLike TL.Text Int32 e
    => (a -> T.Text) -> a -> Encode (Just e)
encodeName unwrapName obj =
    Just
        . Util.fromEither @TL.Text @Int32
        . Right
        <$> encodeNameId unwrapName obj


encodeNameId :: (a -> T.Text) -> a -> Encode Int32
encodeNameId unwrapName (unwrapName -> unmangled) =
    coerce (encodeNames @Identity) unmangled

encodeNames :: Traversable t => t T.Text -> Encode (t Int32)
encodeNames = encodeInternableStrings . fmap mangleName
    where
        mangleName :: T.Text -> T.Text
        mangleName unmangled = case mangleIdentifier unmangled of
           Left err -> error $ "IMPOSSIBLE: could not mangle name " ++ show unmangled ++ ": " ++ err
           Right mangled -> mangled


-- TODO(https://github.com/digital-asset/daml/issues/18240): change the proto
--  to only use interned names. Then we don't need this EitherLike variant of
--  encodeDottedName anymore.
-- | Encode the multi-component name of a syntactic object, e.g., a type
-- constructor. All compononents are mangled. Dotted names are interned.
encodeDottedName :: Util.EitherLike P.DottedName Int32 e
                 => (a -> [T.Text]) -> a -> Encode (Just e)
encodeDottedName unwrapDottedName (unwrapDottedName -> unmangled) =
    Just .
      Util.fromEither @P.DottedName @Int32 .
      Right <$>
      encodeDottedName' unmangled

encodeDottedName' :: [T.Text] -> Encode Int32
encodeDottedName' unmangled = do
    ids <- encodeNames unmangled
    allocDottedName ids

encodeDottedNameId :: (a -> [T.Text]) -> a -> Encode Int32
encodeDottedNameId unwrapDottedName (unwrapDottedName -> unmangled) =
    encodeDottedName' unmangled

-- TODO(https://github.com/digital-asset/daml/issues/18240): change the proto
--  to only store an interned name ID. Then we can simply return an Int32.
-- | Encode the name of a top-level value. The name is mangled and interned.
--
-- For now, value names are always encoded using a single segment.
-- This is to keep backwards compat with older .dalf files, but also
-- because currently GenDALF generates weird names like `.` that we'd
-- have to handle separatedly. So for now, considering that we do not
-- use values in codegen, just mangle the entire thing.
encodeValueName :: ExprValName -> Encode (V.Vector TL.Text, Int32)
encodeValueName valName = do
    id <- encodeDottedName' [unExprValName valName]
    pure (V.empty, id)

-- | Encode a reference to a package. Package names are not mangled. Package
-- names are interned.
encodePackageRef :: PackageRef -> Encode (Just P.PackageRef)
encodePackageRef = fmap (Just . P.PackageRef . Just) . \case
    PRSelf -> pure $ P.PackageRefSumSelf P.Unit
    PRImport (PackageId pkgId) -> do
        P.PackageRefSumPackageIdInternedStr <$> allocString pkgId

-- | Interface method names are always interned, since interfaces were
-- introduced after name interning.
encodeMethodName :: MethodName -> Encode Int32
encodeMethodName = encodeNameId unMethodName

------------------------------------------------------------------------
-- Simple encodings
------------------------------------------------------------------------

encodeList :: (a -> Encode b) -> [a] -> Encode (V.Vector b)
encodeList encodeElem = fmap V.fromList . mapM encodeElem

encodeNameMap :: NM.Named a => (a -> Encode b) -> NM.NameMap a -> Encode (V.Vector b)
encodeNameMap encodeElem = fmap V.fromList . mapM encodeElem . NM.toList

encodeSet :: (a -> Encode b) -> S.Set a -> Encode (V.Vector b)
encodeSet encodeElem = fmap V.fromList . mapM encodeElem . S.toList

encodeQualTypeSynName' :: Qualified TypeSynName -> Encode P.TypeSynName
encodeQualTypeSynName' (Qualified pref mname syn) = do
    typeSynNameModule <- encodeModuleRef pref mname
    typeSynNameName <- encodeDottedName unTypeSynName syn
    pure $ P.TypeSynName{..}

encodeQualTypeSynName :: Qualified TypeSynName -> Encode (Just P.TypeSynName)
encodeQualTypeSynName tysyn = Just <$> encodeQualTypeSynName' tysyn

encodeQualTypeConName' :: Qualified TypeConName -> Encode P.TypeConName
encodeQualTypeConName' (Qualified pref mname con) = do
    typeConNameModule <- encodeModuleRef pref mname
    typeConNameName <- encodeDottedName unTypeConName con
    pure $ P.TypeConName{..}

encodeQualTypeConName :: Qualified TypeConName -> Encode (Just P.TypeConName)
encodeQualTypeConName tycon = Just <$> encodeQualTypeConName' tycon


encodeSourceLoc :: SourceLoc -> Encode P.Location
encodeSourceLoc SourceLoc{..} = do
    locationModule <- case slocModuleRef of
        Nothing -> pure Nothing
        Just (pkgRef, modName) -> encodeModuleRef pkgRef modName
    let locationRange = Just $ P.Location_Range
            (fromIntegral slocStartLine)
            (fromIntegral slocStartCol)
            (fromIntegral slocEndLine)
            (fromIntegral slocEndCol)
    pure P.Location{..}

encodeModuleRef :: PackageRef -> ModuleName -> Encode (Just P.ModuleRef)
encodeModuleRef pkgRef modName = do
    moduleRefPackageRef <- encodePackageRef pkgRef
    moduleRefModuleName <- encodeDottedName unModuleName modName
    pure $ Just P.ModuleRef{..}

encodeFieldsWithTypes :: (a -> T.Text) -> [(a, Type)] -> Encode (V.Vector P.FieldWithType)
encodeFieldsWithTypes unwrapName =
    encodeList $ \(name, typ) -> P.FieldWithType <$> encodeName unwrapName name <*> encodeType typ

encodeFieldsWithExprs :: (a -> T.Text) -> [(a, Expr)] -> Encode (V.Vector P.FieldWithExpr)
encodeFieldsWithExprs unwrapName =
    encodeList $ \(name, expr) -> P.FieldWithExpr <$> encodeName unwrapName name <*> encodeExpr expr

encodeTypeVarsWithKinds :: [(TypeVarName, Kind)] -> Encode (V.Vector P.TypeVarWithKind)
encodeTypeVarsWithKinds =
    encodeList $ \(name, kind) -> P.TypeVarWithKind <$> encodeName unTypeVarName name <*> (Just <$> encodeKind kind)

encodeExprVarWithType :: (ExprVarName, Type) -> Encode P.VarWithType
encodeExprVarWithType (name, typ) = do
    varWithTypeVar <- encodeName unExprVarName name
    varWithTypeType <- encodeType typ
    pure P.VarWithType{..}

------------------------------------------------------------------------
-- Encoding of kinds
------------------------------------------------------------------------

encodeKind :: Kind -> Encode P.Kind
encodeKind = fmap (P.Kind . Just) . \case
    KStar -> pure $ P.KindSumStar P.Unit
    KNat -> pure $ P.KindSumNat P.Unit
    k@KArrow{} -> do
        let (params, result) = k ^. rightSpine _KArrow
        kind_ArrowParams <- encodeList encodeKind params
        kind_ArrowResult <- Just <$> encodeKind result
        pure $ P.KindSumArrow P.Kind_Arrow{..}

------------------------------------------------------------------------
-- Encoding of types
------------------------------------------------------------------------

encodeBuiltinType :: BuiltinType -> P.Enumerated P.PrimType
encodeBuiltinType = P.Enumerated . Right . \case
    BTInt64 -> P.PrimTypeINT64
    BTText -> P.PrimTypeTEXT
    BTTimestamp -> P.PrimTypeTIMESTAMP
    BTParty -> P.PrimTypePARTY
    BTUnit -> P.PrimTypeUNIT
    BTBool -> P.PrimTypeBOOL
    BTList -> P.PrimTypeLIST
    BTUpdate -> P.PrimTypeUPDATE
    BTScenario -> P.PrimTypeSCENARIO
    BTDate -> P.PrimTypeDATE
    BTContractId -> P.PrimTypeCONTRACT_ID
    BTOptional -> P.PrimTypeOPTIONAL
    BTTextMap -> P.PrimTypeTEXTMAP
    BTGenMap -> P.PrimTypeGENMAP
    BTArrow -> P.PrimTypeARROW
    BTNumeric -> P.PrimTypeNUMERIC
    BTAny -> P.PrimTypeANY
    BTTypeRep -> P.PrimTypeTYPE_REP
    BTRoundingMode -> P.PrimTypeROUNDING_MODE
    BTBigNumeric -> P.PrimTypeBIGNUMERIC
    BTAnyException -> P.PrimTypeANY_EXCEPTION

encodeType' :: Type -> Encode P.Type
encodeType' typ = do
  ptyp <- case typ ^. _TApps of
    (TVar var, args) -> do
        type_VarVar <- encodeName unTypeVarName var
        type_VarArgs <- encodeList encodeType' args
        pure $ P.TypeSumVar P.Type_Var{..}
    (TCon con, args) -> do
        type_ConTycon <- encodeQualTypeConName con
        type_ConArgs <- encodeList encodeType' args
        pure $ P.TypeSumCon P.Type_Con{..}
    (TSynApp syn args, []) -> do
        type_SynTysyn <- encodeQualTypeSynName syn
        type_SynArgs <- encodeList encodeType' args
        pure $ P.TypeSumSyn P.Type_Syn{..}
    (TBuiltin bltn, args) -> do
        let type_PrimPrim = encodeBuiltinType bltn
        type_PrimArgs <- encodeList encodeType' args
        pure $ P.TypeSumPrim P.Type_Prim{..}
    (t@TForall{}, []) -> do
        let (binders, body) = t ^. _TForalls
        type_ForallVars <- encodeTypeVarsWithKinds binders
        type_ForallBody <- encodeType body
        pure $ P.TypeSumForall P.Type_Forall{..}
    (TStruct flds, []) -> do
        type_StructFields <- encodeFieldsWithTypes unFieldName flds
        pure $ P.TypeSumStruct P.Type_Struct{..}

    (TNat n, _) ->
        pure $ P.TypeSumNat (fromTypeLevelNat n)

    (TApp{}, _) -> error "TApp after unwinding TApp"
    -- NOTE(MH): The following case is ill-kinded.
    (TStruct{}, _:_) -> error "Application of TStruct"
    -- NOTE(MH): The following case requires impredicative polymorphism,
    -- which we don't support.
    (TForall{}, _:_) -> error "Application of TForall"
    (TSynApp{}, _:_) -> error "Application of TSynApp"
  allocType ptyp

encodeType :: Type -> Encode (Just P.Type)
encodeType t = Just <$> encodeType' t

allocType :: P.TypeSum -> Encode P.Type
allocType ptyp = fmap (P.Type . Just) $ do
    env@EncodeEnv{internedTypes, nextInternedTypeId = n} <- get
    case ptyp `Map.lookup` internedTypes of
        Just n -> pure (P.TypeSumInterned n)
        Nothing -> do
            when (n == maxBound) $
                error "Type interning table grew too large"
            put $! env
                { internedTypes = Map.insert ptyp n internedTypes
                , nextInternedTypeId = n + 1
                }
            pure (P.TypeSumInterned n)

------------------------------------------------------------------------
-- Encoding of expressions
------------------------------------------------------------------------

encodeTypeConApp :: TypeConApp -> Encode (Just P.Type_Con)
encodeTypeConApp (TypeConApp tycon args) = do
    type_ConTycon <- encodeQualTypeConName tycon
    type_ConArgs <- encodeList encodeType' args
    pure $ Just P.Type_Con{..}

encodeBuiltinExpr :: BuiltinExpr -> Encode P.ExprSum
encodeBuiltinExpr = \case
    BEInt64 x -> pureLit $ P.PrimLitSumInt64 x
    BENumeric num ->
        lit . P.PrimLitSumNumericInternedStr <$> allocString (T.pack (show num))
    BEText x ->
        lit . P.PrimLitSumTextInternedStr <$> encodeInternableString x
    BETimestamp x -> pureLit $ P.PrimLitSumTimestamp x
    BEDate x -> pureLit $ P.PrimLitSumDate x

    BEUnit -> pure $ P.ExprSumPrimCon $ P.Enumerated $ Right P.PrimConCON_UNIT
    BEBool b -> pure $ P.ExprSumPrimCon $ P.Enumerated $ Right $ case b of
        False -> P.PrimConCON_FALSE
        True -> P.PrimConCON_TRUE

    BERoundingMode r -> case r of
      LitRoundingUp -> pureLit $ P.PrimLitSumRoundingMode $ P.Enumerated $ Right P.PrimLit_RoundingModeUP
      LitRoundingDown -> pureLit $ P.PrimLitSumRoundingMode $ P.Enumerated $ Right P.PrimLit_RoundingModeDOWN
      LitRoundingCeiling -> pureLit $ P.PrimLitSumRoundingMode $ P.Enumerated $ Right P.PrimLit_RoundingModeCEILING
      LitRoundingFloor -> pureLit $ P.PrimLitSumRoundingMode $ P.Enumerated $ Right P.PrimLit_RoundingModeFLOOR
      LitRoundingHalfUp -> pureLit $ P.PrimLitSumRoundingMode $ P.Enumerated $ Right P.PrimLit_RoundingModeHALF_UP
      LitRoundingHalfDown -> pureLit $ P.PrimLitSumRoundingMode $ P.Enumerated $ Right P.PrimLit_RoundingModeHALF_DOWN
      LitRoundingHalfEven -> pureLit $ P.PrimLitSumRoundingMode $ P.Enumerated $ Right P.PrimLit_RoundingModeHALF_EVEN
      LitRoundingUnnecessary -> pureLit $ P.PrimLitSumRoundingMode $ P.Enumerated $ Right P.PrimLit_RoundingModeUNNECESSARY

    BEEqualGeneric -> builtin P.BuiltinFunctionEQUAL
    BELessGeneric -> builtin P.BuiltinFunctionLESS
    BELessEqGeneric -> builtin P.BuiltinFunctionLESS_EQ
    BEGreaterGeneric -> builtin P.BuiltinFunctionGREATER
    BEGreaterEqGeneric -> builtin P.BuiltinFunctionGREATER_EQ

    BEEqual typ -> case typ of
      BTInt64 -> builtin P.BuiltinFunctionEQUAL_INT64
      BTText -> builtin P.BuiltinFunctionEQUAL_TEXT
      BTTimestamp -> builtin P.BuiltinFunctionEQUAL_TIMESTAMP
      BTDate -> builtin P.BuiltinFunctionEQUAL_DATE
      BTParty -> builtin P.BuiltinFunctionEQUAL_PARTY
      BTBool -> builtin P.BuiltinFunctionEQUAL_BOOL
      BTTypeRep -> builtin P.BuiltinFunctionEQUAL_TYPE_REP
      other -> error $ "BEEqual unexpected type " <> show other

    BELessEq typ -> case typ of
      BTInt64 -> builtin P.BuiltinFunctionLEQ_INT64
      BTText -> builtin P.BuiltinFunctionLEQ_TEXT
      BTTimestamp -> builtin P.BuiltinFunctionLEQ_TIMESTAMP
      BTDate -> builtin P.BuiltinFunctionLEQ_DATE
      BTParty -> builtin P.BuiltinFunctionLEQ_PARTY
      other -> error $ "BELessEq unexpected type " <> show other

    BELess typ -> case typ of
      BTInt64 -> builtin P.BuiltinFunctionLESS_INT64
      BTText -> builtin P.BuiltinFunctionLESS_TEXT
      BTTimestamp -> builtin P.BuiltinFunctionLESS_TIMESTAMP
      BTDate -> builtin P.BuiltinFunctionLESS_DATE
      BTParty -> builtin P.BuiltinFunctionLESS_PARTY
      other -> error $ "BELess unexpected type " <> show other

    BEGreaterEq typ -> case typ of
      BTInt64 -> builtin P.BuiltinFunctionGEQ_INT64
      BTText -> builtin P.BuiltinFunctionGEQ_TEXT
      BTTimestamp -> builtin P.BuiltinFunctionGEQ_TIMESTAMP
      BTDate -> builtin P.BuiltinFunctionGEQ_DATE
      BTParty -> builtin P.BuiltinFunctionGEQ_PARTY
      other -> error $ "BEGreaterEq unexpected type " <> show other

    BEGreater typ -> case typ of
      BTInt64 -> builtin P.BuiltinFunctionGREATER_INT64
      BTText -> builtin P.BuiltinFunctionGREATER_TEXT
      BTTimestamp -> builtin P.BuiltinFunctionGREATER_TIMESTAMP
      BTDate -> builtin P.BuiltinFunctionGREATER_DATE
      BTParty -> builtin P.BuiltinFunctionGREATER_PARTY
      other -> error $ "BEGreater unexpected type " <> show other

    BEEqualNumeric -> builtin P.BuiltinFunctionEQUAL_NUMERIC
    BELessNumeric -> builtin P.BuiltinFunctionLESS_NUMERIC
    BELessEqNumeric -> builtin P.BuiltinFunctionLEQ_NUMERIC
    BEGreaterNumeric -> builtin P.BuiltinFunctionGREATER_NUMERIC
    BEGreaterEqNumeric -> builtin P.BuiltinFunctionGEQ_NUMERIC

    BEToText typ -> case typ of
      BTInt64 -> builtin P.BuiltinFunctionINT64_TO_TEXT
      BTText -> builtin P.BuiltinFunctionTEXT_TO_TEXT
      BTTimestamp -> builtin P.BuiltinFunctionTIMESTAMP_TO_TEXT
      BTDate -> builtin P.BuiltinFunctionDATE_TO_TEXT
      BTParty -> builtin P.BuiltinFunctionPARTY_TO_TEXT
      BTBigNumeric ->  builtin P.BuiltinFunctionBIGNUMERIC_TO_TEXT
      other -> error $ "BEToText unexpected type " <> show other
    BEContractIdToText -> builtin P.BuiltinFunctionCONTRACT_ID_TO_TEXT
    BENumericToText -> builtin P.BuiltinFunctionNUMERIC_TO_TEXT
    BECodePointsToText -> builtin P.BuiltinFunctionCODE_POINTS_TO_TEXT
    BETextToParty -> builtin P.BuiltinFunctionTEXT_TO_PARTY
    BETextToInt64 -> builtin P.BuiltinFunctionTEXT_TO_INT64
    BETextToNumericLegacy -> builtin P.BuiltinFunctionTEXT_TO_NUMERIC_LEGACY
    BETextToNumeric -> builtin P.BuiltinFunctionTEXT_TO_NUMERIC
    BETextToCodePoints -> builtin P.BuiltinFunctionTEXT_TO_CODE_POINTS
    BEPartyToQuotedText -> builtin P.BuiltinFunctionPARTY_TO_QUOTED_TEXT

    BEAddNumeric -> builtin P.BuiltinFunctionADD_NUMERIC
    BESubNumeric -> builtin P.BuiltinFunctionSUB_NUMERIC
    BEMulNumericLegacy -> builtin P.BuiltinFunctionMUL_NUMERIC_LEGACY
    BEMulNumeric -> builtin P.BuiltinFunctionMUL_NUMERIC
    BEDivNumericLegacy -> builtin P.BuiltinFunctionDIV_NUMERIC_LEGACY
    BEDivNumeric -> builtin P.BuiltinFunctionDIV_NUMERIC
    BERoundNumeric -> builtin P.BuiltinFunctionROUND_NUMERIC
    BECastNumericLegacy -> builtin P.BuiltinFunctionCAST_NUMERIC_LEGACY
    BECastNumeric -> builtin P.BuiltinFunctionCAST_NUMERIC
    BEShiftNumericLegacy -> builtin P.BuiltinFunctionSHIFT_NUMERIC_LEGACY
    BEShiftNumeric -> builtin P.BuiltinFunctionSHIFT_NUMERIC

    BEScaleBigNumeric -> builtin P.BuiltinFunctionSCALE_BIGNUMERIC
    BEPrecisionBigNumeric -> builtin P.BuiltinFunctionPRECISION_BIGNUMERIC
    BEAddBigNumeric -> builtin P.BuiltinFunctionADD_BIGNUMERIC
    BESubBigNumeric -> builtin P.BuiltinFunctionSUB_BIGNUMERIC
    BEMulBigNumeric -> builtin P.BuiltinFunctionMUL_BIGNUMERIC
    BEDivBigNumeric -> builtin P.BuiltinFunctionDIV_BIGNUMERIC
    BEShiftRightBigNumeric -> builtin P.BuiltinFunctionSHIFT_RIGHT_BIGNUMERIC
    BEBigNumericToNumericLegacy -> builtin P.BuiltinFunctionBIGNUMERIC_TO_NUMERIC_LEGACY
    BEBigNumericToNumeric -> builtin P.BuiltinFunctionBIGNUMERIC_TO_NUMERIC
    BENumericToBigNumeric -> builtin P.BuiltinFunctionNUMERIC_TO_BIGNUMERIC

    BEAddInt64 -> builtin P.BuiltinFunctionADD_INT64
    BESubInt64 -> builtin P.BuiltinFunctionSUB_INT64
    BEMulInt64 -> builtin P.BuiltinFunctionMUL_INT64
    BEDivInt64 -> builtin P.BuiltinFunctionDIV_INT64
    BEModInt64 -> builtin P.BuiltinFunctionMOD_INT64
    BEExpInt64 -> builtin P.BuiltinFunctionEXP_INT64

    BEInt64ToNumericLegacy -> builtin P.BuiltinFunctionINT64_TO_NUMERIC_LEGACY
    BEInt64ToNumeric -> builtin P.BuiltinFunctionINT64_TO_NUMERIC
    BENumericToInt64 -> builtin P.BuiltinFunctionNUMERIC_TO_INT64

    BEFoldl -> builtin P.BuiltinFunctionFOLDL
    BEFoldr -> builtin P.BuiltinFunctionFOLDR
    BEEqualList -> builtin P.BuiltinFunctionEQUAL_LIST
    BEExplodeText -> builtin P.BuiltinFunctionEXPLODE_TEXT
    BEAppendText -> builtin P.BuiltinFunctionAPPEND_TEXT
    BEImplodeText -> builtin P.BuiltinFunctionIMPLODE_TEXT
    BESha256Text -> builtin P.BuiltinFunctionSHA256_TEXT

    BEError -> builtin P.BuiltinFunctionERROR
    BEAnyExceptionMessage -> builtin P.BuiltinFunctionANY_EXCEPTION_MESSAGE

    BETextMapEmpty -> builtin P.BuiltinFunctionTEXTMAP_EMPTY
    BETextMapInsert -> builtin P.BuiltinFunctionTEXTMAP_INSERT
    BETextMapLookup -> builtin P.BuiltinFunctionTEXTMAP_LOOKUP
    BETextMapDelete -> builtin P.BuiltinFunctionTEXTMAP_DELETE
    BETextMapSize -> builtin P.BuiltinFunctionTEXTMAP_SIZE
    BETextMapToList -> builtin P.BuiltinFunctionTEXTMAP_TO_LIST

    BEGenMapEmpty -> builtin P.BuiltinFunctionGENMAP_EMPTY
    BEGenMapInsert -> builtin P.BuiltinFunctionGENMAP_INSERT
    BEGenMapLookup -> builtin P.BuiltinFunctionGENMAP_LOOKUP
    BEGenMapDelete -> builtin P.BuiltinFunctionGENMAP_DELETE
    BEGenMapSize -> builtin P.BuiltinFunctionGENMAP_SIZE
    BEGenMapKeys -> builtin P.BuiltinFunctionGENMAP_KEYS
    BEGenMapValues -> builtin P.BuiltinFunctionGENMAP_VALUES

    BETimestampToUnixMicroseconds -> builtin P.BuiltinFunctionTIMESTAMP_TO_UNIX_MICROSECONDS
    BEUnixMicrosecondsToTimestamp -> builtin P.BuiltinFunctionUNIX_MICROSECONDS_TO_TIMESTAMP

    BEDateToUnixDays -> builtin P.BuiltinFunctionDATE_TO_UNIX_DAYS
    BEUnixDaysToDate -> builtin P.BuiltinFunctionUNIX_DAYS_TO_DATE

    BETrace -> builtin P.BuiltinFunctionTRACE
    BEEqualContractId -> builtin P.BuiltinFunctionEQUAL_CONTRACT_ID
    BECoerceContractId -> builtin P.BuiltinFunctionCOERCE_CONTRACT_ID

    BETypeRepTyConName -> builtin P.BuiltinFunctionTYPE_REP_TYCON_NAME

    where
      builtin = pure . P.ExprSumBuiltin . P.Enumerated . Right
      lit = P.ExprSumPrimLit . P.PrimLit . Just
      pureLit = pure . lit

encodeExpr' :: Expr -> Encode P.Expr
encodeExpr' = \case
    EVar v -> expr . P.ExprSumVarInternedStr <$> encodeNameId unExprVarName v
    EVal (Qualified pkgRef modName val) -> do
        valNameModule <- encodeModuleRef pkgRef modName
        (valNameNameDname, valNameNameInternedDname) <- encodeValueName val
        pureExpr $ P.ExprSumVal P.ValName{..}
    EBuiltin bi -> expr <$> encodeBuiltinExpr bi
    ERecCon{..} -> do
        expr_RecConTycon <- encodeTypeConApp recTypeCon
        expr_RecConFields <- encodeFieldsWithExprs unFieldName recFields
        pureExpr $ P.ExprSumRecCon P.Expr_RecCon{..}
    ERecProj{..} -> do
        expr_RecProjTycon <- encodeTypeConApp recTypeCon
        expr_RecProjField <- encodeName unFieldName recField
        expr_RecProjRecord <- encodeExpr recExpr
        pureExpr $ P.ExprSumRecProj P.Expr_RecProj{..}
    ERecUpd{..} -> do
        expr_RecUpdTycon <- encodeTypeConApp recTypeCon
        expr_RecUpdField <- encodeName unFieldName recField
        expr_RecUpdRecord <- encodeExpr recExpr
        expr_RecUpdUpdate <- encodeExpr recUpdate
        pureExpr $ P.ExprSumRecUpd P.Expr_RecUpd{..}
    EVariantCon{..} -> do
        expr_VariantConTycon <- encodeTypeConApp varTypeCon
        expr_VariantConVariantCon <- encodeName unVariantConName varVariant
        expr_VariantConVariantArg <- encodeExpr varArg
        pureExpr $ P.ExprSumVariantCon P.Expr_VariantCon{..}
    EEnumCon{..} -> do
        expr_EnumConTycon <- encodeQualTypeConName enumTypeCon
        expr_EnumConEnumCon <- encodeName unVariantConName enumDataCon
        pureExpr $ P.ExprSumEnumCon P.Expr_EnumCon{..}
    EStructCon{..} -> do
        expr_StructConFields <- encodeFieldsWithExprs unFieldName structFields
        pureExpr $ P.ExprSumStructCon P.Expr_StructCon{..}
    EStructProj{..} -> do
        expr_StructProjField <- encodeName unFieldName structField
        expr_StructProjStruct <- encodeExpr structExpr
        pureExpr $ P.ExprSumStructProj P.Expr_StructProj{..}
    EStructUpd{..} -> do
        expr_StructUpdField <- encodeName unFieldName structField
        expr_StructUpdStruct <- encodeExpr structExpr
        expr_StructUpdUpdate <- encodeExpr structUpdate
        pureExpr $ P.ExprSumStructUpd P.Expr_StructUpd{..}
    e@ETmApp{} -> do
        let (fun, args) = e ^. _ETmApps
        expr_AppFun <- encodeExpr fun
        expr_AppArgs <- encodeList encodeExpr' args
        pureExpr $ P.ExprSumApp P.Expr_App{..}
    e@ETyApp{} -> do
        let (fun, args) = e ^. _ETyApps
        expr_TyAppExpr <- encodeExpr fun
        expr_TyAppTypes <- encodeList encodeType' args
        pureExpr $ P.ExprSumTyApp P.Expr_TyApp{..}
    e@ETmLam{} -> do
        let (params, body) = e ^. _ETmLams
        expr_AbsParam <- encodeList encodeExprVarWithType params
        expr_AbsBody <- encodeExpr body
        pureExpr $ P.ExprSumAbs P.Expr_Abs{..}
    e@ETyLam{} -> do
        let (params, body) = e ^. _ETyLams
        expr_TyAbsParam <- encodeTypeVarsWithKinds params
        expr_TyAbsBody <- encodeExpr body
        pureExpr $ P.ExprSumTyAbs P.Expr_TyAbs{..}
    ECase{..} -> do
        caseScrut <- encodeExpr casScrutinee
        caseAlts <- encodeList encodeCaseAlternative casAlternatives
        pureExpr $ P.ExprSumCase P.Case{..}
    e@ELet{} -> do
      let (lets, body) = e ^. _ELets
      expr . P.ExprSumLet <$> encodeBlock lets body
    ENil{..} -> do
      expr_NilType <- encodeType nilType
      pureExpr $ P.ExprSumNil P.Expr_Nil{..}
    ECons{..} -> do
        let unwind e0 as = case matching _ECons e0 of
                Left e1 -> (e1, as)
                Right (typ, hd, tl)
                  | typ /= consType -> error "internal error: unexpected mismatch in cons cell type"
                  | otherwise -> unwind tl (hd:as)
        let (ctail, cfront) = unwind consTail [consHead]
        expr_ConsType <- encodeType consType
        expr_ConsFront <- encodeList encodeExpr' $ reverse cfront
        expr_ConsTail <- encodeExpr ctail
        pureExpr $ P.ExprSumCons P.Expr_Cons{..}
    EUpdate u -> expr . P.ExprSumUpdate <$> encodeUpdate u
    EScenario s -> expr . P.ExprSumScenario <$> encodeScenario s
    ELocation loc e -> do
        P.Expr{..} <- encodeExpr' e
        exprLocation <- Just <$> encodeSourceLoc loc
        pure P.Expr{..}
    ENone typ -> do
        expr_OptionalNoneType <- encodeType typ
        pureExpr $ P.ExprSumOptionalNone P.Expr_OptionalNone{..}
    ESome typ body -> do
        expr_OptionalSomeType <- encodeType typ
        expr_OptionalSomeBody <- encodeExpr body
        pureExpr $ P.ExprSumOptionalSome P.Expr_OptionalSome{..}
    EToAny ty body -> do
        expr_ToAnyType <- encodeType ty
        expr_ToAnyExpr <- encodeExpr body
        pureExpr $ P.ExprSumToAny P.Expr_ToAny{..}
    EFromAny ty body -> do
        expr_FromAnyType <- encodeType ty
        expr_FromAnyExpr <- encodeExpr body
        pureExpr $ P.ExprSumFromAny P.Expr_FromAny{..}
    ETypeRep ty -> do
        expr . P.ExprSumTypeRep <$> encodeType' ty
    EToAnyException ty val -> do
        expr_ToAnyExceptionType <- encodeType ty
        expr_ToAnyExceptionExpr <- encodeExpr val
        pureExpr $ P.ExprSumToAnyException P.Expr_ToAnyException{..}
    EFromAnyException ty val -> do
        expr_FromAnyExceptionType <- encodeType ty
        expr_FromAnyExceptionExpr <- encodeExpr val
        pureExpr $ P.ExprSumFromAnyException P.Expr_FromAnyException{..}
    EThrow ty1 ty2 val -> do
        expr_ThrowReturnType <- encodeType ty1
        expr_ThrowExceptionType <- encodeType ty2
        expr_ThrowExceptionExpr <- encodeExpr val
        pureExpr $ P.ExprSumThrow P.Expr_Throw{..}
    EToInterface ty1 ty2 val -> do
        expr_ToInterfaceInterfaceType <- encodeQualTypeConName ty1
        expr_ToInterfaceTemplateType <- encodeQualTypeConName ty2
        expr_ToInterfaceTemplateExpr <- encodeExpr val
        pureExpr $ P.ExprSumToInterface P.Expr_ToInterface{..}
    EFromInterface ty1 ty2 val -> do
        expr_FromInterfaceInterfaceType <- encodeQualTypeConName ty1
        expr_FromInterfaceTemplateType <- encodeQualTypeConName ty2
        expr_FromInterfaceInterfaceExpr <- encodeExpr val
        pureExpr $ P.ExprSumFromInterface P.Expr_FromInterface{..}
    EUnsafeFromInterface ty1 ty2 cid val -> do
        expr_UnsafeFromInterfaceInterfaceType <- encodeQualTypeConName ty1
        expr_UnsafeFromInterfaceTemplateType <- encodeQualTypeConName ty2
        expr_UnsafeFromInterfaceContractIdExpr <- encodeExpr cid
        expr_UnsafeFromInterfaceInterfaceExpr <- encodeExpr val
        pureExpr $ P.ExprSumUnsafeFromInterface P.Expr_UnsafeFromInterface{..}
    ECallInterface ty mth val -> do
        expr_CallInterfaceInterfaceType <- encodeQualTypeConName ty
        expr_CallInterfaceMethodInternedName <- encodeMethodName mth
        expr_CallInterfaceInterfaceExpr <- encodeExpr val
        pureExpr $ P.ExprSumCallInterface P.Expr_CallInterface{..}
    EToRequiredInterface ty1 ty2 val -> do
        expr_ToRequiredInterfaceRequiredInterface <- encodeQualTypeConName ty1
        expr_ToRequiredInterfaceRequiringInterface <- encodeQualTypeConName ty2
        expr_ToRequiredInterfaceExpr <- encodeExpr val
        pureExpr $ P.ExprSumToRequiredInterface P.Expr_ToRequiredInterface{..}
    EFromRequiredInterface ty1 ty2 val -> do
        expr_FromRequiredInterfaceRequiredInterface <- encodeQualTypeConName ty1
        expr_FromRequiredInterfaceRequiringInterface <- encodeQualTypeConName ty2
        expr_FromRequiredInterfaceExpr <- encodeExpr val
        pureExpr $ P.ExprSumFromRequiredInterface P.Expr_FromRequiredInterface{..}
    EUnsafeFromRequiredInterface ty1 ty2 cid val -> do
        expr_UnsafeFromRequiredInterfaceRequiredInterface <- encodeQualTypeConName ty1
        expr_UnsafeFromRequiredInterfaceRequiringInterface <- encodeQualTypeConName ty2
        expr_UnsafeFromRequiredInterfaceContractIdExpr <- encodeExpr cid
        expr_UnsafeFromRequiredInterfaceInterfaceExpr <- encodeExpr val
        pureExpr $ P.ExprSumUnsafeFromRequiredInterface P.Expr_UnsafeFromRequiredInterface{..}
    EInterfaceTemplateTypeRep ty val -> do
        expr_InterfaceTemplateTypeRepInterface <- encodeQualTypeConName ty
        expr_InterfaceTemplateTypeRepExpr <- encodeExpr val
        pureExpr $ P.ExprSumInterfaceTemplateTypeRep P.Expr_InterfaceTemplateTypeRep{..}
    ESignatoryInterface ty val -> do
        expr_SignatoryInterfaceInterface <- encodeQualTypeConName ty
        expr_SignatoryInterfaceExpr <- encodeExpr val
        pureExpr $ P.ExprSumSignatoryInterface P.Expr_SignatoryInterface{..}
    EObserverInterface ty val -> do
        expr_ObserverInterfaceInterface <- encodeQualTypeConName ty
        expr_ObserverInterfaceExpr <- encodeExpr val
        pureExpr $ P.ExprSumObserverInterface P.Expr_ObserverInterface{..}
    EViewInterface iface expr -> do
        expr_ViewInterfaceInterface <- encodeQualTypeConName iface
        expr_ViewInterfaceExpr <- encodeExpr expr
        pureExpr $ P.ExprSumViewInterface P.Expr_ViewInterface{..}
    EChoiceController tpl ch expr1 expr2 -> do
        expr_ChoiceControllerTemplate <- encodeQualTypeConName tpl
        expr_ChoiceControllerChoiceInternedStr <- encodeNameId unChoiceName ch
        expr_ChoiceControllerContractExpr <- encodeExpr expr1
        expr_ChoiceControllerChoiceArgExpr <- encodeExpr expr2
        pureExpr $ P.ExprSumChoiceController P.Expr_ChoiceController{..}
    EChoiceObserver tpl ch expr1 expr2 -> do
        expr_ChoiceObserverTemplate <- encodeQualTypeConName tpl
        expr_ChoiceObserverChoiceInternedStr <- encodeNameId unChoiceName ch
        expr_ChoiceObserverContractExpr <- encodeExpr expr1
        expr_ChoiceObserverChoiceArgExpr <- encodeExpr expr2
        pureExpr $ P.ExprSumChoiceObserver P.Expr_ChoiceObserver{..}
    EExperimental name ty -> do
        let expr_ExperimentalName = encodeString name
        expr_ExperimentalType <- encodeType ty
        pureExpr $ P.ExprSumExperimental P.Expr_Experimental{..}
  where
    expr = P.Expr Nothing . Just
    pureExpr = pure . expr

encodeExpr :: Expr -> Encode (Just P.Expr)
encodeExpr e = Just <$> encodeExpr' e

encodeUpdate :: Update -> Encode P.Update
encodeUpdate = fmap (P.Update . Just) . \case
    UPure{..} -> do
        pureType <- encodeType pureType
        pureExpr <- encodeExpr pureExpr
        pure $ P.UpdateSumPure P.Pure{..}
    e@UBind{} -> do
      let (bindings, body) = EUpdate e ^. rightSpine (_EUpdate . _UBind)
      P.UpdateSumBlock <$> encodeBlock bindings body
    UCreate{..} -> do
        update_CreateTemplate <- encodeQualTypeConName creTemplate
        update_CreateExpr <- encodeExpr creArg
        pure $ P.UpdateSumCreate P.Update_Create{..}
    UCreateInterface{..} -> do
        update_CreateInterfaceInterface <- encodeQualTypeConName creInterface
        update_CreateInterfaceExpr <- encodeExpr creArg
        pure $ P.UpdateSumCreateInterface P.Update_CreateInterface{..}
    UExercise{..} -> do
        update_ExerciseTemplate <- encodeQualTypeConName exeTemplate
        update_ExerciseChoice <- encodeName unChoiceName exeChoice
        update_ExerciseCid <- encodeExpr exeContractId
        update_ExerciseArg <- encodeExpr exeArg
        pure $ P.UpdateSumExercise P.Update_Exercise{..}
    USoftExercise{..} -> do
        update_SoftExerciseTemplate <- encodeQualTypeConName exeTemplate
        update_SoftExerciseChoice <- encodeName unChoiceName exeChoice
        update_SoftExerciseCid <- encodeExpr exeContractId
        update_SoftExerciseArg <- encodeExpr exeArg
        pure $ P.UpdateSumSoftExercise P.Update_SoftExercise{..}
    UDynamicExercise{..} -> do
        update_DynamicExerciseTemplate <- encodeQualTypeConName exeTemplate
        update_DynamicExerciseChoiceInternedStr <- encodeNameId unChoiceName exeChoice
        update_DynamicExerciseCid <- encodeExpr exeContractId
        update_DynamicExerciseArg <- encodeExpr exeArg
        pure $ P.UpdateSumDynamicExercise P.Update_DynamicExercise{..}
    UExerciseInterface{..} -> do
        update_ExerciseInterfaceInterface <- encodeQualTypeConName exeInterface
        update_ExerciseInterfaceChoiceInternedStr <- encodeNameId unChoiceName exeChoice
        update_ExerciseInterfaceCid <- encodeExpr exeContractId
        update_ExerciseInterfaceArg <- encodeExpr exeArg
        update_ExerciseInterfaceGuard <- traverse encodeExpr' exeGuard
        pure $ P.UpdateSumExerciseInterface P.Update_ExerciseInterface{..}
    UExerciseByKey{..} -> do
        update_ExerciseByKeyTemplate <- encodeQualTypeConName exeTemplate
        update_ExerciseByKeyChoiceInternedStr <- encodeNameId unChoiceName exeChoice
        update_ExerciseByKeyKey <- encodeExpr exeKey
        update_ExerciseByKeyArg <- encodeExpr exeArg
        pure $ P.UpdateSumExerciseByKey P.Update_ExerciseByKey{..}
    UFetch{..} -> do
        update_FetchTemplate <- encodeQualTypeConName fetTemplate
        update_FetchCid <- encodeExpr fetContractId
        pure $ P.UpdateSumFetch P.Update_Fetch{..}
    USoftFetch{..} -> do
        update_SoftFetchTemplate <- encodeQualTypeConName fetTemplate
        update_SoftFetchCid <- encodeExpr fetContractId
        pure $ P.UpdateSumSoftFetch P.Update_SoftFetch{..}
    UFetchInterface{..} -> do
        update_FetchInterfaceInterface <- encodeQualTypeConName fetInterface
        update_FetchInterfaceCid <- encodeExpr fetContractId
        pure $ P.UpdateSumFetchInterface P.Update_FetchInterface{..}
    UGetTime -> pure $ P.UpdateSumGetTime P.Unit
    UEmbedExpr typ e -> do
        update_EmbedExprType <- encodeType typ
        update_EmbedExprBody <- encodeExpr e
        pure $ P.UpdateSumEmbedExpr P.Update_EmbedExpr{..}
    UFetchByKey rbk ->
        P.UpdateSumFetchByKey <$> encodeRetrieveByKey rbk
    ULookupByKey rbk ->
        P.UpdateSumLookupByKey <$> encodeRetrieveByKey rbk
    UTryCatch{..} -> do
        update_TryCatchReturnType <- encodeType tryCatchType
        update_TryCatchTryExpr <- encodeExpr tryCatchExpr
        update_TryCatchVarInternedStr <- encodeNameId unExprVarName tryCatchVar
        update_TryCatchCatchExpr <- encodeExpr tryCatchHandler
        pure $ P.UpdateSumTryCatch P.Update_TryCatch{..}

encodeRetrieveByKey :: RetrieveByKey -> Encode P.Update_RetrieveByKey
encodeRetrieveByKey RetrieveByKey{..} = do
    update_RetrieveByKeyTemplate <- encodeQualTypeConName retrieveByKeyTemplate
    update_RetrieveByKeyKey <- encodeExpr retrieveByKeyKey
    pure P.Update_RetrieveByKey{..}

encodeScenario :: Scenario -> Encode P.Scenario
encodeScenario = fmap (P.Scenario . Just) . \case
    SPure{..} -> do
        pureType <- encodeType spureType
        pureExpr <- encodeExpr spureExpr
        pure $ P.ScenarioSumPure P.Pure{..}
    e@SBind{} -> do
      let (bindings, body) = EScenario e ^. rightSpine (_EScenario . _SBind)
      P.ScenarioSumBlock <$> encodeBlock bindings body
    SCommit{..} -> do
        scenario_CommitParty <- encodeExpr scommitParty
        scenario_CommitExpr <- encodeExpr scommitExpr
        scenario_CommitRetType <- encodeType scommitType
        pure $ P.ScenarioSumCommit P.Scenario_Commit{..}
    SMustFailAt{..} -> do
        scenario_CommitParty <- encodeExpr smustFailAtParty
        scenario_CommitExpr <- encodeExpr smustFailAtExpr
        scenario_CommitRetType <- encodeType smustFailAtType
        pure $ P.ScenarioSumMustFailAt P.Scenario_Commit{..}
    SPass{..} ->
        P.ScenarioSumPass <$> encodeExpr' spassDelta
    SGetTime -> pure $ P.ScenarioSumGetTime P.Unit
    SGetParty{..} ->
        P.ScenarioSumGetParty <$> encodeExpr' sgetPartyName
    SEmbedExpr typ e -> do
        scenario_EmbedExprType <- encodeType typ
        scenario_EmbedExprBody <- encodeExpr e
        pure $ P.ScenarioSumEmbedExpr P.Scenario_EmbedExpr{..}

encodeBinding :: Binding -> Encode P.Binding
encodeBinding (Binding binder bound) = do
    bindingBinder <- Just <$> encodeExprVarWithType binder
    bindingBound <- encodeExpr bound
    pure P.Binding{..}

encodeBlock :: [Binding] -> Expr -> Encode P.Block
encodeBlock bindings body = do
    blockBindings <- encodeList encodeBinding bindings
    blockBody <- encodeExpr body
    pure P.Block{..}

encodeCaseAlternative :: CaseAlternative -> Encode P.CaseAlt
encodeCaseAlternative CaseAlternative{..} = do
    caseAltSum <- fmap Just $ case altPattern of
        CPDefault -> pure $ P.CaseAltSumDefault P.Unit
        CPVariant{..} -> do
            caseAlt_VariantCon <- encodeQualTypeConName patTypeCon
            caseAlt_VariantVariant <- encodeName unVariantConName patVariant
            caseAlt_VariantBinder <- encodeName unExprVarName patBinder
            pure $ P.CaseAltSumVariant P.CaseAlt_Variant{..}
        CPEnum{..} -> do
            caseAlt_EnumCon <- encodeQualTypeConName patTypeCon
            caseAlt_EnumConstructor <- encodeName unVariantConName patDataCon
            pure $ P.CaseAltSumEnum P.CaseAlt_Enum{..}
        CPUnit -> pure $ P.CaseAltSumPrimCon $ P.Enumerated $ Right P.PrimConCON_UNIT
        CPBool b -> pure $ P.CaseAltSumPrimCon $ P.Enumerated $ Right $ case b of
            False -> P.PrimConCON_FALSE
            True -> P.PrimConCON_TRUE
        CPNil -> pure $ P.CaseAltSumNil P.Unit
        CPCons{..} -> do
            caseAlt_ConsVarHead <- encodeName unExprVarName patHeadBinder
            caseAlt_ConsVarTail <- encodeName unExprVarName patTailBinder
            pure $ P.CaseAltSumCons P.CaseAlt_Cons{..}
        CPNone -> pure $ P.CaseAltSumOptionalNone P.Unit
        CPSome{..} -> do
            caseAlt_OptionalSomeVarBody <- encodeName unExprVarName patBodyBinder
            pure $ P.CaseAltSumOptionalSome P.CaseAlt_OptionalSome{..}
    caseAltBody <- encodeExpr altExpr
    pure P.CaseAlt{..}

encodeDefTypeSyn :: DefTypeSyn -> Encode P.DefTypeSyn
encodeDefTypeSyn DefTypeSyn{..} = do
    defTypeSynName <- encodeDottedName unTypeSynName synName
    defTypeSynParams <- encodeTypeVarsWithKinds synParams
    defTypeSynType <- encodeType synType
    defTypeSynLocation <- traverse encodeSourceLoc synLocation
    pure P.DefTypeSyn{..}

encodeDefDataType :: DefDataType -> Encode P.DefDataType
encodeDefDataType DefDataType{..} = do
    defDataTypeName <- encodeDottedName unTypeConName dataTypeCon
    defDataTypeParams <- encodeTypeVarsWithKinds dataParams
    defDataTypeDataCons <- fmap Just $ case dataCons of
        DataRecord fs -> do
            defDataType_FieldsFields <- encodeFieldsWithTypes unFieldName fs
            pure $ P.DefDataTypeDataConsRecord P.DefDataType_Fields{..}
        DataVariant fs -> do
            defDataType_FieldsFields <- encodeFieldsWithTypes unVariantConName fs
            pure $ P.DefDataTypeDataConsVariant P.DefDataType_Fields{..}
        DataEnum cs -> do
            mangledIds <- encodeNames (map unVariantConName cs)
            -- TODO(https://github.com/digital-asset/daml/issues/18240): remove
            -- the constructors_str field from the proto definition.
            let defDataType_EnumConstructorsConstructorsStr = V.empty
            let defDataType_EnumConstructorsConstructorsInternedStr = V.fromList mangledIds
            pure $ P.DefDataTypeDataConsEnum P.DefDataType_EnumConstructors{..}
        DataInterface -> pure $ P.DefDataTypeDataConsInterface P.Unit
    let defDataTypeSerializable = getIsSerializable dataSerializable
    defDataTypeLocation <- traverse encodeSourceLoc dataLocation
    pure P.DefDataType{..}

encodeDefValue :: DefValue -> Encode P.DefValue
encodeDefValue DefValue{..} = do
    (defValue_NameWithTypeNameDname, defValue_NameWithTypeNameInternedDname) <- encodeValueName (fst dvalBinder)
    defValue_NameWithTypeType <- encodeType (snd dvalBinder)
    let defValueNameWithType = Just P.DefValue_NameWithType{..}
    defValueExpr <- encodeExpr dvalBody
    let defValueNoPartyLiterals = True
    let defValueIsTest = getIsTest dvalIsTest
    defValueLocation <- traverse encodeSourceLoc dvalLocation
    pure P.DefValue{..}

encodeDefException :: DefException -> Encode P.DefException
encodeDefException DefException{..} = do
    defExceptionNameInternedDname <- encodeDottedNameId unTypeConName exnName
    defExceptionLocation <- traverse encodeSourceLoc exnLocation
    defExceptionMessage <- encodeExpr exnMessage
    pure P.DefException{..}


encodeTemplate :: Template -> Encode P.DefTemplate
encodeTemplate Template{..} = do
    defTemplateTycon <- encodeDottedName unTypeConName tplTypeCon
    defTemplateParam <- encodeName unExprVarName tplParam
    defTemplatePrecond <- encodeExpr tplPrecondition
    defTemplateSignatories <- encodeExpr tplSignatories
    defTemplateObservers <- encodeExpr tplObservers
    defTemplateChoices <- encodeNameMap encodeTemplateChoice tplChoices
    defTemplateLocation <- traverse encodeSourceLoc tplLocation
    defTemplateKey <- traverse encodeTemplateKey tplKey
    defTemplateImplements <- encodeNameMap encodeTemplateImplements tplImplements
    let defTemplateAgreement = Nothing
    pure P.DefTemplate{..}

encodeTemplateImplements :: TemplateImplements -> Encode P.DefTemplate_Implements
encodeTemplateImplements TemplateImplements{..} = do
    defTemplate_ImplementsInterface <- encodeQualTypeConName tpiInterface
    defTemplate_ImplementsBody <- encodeInterfaceInstanceBody tpiBody
    defTemplate_ImplementsLocation <- traverse encodeSourceLoc tpiLocation
    pure P.DefTemplate_Implements {..}

encodeInterfaceInstanceBody :: InterfaceInstanceBody -> Encode (Just P.InterfaceInstanceBody)
encodeInterfaceInstanceBody InterfaceInstanceBody {..} = do
    interfaceInstanceBodyMethods <- encodeNameMap encodeInterfaceInstanceMethod iiMethods
    interfaceInstanceBodyView <- encodeExpr iiView
    pure $ Just P.InterfaceInstanceBody {..}

encodeInterfaceInstanceMethod :: InterfaceInstanceMethod -> Encode P.InterfaceInstanceBody_InterfaceInstanceMethod
encodeInterfaceInstanceMethod InterfaceInstanceMethod{..} = do
    interfaceInstanceBody_InterfaceInstanceMethodMethodInternedName <- encodeMethodName iiMethodName
    interfaceInstanceBody_InterfaceInstanceMethodValue <- encodeExpr iiMethodExpr
    pure P.InterfaceInstanceBody_InterfaceInstanceMethod {..}

encodeTemplateKey :: TemplateKey -> Encode P.DefTemplate_DefKey
encodeTemplateKey TemplateKey{..} = do
    defTemplate_DefKeyType <- encodeType tplKeyType
    defTemplate_DefKeyKeyExpr <-
        Just . P.DefTemplate_DefKeyKeyExprComplexKey <$> encodeExpr' tplKeyBody
    defTemplate_DefKeyMaintainers <- encodeExpr tplKeyMaintainers
    pure P.DefTemplate_DefKey{..}

encodeChoiceObservers :: Maybe Expr -> Encode (Just P.Expr)
encodeChoiceObservers chcObservers =
  encodeExpr (fromMaybe (ENil TParty) chcObservers)

encodeChoiceAuthorizers :: Maybe Expr -> Encode (Maybe P.Expr)
encodeChoiceAuthorizers = \case
  Nothing -> pure Nothing -- dont add field to proto
  Just xs -> encodeExpr xs

encodeTemplateChoice :: TemplateChoice -> Encode P.TemplateChoice
encodeTemplateChoice TemplateChoice{..} = do
    templateChoiceName <- encodeName unChoiceName chcName
    let templateChoiceConsuming = chcConsuming
    templateChoiceControllers <- encodeExpr chcControllers
    templateChoiceObservers <- encodeChoiceObservers chcObservers
    templateChoiceAuthorizers <- encodeChoiceAuthorizers chcAuthorizers
    templateChoiceSelfBinder <- encodeName unExprVarName chcSelfBinder
    templateChoiceArgBinder <- Just <$> encodeExprVarWithType chcArgBinder
    templateChoiceRetType <- encodeType chcReturnType
    templateChoiceUpdate <- encodeExpr chcUpdate
    templateChoiceLocation <- traverse encodeSourceLoc chcLocation
    pure P.TemplateChoice{..}

encodeFeatureFlags :: FeatureFlags -> Just P.FeatureFlags
encodeFeatureFlags FeatureFlags = Just P.FeatureFlags
    { P.featureFlagsForbidPartyLiterals = True
    , P.featureFlagsDontDivulgeContractIdsInCreateArguments = True
    , P.featureFlagsDontDiscloseNonConsumingChoicesToObservers = True
    }

-- each scenario module is wrapped in a proto package
encodeScenarioModule :: Version -> Module -> P.Package
encodeScenarioModule version mod =
    encodePackage (Package version (NM.insert mod NM.empty) metadata)
  where
    metadata = PackageMetadata
      { packageName = PackageName "scenario"
      , packageVersion = PackageVersion "0.0.0"
      , upgradedPackageId = Nothing
      }

encodeModule :: Module -> Encode P.Module
encodeModule Module{..} = do
    moduleName <- encodeDottedName unModuleName moduleName
    let moduleFlags = encodeFeatureFlags moduleFeatureFlags
    moduleSynonyms <- encodeNameMap encodeDefTypeSyn moduleSynonyms
    moduleDataTypes <- encodeNameMap encodeDefDataType moduleDataTypes
    moduleValues <- encodeNameMap encodeDefValue moduleValues
    moduleTemplates <- encodeNameMap encodeTemplate moduleTemplates
    moduleExceptions <- encodeNameMap encodeDefException moduleExceptions
    moduleInterfaces <- encodeNameMap encodeDefInterface moduleInterfaces
    pure P.Module{..}

encodeDefInterface :: DefInterface -> Encode P.DefInterface
encodeDefInterface DefInterface{..} = do
    defInterfaceLocation <- traverse encodeSourceLoc intLocation
    defInterfaceTyconInternedDname <- encodeDottedNameId unTypeConName intName
    defInterfaceRequires <- encodeSet encodeQualTypeConName' intRequires
    defInterfaceMethods <- encodeNameMap encodeInterfaceMethod intMethods
    defInterfaceParamInternedStr <- encodeNameId unExprVarName intParam
    defInterfaceChoices <- encodeNameMap encodeTemplateChoice intChoices
    defInterfaceCoImplements <- encodeNameMap encodeInterfaceCoImplements intCoImplements
    defInterfaceView <- encodeType intView
    pure $ P.DefInterface{..}

encodeInterfaceMethod :: InterfaceMethod -> Encode P.InterfaceMethod
encodeInterfaceMethod InterfaceMethod {..} = do
    interfaceMethodLocation <- traverse encodeSourceLoc ifmLocation
    interfaceMethodMethodInternedName <- encodeMethodName ifmName
    interfaceMethodType <- encodeType ifmType
    pure $ P.InterfaceMethod{..}

encodeInterfaceCoImplements :: InterfaceCoImplements -> Encode P.DefInterface_CoImplements
encodeInterfaceCoImplements InterfaceCoImplements {..} = do
    defInterface_CoImplementsTemplate <- encodeQualTypeConName iciTemplate
    defInterface_CoImplementsBody <- encodeInterfaceInstanceBody iciBody
    defInterface_CoImplementsLocation <- traverse encodeSourceLoc iciLocation
    pure P.DefInterface_CoImplements {..}

encodeUpgradedPackageId :: PackageId -> Encode P.UpgradedPackageId
encodeUpgradedPackageId upgradedPackageId = do
  upgradedPackageIdUpgradedPackageIdInternedStr <- encodeInternableString (unPackageId upgradedPackageId)
  pure P.UpgradedPackageId{..}

encodePackageMetadata :: PackageMetadata -> Encode P.PackageMetadata
encodePackageMetadata PackageMetadata{..} = do
    packageMetadataNameInternedStr <- encodeInternableString (unPackageName packageName)
    packageMetadataVersionInternedStr <- encodeInternableString (unPackageVersion packageVersion)
    packageMetadataUpgradedPackageId <- traverse encodeUpgradedPackageId upgradedPackageId
    pure P.PackageMetadata{..}

encodePackage :: Package -> P.Package
encodePackage (Package version mods metadata) =
    let env = initEncodeEnv version
        ((packageModules, packageMetadata), EncodeEnv{internedStrings, internedDottedNames, internedTypes}) =
            runState ((,) <$> encodeNameMap encodeModule mods <*> fmap Just (encodePackageMetadata metadata)) env
        packageInternedStrings =
            V.fromList $ map (encodeString . fst) $ L.sortOn snd $ HMS.toList internedStrings
        packageInternedDottedNames =
            V.fromList $ map (P.InternedDottedName . V.fromList . fst) $ L.sortOn snd $ HMS.toList internedDottedNames
        packageInternedTypes =
            V.fromList $ map (P.Type . Just . fst) $ L.sortOn snd $ Map.toList internedTypes
    in
    P.Package{..}

-- | NOTE(MH): This functions is used for sanity checking. The actual checks
-- are done in the conversion to Daml-LF.
_checkFeature :: Feature -> Version -> a -> a
_checkFeature feature version x
    | version `supports` feature = x
    | otherwise = error $ "Daml-LF " ++ renderPretty version ++ " cannot encode feature: " ++ T.unpack (featureName feature)
