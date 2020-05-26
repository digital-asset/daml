-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}
-- | Encoding of the LF package into LF version 1 format.
module DA.Daml.LF.Proto3.EncodeV1
  ( encodeScenarioModule
  , encodePackage
  ) where

import           Control.Lens ((^.), matching)
import           Control.Lens.Ast (rightSpine)
import           Control.Monad.State.Strict

import qualified Data.Bifunctor as Bf
import           Data.Coerce
import           Data.Functor.Identity
import qualified Data.HashMap.Strict as HMS
import qualified Data.List as L
import qualified Data.NameMap as NM
import qualified Data.Text           as T
import qualified Data.Text.Lazy      as TL
import qualified Data.Vector         as V
import           Data.Int

import           DA.Pretty
import           DA.Daml.LF.Ast
import           DA.Daml.LF.Mangling
import qualified DA.Daml.LF.Proto3.Util as Util
import qualified Com.Daml.DamlLfDev.DamlLf1 as P

import qualified Proto3.Suite as P (Enumerated (..))

-- NOTE(MH): Type synonym for a `Maybe` that is always known to be a `Just`.
-- Some functions always return `Just x` instead of `x` since they would
-- otherwise always be wrapped in `Just` at their call sites.
type Just a = Maybe a

type Encode a = State EncodeEnv a

newtype WithInterning = WithInterning{getWithInterning :: Bool}

data EncodeEnv = EncodeEnv
    { version :: !Version
    , withInterning :: !WithInterning
    , internedStrings :: !(HMS.HashMap T.Text Int32)
    , nextInternedStringId :: !Int32
      -- ^ We track the size of `internedStrings` explicitly since `HMS.size` is `O(n)`.
    , internedDottedNames :: !(HMS.HashMap [Int32] Int32)
    , nextInternedDottedNameId :: !Int32
      -- ^ We track the size of `internedDottedNames` explicitly since `HMS.size` is `O(n)`.
    }

initEncodeEnv :: Version -> WithInterning -> EncodeEnv
initEncodeEnv version withInterning =
    EncodeEnv
    { nextInternedStringId = 0
    , internedStrings = HMS.empty
    , internedDottedNames = HMS.empty
    , nextInternedDottedNameId = 0
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

-- | Encode a string that will be interned in DAML-LF 1.7 and onwards.
encodeInternableString :: T.Text -> Encode (Either TL.Text Int32)
encodeInternableString = coerce (encodeInternableStrings @Identity)

-- | Encode a string that will be interned in DAML-LF 1.7 and onwards.
encodeInternableStrings :: Traversable t => t T.Text -> Encode (Either (t TL.Text) (t Int32))
encodeInternableStrings strs = do
    EncodeEnv{..} <- get
    if getWithInterning withInterning && version `supports` featureStringInterning
        then Right <$> mapM allocString strs
        else pure $ Left $ fmap encodeString strs

-- | Encode the name of a syntactic object, e.g., a variable or a data
-- constructor. These strings are mangled to escape special characters. All
-- names will be interned in DAML-LF 1.7 and onwards.
encodeName
    :: Util.EitherLike TL.Text Int32 e
    => (a -> T.Text) -> a -> Encode (Just e)
encodeName unwrapName = fmap Just . encodeName' unwrapName

encodeName'
    :: Util.EitherLike TL.Text Int32 e
    => (a -> T.Text) -> a -> Encode e
encodeName' unwrapName (unwrapName -> unmangled) = do
    Util.fromEither @TL.Text @Int32 <$> coerce (encodeNames @Identity) unmangled

encodeNames :: Traversable t => t T.Text -> Encode (Either (t TL.Text) (t Int32))
encodeNames = encodeInternableStrings . fmap mangleName
    where
        mangleName :: T.Text -> T.Text
        mangleName unmangled = case mangleIdentifier unmangled of
           Left err -> error $ "IMPOSSIBLE: could not mangle name " ++ show unmangled ++ ": " ++ err
           Right mangled -> mangled


-- | Encode the multi-component name of a syntactic object, e.g., a type
-- constructor. All compononents are mangled. Dotted names will be interned
-- in DAML-LF 1.7 and onwards.
encodeDottedName :: Util.EitherLike P.DottedName Int32 e
                 => (a -> [T.Text]) -> a -> Encode (Just e)
encodeDottedName unwrapDottedName (unwrapDottedName -> unmangled) =
    Just <$>
      Util.fromEither @P.DottedName @Int32 <$>
      Bf.first P.DottedName <$>
      encodeDottedName' unmangled

encodeDottedName' :: [T.Text] -> Encode (Either (V.Vector TL.Text) Int32)
encodeDottedName' unmangled = do
    mangledAndInterned <- encodeNames unmangled
    case mangledAndInterned of
        Left mangled -> pure $ Left (V.fromList mangled)
        Right ids -> do
            id <- allocDottedName ids
            pure $ Right id

-- | Encode the name of a top-level value. The name is mangled and will be
-- interned in DAML-LF 1.7 and onwards.
--
-- For now, value names are always encoded using a single segment.
-- This is to keep backwards compat with older .dalf files, but also
-- because currently GenDALF generates weird names like `.` that we'd
-- have to handle separatedly. So for now, considering that we do not
-- use values in codegen, just mangle the entire thing.
encodeValueName :: ExprValName -> Encode (V.Vector TL.Text, Int32)
encodeValueName valName = do
    either <- encodeDottedName' [unExprValName valName]
    case either of
        Left mangled -> pure (mangled, 0)
        Right id -> pure (V.empty, id)

-- | Encode a reference to a package. Package names are not mangled. Package
-- name are interned since DAML-LF 1.6.
encodePackageRef :: PackageRef -> Encode (Just P.PackageRef)
encodePackageRef = fmap (Just . P.PackageRef . Just) . \case
    PRSelf -> pure $ P.PackageRefSumSelf P.Unit
    PRImport (PackageId pkgId) -> do
        EncodeEnv{..} <- get
        if getWithInterning withInterning
            then P.PackageRefSumPackageIdInternedStr <$> allocString pkgId
            else pure $ P.PackageRefSumPackageIdStr $ encodeString pkgId


------------------------------------------------------------------------
-- Simple encodings
------------------------------------------------------------------------

encodeList :: (a -> Encode b) -> [a] -> Encode (V.Vector b)
encodeList encodeElem = fmap V.fromList . mapM encodeElem

encodeNameMap :: NM.Named a => (a -> Encode b) -> NM.NameMap a -> Encode (V.Vector b)
encodeNameMap encodeElem = fmap V.fromList . mapM encodeElem . NM.toList

encodeQualTypeSynName' :: Qualified TypeSynName -> Encode (P.TypeSynName)
encodeQualTypeSynName' (Qualified pref mname syn) = do
    typeSynNameModule <- encodeModuleRef pref mname
    typeSynNameName <- encodeDottedName unTypeSynName syn
    pure $ P.TypeSynName{..}

encodeQualTypeSynName :: Qualified TypeSynName -> Encode (Just P.TypeSynName)
encodeQualTypeSynName tysyn = Just <$> encodeQualTypeSynName' tysyn

encodeQualTypeConName' :: Qualified TypeConName -> Encode (P.TypeConName)
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
    BTDecimal -> P.PrimTypeDECIMAL
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

encodeType' :: Type -> Encode P.Type
encodeType' typ = fmap (P.Type . Just) $ case typ ^. _TApps of
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

encodeType :: Type -> Encode (Just P.Type)
encodeType t = Just <$> encodeType' t

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
    BEDecimal dec ->
        pureLit $ P.PrimLitSumDecimalStr $ encodeString (T.pack (show dec))
    BENumeric num ->
        lit . P.PrimLitSumNumericInternedStr <$> allocString (T.pack (show num))
    BEText x ->
        lit . either P.PrimLitSumTextStr P.PrimLitSumTextInternedStr
        <$> encodeInternableString x
    BETimestamp x -> pureLit $ P.PrimLitSumTimestamp x
    BEParty x ->
        lit . either P.PrimLitSumPartyStr P.PrimLitSumPartyInternedStr
        <$> encodeInternableString (unPartyLiteral x)
    BEDate x -> pureLit $ P.PrimLitSumDate x

    BEUnit -> pure $ P.ExprSumPrimCon $ P.Enumerated $ Right P.PrimConCON_UNIT
    BEBool b -> pure $ P.ExprSumPrimCon $ P.Enumerated $ Right $ case b of
        False -> P.PrimConCON_FALSE
        True -> P.PrimConCON_TRUE

    BEEqualGeneric -> builtin P.BuiltinFunctionEQUAL
    BELessGeneric -> builtin P.BuiltinFunctionLESS
    BELessEqGeneric -> builtin P.BuiltinFunctionLESS_EQ
    BEGreaterGeneric -> builtin P.BuiltinFunctionGREATER
    BEGreaterEqGeneric -> builtin P.BuiltinFunctionGREATER_EQ

    BEEqual typ -> case typ of
      BTInt64 -> builtin P.BuiltinFunctionEQUAL_INT64
      BTDecimal -> builtin P.BuiltinFunctionEQUAL_DECIMAL
      BTText -> builtin P.BuiltinFunctionEQUAL_TEXT
      BTTimestamp -> builtin P.BuiltinFunctionEQUAL_TIMESTAMP
      BTDate -> builtin P.BuiltinFunctionEQUAL_DATE
      BTParty -> builtin P.BuiltinFunctionEQUAL_PARTY
      BTBool -> builtin P.BuiltinFunctionEQUAL_BOOL
      BTTypeRep -> builtin P.BuiltinFunctionEQUAL_TYPE_REP
      other -> error $ "BEEqual unexpected type " <> show other

    BELessEq typ -> case typ of
      BTInt64 -> builtin P.BuiltinFunctionLEQ_INT64
      BTDecimal -> builtin P.BuiltinFunctionLEQ_DECIMAL
      BTText -> builtin P.BuiltinFunctionLEQ_TEXT
      BTTimestamp -> builtin P.BuiltinFunctionLEQ_TIMESTAMP
      BTDate -> builtin P.BuiltinFunctionLEQ_DATE
      BTParty -> builtin P.BuiltinFunctionLEQ_PARTY
      other -> error $ "BELessEq unexpected type " <> show other

    BELess typ -> case typ of
      BTInt64 -> builtin P.BuiltinFunctionLESS_INT64
      BTDecimal -> builtin P.BuiltinFunctionLESS_DECIMAL
      BTText -> builtin P.BuiltinFunctionLESS_TEXT
      BTTimestamp -> builtin P.BuiltinFunctionLESS_TIMESTAMP
      BTDate -> builtin P.BuiltinFunctionLESS_DATE
      BTParty -> builtin P.BuiltinFunctionLESS_PARTY
      other -> error $ "BELess unexpected type " <> show other

    BEGreaterEq typ -> case typ of
      BTInt64 -> builtin P.BuiltinFunctionGEQ_INT64
      BTDecimal -> builtin P.BuiltinFunctionGEQ_DECIMAL
      BTText -> builtin P.BuiltinFunctionGEQ_TEXT
      BTTimestamp -> builtin P.BuiltinFunctionGEQ_TIMESTAMP
      BTDate -> builtin P.BuiltinFunctionGEQ_DATE
      BTParty -> builtin P.BuiltinFunctionGEQ_PARTY
      other -> error $ "BEGreaterEq unexpected type " <> show other

    BEGreater typ -> case typ of
      BTInt64 -> builtin P.BuiltinFunctionGREATER_INT64
      BTDecimal -> builtin P.BuiltinFunctionGREATER_DECIMAL
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
      BTInt64 -> builtin P.BuiltinFunctionTO_TEXT_INT64
      BTDecimal -> builtin P.BuiltinFunctionTO_TEXT_DECIMAL
      BTText -> builtin P.BuiltinFunctionTO_TEXT_TEXT
      BTTimestamp -> builtin P.BuiltinFunctionTO_TEXT_TIMESTAMP
      BTDate -> builtin P.BuiltinFunctionTO_TEXT_DATE
      BTParty -> builtin P.BuiltinFunctionTO_TEXT_PARTY
      other -> error $ "BEToText unexpected type " <> show other
    BEToTextNumeric -> builtin P.BuiltinFunctionTO_TEXT_NUMERIC
    BETextFromCodePoints -> builtin P.BuiltinFunctionTEXT_FROM_CODE_POINTS
    BEPartyFromText -> builtin P.BuiltinFunctionFROM_TEXT_PARTY
    BEInt64FromText -> builtin P.BuiltinFunctionFROM_TEXT_INT64
    BEDecimalFromText-> builtin P.BuiltinFunctionFROM_TEXT_DECIMAL
    BENumericFromText-> builtin P.BuiltinFunctionFROM_TEXT_NUMERIC
    BETextToCodePoints -> builtin P.BuiltinFunctionTEXT_TO_CODE_POINTS
    BEPartyToQuotedText -> builtin P.BuiltinFunctionTO_QUOTED_TEXT_PARTY

    BEAddDecimal -> builtin P.BuiltinFunctionADD_DECIMAL
    BESubDecimal -> builtin P.BuiltinFunctionSUB_DECIMAL
    BEMulDecimal -> builtin P.BuiltinFunctionMUL_DECIMAL
    BEDivDecimal -> builtin P.BuiltinFunctionDIV_DECIMAL
    BERoundDecimal -> builtin P.BuiltinFunctionROUND_DECIMAL

    BEAddNumeric -> builtin P.BuiltinFunctionADD_NUMERIC
    BESubNumeric -> builtin P.BuiltinFunctionSUB_NUMERIC
    BEMulNumeric -> builtin P.BuiltinFunctionMUL_NUMERIC
    BEDivNumeric -> builtin P.BuiltinFunctionDIV_NUMERIC
    BERoundNumeric -> builtin P.BuiltinFunctionROUND_NUMERIC
    BECastNumeric -> builtin P.BuiltinFunctionCAST_NUMERIC
    BEShiftNumeric -> builtin P.BuiltinFunctionSHIFT_NUMERIC

    BEAddInt64 -> builtin P.BuiltinFunctionADD_INT64
    BESubInt64 -> builtin P.BuiltinFunctionSUB_INT64
    BEMulInt64 -> builtin P.BuiltinFunctionMUL_INT64
    BEDivInt64 -> builtin P.BuiltinFunctionDIV_INT64
    BEModInt64 -> builtin P.BuiltinFunctionMOD_INT64
    BEExpInt64 -> builtin P.BuiltinFunctionEXP_INT64

    BEInt64ToDecimal -> builtin P.BuiltinFunctionINT64_TO_DECIMAL
    BEDecimalToInt64 -> builtin P.BuiltinFunctionDECIMAL_TO_INT64

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

    BETextToUpper -> builtin P.BuiltinFunctionTEXT_TO_UPPER
    BETextToLower -> builtin P.BuiltinFunctionTEXT_TO_LOWER
    BETextSlice -> builtin P.BuiltinFunctionTEXT_SLICE
    BETextSliceIndex -> builtin P.BuiltinFunctionTEXT_SLICE_INDEX
    BETextContainsOnly -> builtin P.BuiltinFunctionTEXT_CONTAINS_ONLY
    BETextReplicate -> builtin P.BuiltinFunctionTEXT_REPLICATE
    BETextSplitOn -> builtin P.BuiltinFunctionTEXT_SPLIT_ON
    BETextIntercalate -> builtin P.BuiltinFunctionTEXT_INTERCALATE

    where
      builtin = pure . P.ExprSumBuiltin . P.Enumerated . Right
      lit = P.ExprSumPrimLit . P.PrimLit . Just
      pureLit = pure . lit

encodeExpr' :: Expr -> Encode P.Expr
encodeExpr' = \case
    EVar v -> expr . either P.ExprSumVarStr P.ExprSumVarInternedStr <$> encodeName' unExprVarName v
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
    UExercise{..} -> do
        update_ExerciseTemplate <- encodeQualTypeConName exeTemplate
        update_ExerciseChoice <- encodeName unChoiceName exeChoice
        update_ExerciseCid <- encodeExpr exeContractId
        update_ExerciseActor <- traverse encodeExpr' exeActors
        update_ExerciseArg <- encodeExpr exeArg
        pure $ P.UpdateSumExercise P.Update_Exercise{..}
    UFetch{..} -> do
        update_FetchTemplate <- encodeQualTypeConName fetTemplate
        update_FetchCid <- encodeExpr fetContractId
        pure $ P.UpdateSumFetch P.Update_Fetch{..}
    UGetTime -> pure $ P.UpdateSumGetTime P.Unit
    UEmbedExpr typ e -> do
        update_EmbedExprType <- encodeType typ
        update_EmbedExprBody <- encodeExpr e
        pure $ P.UpdateSumEmbedExpr P.Update_EmbedExpr{..}
    UFetchByKey rbk ->
        P.UpdateSumFetchByKey <$> encodeRetrieveByKey rbk
    ULookupByKey rbk ->
        P.UpdateSumLookupByKey <$> encodeRetrieveByKey rbk

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
            mangledAndInterned <- encodeNames (map unVariantConName cs)
            let (defDataType_EnumConstructorsConstructorsStr, defDataType_EnumConstructorsConstructorsInternedStr) = case mangledAndInterned of
                    Left mangled -> (V.fromList mangled, V.empty)
                    Right mangledIds -> (V.empty, V.fromList mangledIds)
            pure $ P.DefDataTypeDataConsEnum P.DefDataType_EnumConstructors{..}
    let defDataTypeSerializable = getIsSerializable dataSerializable
    defDataTypeLocation <- traverse encodeSourceLoc dataLocation
    pure P.DefDataType{..}

encodeDefValue :: DefValue -> Encode P.DefValue
encodeDefValue DefValue{..} = do
    (defValue_NameWithTypeNameDname, defValue_NameWithTypeNameInternedDname) <- encodeValueName (fst dvalBinder)
    defValue_NameWithTypeType <- encodeType (snd dvalBinder)
    let defValueNameWithType = Just P.DefValue_NameWithType{..}
    defValueExpr <- encodeExpr dvalBody
    let defValueNoPartyLiterals = getHasNoPartyLiterals dvalNoPartyLiterals
    let defValueIsTest = getIsTest dvalIsTest
    defValueLocation <- traverse encodeSourceLoc dvalLocation
    pure P.DefValue{..}

encodeTemplate :: Template -> Encode P.DefTemplate
encodeTemplate Template{..} = do
    defTemplateTycon <- encodeDottedName unTypeConName tplTypeCon
    defTemplateParam <- encodeName unExprVarName tplParam
    defTemplatePrecond <- encodeExpr tplPrecondition
    defTemplateSignatories <- encodeExpr tplSignatories
    defTemplateObservers <- encodeExpr tplObservers
    defTemplateAgreement <- encodeExpr tplAgreement
    defTemplateChoices <- encodeNameMap encodeTemplateChoice tplChoices
    defTemplateLocation <- traverse encodeSourceLoc tplLocation
    defTemplateKey <- traverse encodeTemplateKey tplKey
    pure P.DefTemplate{..}

encodeTemplateKey :: TemplateKey -> Encode P.DefTemplate_DefKey
encodeTemplateKey TemplateKey{..} = do
    defTemplate_DefKeyType <- encodeType tplKeyType
    defTemplate_DefKeyKeyExpr <-
        Just . P.DefTemplate_DefKeyKeyExprComplexKey <$> encodeExpr' tplKeyBody
    defTemplate_DefKeyMaintainers <- encodeExpr tplKeyMaintainers
    pure P.DefTemplate_DefKey{..}

encodeTemplateChoice :: TemplateChoice -> Encode P.TemplateChoice
encodeTemplateChoice TemplateChoice{..} = do
    templateChoiceName <- encodeName unChoiceName chcName
    let templateChoiceConsuming = chcConsuming
    templateChoiceControllers <- encodeExpr chcControllers
    templateChoiceSelfBinder <- encodeName unExprVarName chcSelfBinder
    templateChoiceArgBinder <- Just <$> encodeExprVarWithType chcArgBinder
    templateChoiceRetType <- encodeType chcReturnType
    templateChoiceUpdate <- encodeExpr chcUpdate
    templateChoiceLocation <- traverse encodeSourceLoc chcLocation
    pure P.TemplateChoice{..}

encodeFeatureFlags :: FeatureFlags -> Just P.FeatureFlags
encodeFeatureFlags FeatureFlags{..} = Just P.FeatureFlags
    { P.featureFlagsForbidPartyLiterals = forbidPartyLiterals
    -- We only support packages with these enabled -- see #157
    , P.featureFlagsDontDivulgeContractIdsInCreateArguments = True
    , P.featureFlagsDontDiscloseNonConsumingChoicesToObservers = True
    }

-- each scenario module is wrapped in a proto package
encodeScenarioModule :: Version -> Module -> P.Package
encodeScenarioModule version mod =
    encodePackage (Package version (NM.insert mod NM.empty) metadata)
  where
    metadata = getPackageMetadata version (PackageName "scenario") Nothing

encodeModule :: Module -> Encode P.Module
encodeModule Module{..} = do
    moduleName <- encodeDottedName unModuleName moduleName
    let moduleFlags = encodeFeatureFlags moduleFeatureFlags
    moduleSynonyms <- encodeNameMap encodeDefTypeSyn moduleSynonyms
    moduleDataTypes <- encodeNameMap encodeDefDataType moduleDataTypes
    moduleValues <- encodeNameMap encodeDefValue moduleValues
    moduleTemplates <- encodeNameMap encodeTemplate moduleTemplates
    pure P.Module{..}

encodePackageMetadata :: PackageMetadata -> Encode P.PackageMetadata
encodePackageMetadata PackageMetadata{..} = do
    packageMetadataNameInternedStr <- either (const $ error "Package name is always interned") id <$> encodeInternableString (unPackageName packageName)
    packageMetadataVersionInternedStr <- either (const $ error "Package name is always interned") id <$> encodeInternableString (unPackageVersion packageVersion)
    pure P.PackageMetadata{..}

-- | NOTE(MH): Assumes the DAML-LF version of the 'Package' is 'V1'.
encodePackage :: Package -> P.Package
encodePackage (Package version mods metadata) =
    let env = initEncodeEnv version (WithInterning True)
        ((packageModules, packageMetadata), EncodeEnv{internedStrings, internedDottedNames}) =
            runState ((,) <$> encodeNameMap encodeModule mods <*> traverse encodePackageMetadata metadata) env
        packageInternedStrings =
            V.fromList $ map (encodeString . fst) $ L.sortOn snd $ HMS.toList internedStrings
        packageInternedDottedNames =
            V.fromList $ map (P.InternedDottedName . V.fromList . fst) $ L.sortOn snd $ HMS.toList internedDottedNames
    in
    P.Package{..}

-- | NOTE(MH): This functions is used for sanity checking. The actual checks
-- are done in the conversion to DAML-LF.
_checkFeature :: Feature -> Version -> a -> a
_checkFeature feature version x
    | version `supports` feature = x
    | otherwise = error $ "DAML-LF " ++ renderPretty version ++ " cannot encode feature: " ++ T.unpack (featureName feature)
