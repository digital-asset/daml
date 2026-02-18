-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TemplateHaskell #-}
-- | Encoding of the LF package into LF version 1 format.
module DA.Daml.LF.Proto3.EncodeV2 (
  module DA.Daml.LF.Proto3.EncodeV2
) where

import           Control.Lens
import           Control.Lens.Ast (rightSpine)
import           Control.Monad.State.Strict
import           Control.Monad.Reader

import           Data.Coerce
import qualified Data.HashMap.Strict as HMS
import qualified Data.List as L
import qualified Data.List.NonEmpty as NE
import qualified Data.Set as S
import           Data.Maybe (fromMaybe, fromJust)
import qualified Data.NameMap as NM
import qualified Data.Text           as T
import qualified Data.Text.Lazy      as TL
import qualified Data.Vector         as V
import qualified Data.Map            as M
import           Data.Int

import           Text.Printf (printf)

import           DA.Pretty
import           DA.Daml.StablePackagesListReader
import           DA.Daml.LF.Ast
import           DA.Daml.LF.Mangling
import           DA.Daml.LF.Proto3.Interned    as I
import           DA.Daml.LF.Proto3.InternedMap
import           DA.Daml.LF.Proto3.InternedArr

import           DA.Daml.LF.Proto3.WellInterned

import qualified Com.Digitalasset.Daml.Lf.Archive.DamlLf2 as P

import qualified Proto3.Suite as P (Enumerated (..))

-- NOTE(MH): Type synonym for a `Maybe` that is always known to be a `Just`.
-- Some functions always return `Just x` instead of `x` since they would
-- otherwise always be wrapped in `Just` at their call sites.
type Just a = Maybe a

type InternedKindsMap = InternedMap P.KindSum
type InternedTypesMap = InternedMap P.TypeSum
type InternedExprsMap = InternedArr P.ExprSum

data EncodeState = EncodeState
    { internedStrings :: !(HMS.HashMap T.Text Int32)
    , nextInternedStringId :: !Int32
      -- ^ We track the size of `internedStrings` explicitly since `HMS.size` is `O(n)`.
    , internedDottedNames :: !(HMS.HashMap [Int32] Int32)
    , nextInternedDottedNameId :: !Int32
      -- ^ We track the size of `internedDottedNames` explicitly since `HMS.size` is `O(n)`.
    , internedKindsMap :: !InternedKindsMap
    , internedTypesMap :: !InternedTypesMap
    , internedExprsMap :: !InternedExprsMap
    }

makeLensesFor [ ("internedKindsMap", "internedKindsMapLens")
              , ("internedTypesMap", "internedTypesMapLens")
              , ("internedExprsMap", "internedExprsMapLens")] ''EncodeState

type ImportMap = M.Map PackageId Int32
data EncodeConfig = EncodeConfig
    { _version :: !Version
    , _importMap :: Either NoPkgImportsReasons ImportMap
    }
makeLenses ''EncodeConfig

type Encode a = ReaderT EncodeConfig (StateT EncodeState Identity) a

ifSupportsFlattening :: Encode a -> Encode a -> Encode a
ifSupportsFlattening = ifSupports version featureFlatArchive

ifSupportsFlattening_ :: a -> a -> Encode a
ifSupportsFlattening_ b1 b2 = ifSupports version featureFlatArchive (return b1) (return b2)

whenSupportsFlattening :: Encode ()
whenSupportsFlattening =
  whenSupports version featureFlatArchive $ \v ->
    error $ printf "assertion failiure: lf version %s does not support flattening" (show v)

initEncodeState :: EncodeState
initEncodeState =
    EncodeState
    { nextInternedStringId = 0
    , internedStrings = HMS.empty
    , internedDottedNames = HMS.empty
    , nextInternedDottedNameId = 0
    , internedKindsMap = I.empty
    , internedTypesMap = I.empty
    , internedExprsMap = I.empty
    , ..
    }

initTestEncodeConfig :: Version -> EncodeConfig
initTestEncodeConfig = flip EncodeConfig $ Left $ noPkgImportsReasonTesting "DA.Daml.LF.Proto3.EncodeV2:initTestEncodeConfig"

runEncode :: EncodeConfig           -- The read-only config.
          -> EncodeState            -- The initial state.
          -> Encode a               -- The computation to run.
          -> (a, EncodeState)       -- The final result and the final state.
runEncode config initialState computation = runIdentity $ runStateT (runReaderT computation config) initialState

-- | Find or allocate a string in the interning table. Return the index of
-- the string in the resulting interning table.
allocString :: T.Text -> Encode Int32
allocString t = do
    env@EncodeState{internedStrings, nextInternedStringId = n} <- get
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
    env@EncodeState{internedDottedNames, nextInternedDottedNameId = n} <- get
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
encodeDottedName :: (a -> [T.Text]) -> a -> Encode Int32
encodeDottedName unwrapDottedName (unwrapDottedName -> unmangled) =
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
encodeValueName :: ExprValName -> Encode Int32
encodeValueName valName = do
    encodeDottedName' [unExprValName valName]

-- | Encode a reference to a package. Package names are not mangled. Package
-- names are interned.
encodePackageId :: SelfOrImportedPackageId -> Encode (Just P.SelfOrImportedPackageId)
encodePackageId = fmap (Just . P.SelfOrImportedPackageId . Just) . go
  where
    go :: SelfOrImportedPackageId -> Encode P.SelfOrImportedPackageIdSum
    go = \case
      SelfPackageId ->
        pure $ P.SelfOrImportedPackageIdSumSelfPackageId P.Unit
      ImportedPackageId p@(PackageId pkgId) -> do
        (eMap :: Either NoPkgImportsReasons ImportMap) <- asks (view importMap)
        ifVersion version (\v -> p `notElem` allStablePackageIds  && v `supports` featurePackageImports)
          {-then-}
             (case eMap of
               Left r ->
                 error $ printf "Encountered an package ID of type ImportedPackage in a module that doesn't expose an import map, reason: %s, pkgId: %s" (show r) (show pkgId)
               Right ids ->
                 let (mID :: Maybe Int32) = M.lookup p ids
                 in  maybe (error $ printf "During encoding, did not find imported package id %s in import map %s" (show p) (show ids)) (return . P.SelfOrImportedPackageIdSumPackageImportId) mID)
          {-else-}
            (P.SelfOrImportedPackageIdSumImportedPackageIdInternedStr <$> allocString pkgId)

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

encodeQualTypeSynId' :: Qualified TypeSynName -> Encode P.TypeSynId
encodeQualTypeSynId' (Qualified pref mname syn) = do
    typeSynIdModule <- encodeModuleId pref mname
    typeSynIdNameInternedDname <- encodeDottedName unTypeSynName syn
    pure $ P.TypeSynId{..}

encodeQualTypeSynId :: Qualified TypeSynName -> Encode (Just P.TypeSynId)
encodeQualTypeSynId tysyn = Just <$> encodeQualTypeSynId' tysyn

encodeQualTypeConId' :: Qualified TypeConName -> Encode P.TypeConId
encodeQualTypeConId' (Qualified pref mname con) = do
    typeConIdModule <- encodeModuleId pref mname
    typeConIdNameInternedDname <- encodeDottedName unTypeConName con
    pure $ P.TypeConId{..}

encodeQualTypeConId :: Qualified TypeConName -> Encode (Just P.TypeConId)
encodeQualTypeConId tycon = Just <$> encodeQualTypeConId' tycon


encodeSourceLoc :: SourceLoc -> Encode P.Location
encodeSourceLoc SourceLoc{..} = do
    locationModule <- case slocModuleRef of
        Nothing -> pure Nothing
        Just (pkgRef, modName) -> encodeModuleId pkgRef modName
    let locationRange = Just $ P.Location_Range
            (fromIntegral slocStartLine)
            (fromIntegral slocStartCol)
            (fromIntegral slocEndLine)
            (fromIntegral slocEndCol)
    pure P.Location{..}

encodeModuleId :: SelfOrImportedPackageId -> ModuleName -> Encode (Just P.ModuleId)
encodeModuleId pkgRef modName = do
    moduleIdPackageId <- encodePackageId pkgRef
    moduleIdModuleNameInternedDname <- encodeDottedName unModuleName modName
    pure $ Just P.ModuleId{..}

encodeFieldsWithTypes :: (a -> T.Text) -> [(a, Type)] -> Encode (V.Vector P.FieldWithType)
encodeFieldsWithTypes unwrapName =
    encodeList $ \(name, typ) -> P.FieldWithType <$> encodeNameId unwrapName name <*> encodeType typ

encodeFieldsWithExprs :: (a -> T.Text) -> [(a, Expr)] -> Encode (V.Vector P.FieldWithExpr)
encodeFieldsWithExprs unwrapName =
    encodeList $ \(name, expr) -> P.FieldWithExpr <$> encodeNameId unwrapName name <*> encodeExpr expr

encodeTypeVarsWithKinds :: [(TypeVarName, Kind)] -> Encode (V.Vector P.TypeVarWithKind)
encodeTypeVarsWithKinds =
    encodeList $ \(name, kind) -> P.TypeVarWithKind <$> encodeNameId unTypeVarName name <*> (Just <$> encodeKind kind)

encodeExprVarWithType :: (ExprVarName, Type) -> Encode P.VarWithType
encodeExprVarWithType (name, typ) = do
    varWithTypeVarInternedStr <- encodeNameId unExprVarName name
    varWithTypeType <- encodeType typ
    pure P.VarWithType{..}


------------------------------------------------------------------------
-- Encoding of kinds
------------------------------------------------------------------------

encodeKind :: Kind -> Encode P.Kind
encodeKind k = do
  k' <- case k of
      KStar -> return $ (P.Kind . Just . P.KindSumStar) P.Unit
      KNat -> return $ (P.Kind . Just . P.KindSumNat) P.Unit
      (KArrow k1 k2) -> do
      (kind_ArrowParams :: V.Vector P.Kind) <- V.singleton <$> encodeKind k1
      (kind_ArrowResult :: Maybe P.Kind) <- Just <$> encodeKind k2
      return $ (P.Kind . Just . P.KindSumArrow) P.Kind_Arrow{..}
  internKind k'

internKind :: P.Kind -> Encode P.Kind
internKind k =
    ifSupportsFlattening
      {-then-}
        (case k of
            (P.Kind (Just k')) -> do
                n <- lift $ zoom internedKindsMapLens $ I.internState k'
                return $ (P.Kind . Just . P.KindSumInternedKind) n
            (P.Kind Nothing) -> error "nothing kind during encoding")
      {-then-}
        (return k)


------------------------------------------------------------------------
-- Encoding of types
------------------------------------------------------------------------

encodeBuiltinType :: BuiltinType -> P.Enumerated P.BuiltinType
encodeBuiltinType = P.Enumerated . Right . \case
    BTInt64 -> P.BuiltinTypeINT64
    BTText -> P.BuiltinTypeTEXT
    BTTimestamp -> P.BuiltinTypeTIMESTAMP
    BTParty -> P.BuiltinTypePARTY
    BTUnit -> P.BuiltinTypeUNIT
    BTBool -> P.BuiltinTypeBOOL
    BTList -> P.BuiltinTypeLIST
    BTUpdate -> P.BuiltinTypeUPDATE
    BTDate -> P.BuiltinTypeDATE
    BTContractId -> P.BuiltinTypeCONTRACT_ID
    BTOptional -> P.BuiltinTypeOPTIONAL
    BTTextMap -> P.BuiltinTypeTEXTMAP
    BTGenMap -> P.BuiltinTypeGENMAP
    BTArrow -> P.BuiltinTypeARROW
    BTNumeric -> P.BuiltinTypeNUMERIC
    BTAny -> P.BuiltinTypeANY
    BTTypeRep -> P.BuiltinTypeTYPE_REP
    BTRoundingMode -> P.BuiltinTypeROUNDING_MODE
    BTBigNumeric -> P.BuiltinTypeBIGNUMERIC
    BTAnyException -> P.BuiltinTypeANY_EXCEPTION
    BTFailureCategory -> P.BuiltinTypeFAILURE_CATEGORY

{- This code implements the local flattening, using leftSpine via _TApps. On lf
 versions that support a flat archive this is not needed anymore. To avoid
 duplicate logic, we hack the case statement, and simply give the entire
 expression as lhs and empty arg list as rhs. -}
encodeType' :: Type -> Encode P.Type
encodeType' typ = do
  pat <- ifSupportsFlattening_
    {-then-} (typ, [])
    {-else-} (typ ^. _TApps)
  ptyp <- case pat of
    (TVar var, args) -> do
        type_VarVarInternedStr <- encodeNameId unTypeVarName var
        type_VarArgs <- encodeList encodeType' args
        pure $ P.TypeSumVar P.Type_Var{..}
    (TCon con, args) -> do
        type_ConTycon <- encodeQualTypeConId con
        type_ConArgs <- encodeList encodeType' args
        pure $ P.TypeSumCon P.Type_Con{..}
    (TSynApp syn args, []) -> do
        type_SynTysyn <- encodeQualTypeSynId syn
        type_SynArgs <- encodeList encodeType' args
        pure $ P.TypeSumSyn P.Type_Syn{..}
    (TBuiltin bltn, args) -> do
        let type_BuiltinBuiltin = encodeBuiltinType bltn
        type_BuiltinArgs <- encodeList encodeType' args
        pure $ P.TypeSumBuiltin P.Type_Builtin{..}
    (t@(TForall bn bdy), []) -> do
        (binders, body) <- ifSupportsFlattening_
            {-then-} ([bn], bdy)
            {-else-} (t ^. _TForalls)
        type_ForallVars <- encodeTypeVarsWithKinds binders
        type_ForallBody <- encodeType body
        pure $ P.TypeSumForall P.Type_Forall{..}
    (TStruct flds, []) -> do
        type_StructFields <- encodeFieldsWithTypes unFieldName flds
        pure $ P.TypeSumStruct P.Type_Struct{..}

    (TNat n, _) ->
        pure $ P.TypeSumNat (fromTypeLevelNat n)

    (TApp lhs rhs, args) -> do
      unless (null args) $ error "Arguments set on TApp"
      whenSupportsFlattening
      type_TAppLhs <- encodeType lhs
      type_TAppRhs <- encodeType rhs
      pure $ P.TypeSumTapp P.Type_TApp{..}

    (TStruct{}, _:_) -> error "Application of TStruct"
    -- NOTE(MH): The following case requires impredicative polymorphism,
    -- which we don't support.
    (TForall{}, _:_) -> error "Application of TForall"
    (TSynApp{}, _:_) -> error "Application of TSynApp"
  allocType ptyp

encodeType :: Type -> Encode (Just P.Type)
encodeType = (Just <$>) . encodeType'

allocType :: P.TypeSum -> Encode P.Type
allocType = internType . P.Type . Just

internType :: P.Type -> Encode P.Type
internType = \case
  (P.Type (Just t')) -> do
    n <- lift $ zoom internedTypesMapLens $ I.internState t'
    return $ (P.Type . Just . P.TypeSumInternedType) n
  (P.Type Nothing) -> error "nothing type during encoding"

------------------------------------------------------------------------
-- Encoding of expressions
------------------------------------------------------------------------

encodeTypeConApp :: TypeConApp -> Encode (Just P.Type_Con)
encodeTypeConApp (TypeConApp tycon args) = do
    type_ConTycon <- encodeQualTypeConId tycon
    type_ConArgs <- encodeList encodeType' args
    pure $ Just P.Type_Con{..}

encodeBuiltinExpr :: BuiltinExpr -> Encode P.ExprSum
encodeBuiltinExpr = \case
    BEInt64 x -> pureLit $ P.BuiltinLitSumInt64 x
    BENumeric num ->
        lit . P.BuiltinLitSumNumericInternedStr <$> allocString (T.pack (show num))
    BEText x ->
        lit . P.BuiltinLitSumTextInternedStr <$> encodeInternableString x
    BETimestamp x -> pureLit $ P.BuiltinLitSumTimestamp x
    BEDate x -> pureLit $ P.BuiltinLitSumDate x

    BEUnit -> pure $ P.ExprSumBuiltinCon $ P.Enumerated $ Right P.BuiltinConCON_UNIT
    BEBool b -> pure $ P.ExprSumBuiltinCon $ P.Enumerated $ Right $ case b of
        False -> P.BuiltinConCON_FALSE
        True -> P.BuiltinConCON_TRUE

    BERoundingMode r -> case r of
      LitRoundingUp -> pureLit $ P.BuiltinLitSumRoundingMode $ P.Enumerated $ Right P.BuiltinLit_RoundingModeUP
      LitRoundingDown -> pureLit $ P.BuiltinLitSumRoundingMode $ P.Enumerated $ Right P.BuiltinLit_RoundingModeDOWN
      LitRoundingCeiling -> pureLit $ P.BuiltinLitSumRoundingMode $ P.Enumerated $ Right P.BuiltinLit_RoundingModeCEILING
      LitRoundingFloor -> pureLit $ P.BuiltinLitSumRoundingMode $ P.Enumerated $ Right P.BuiltinLit_RoundingModeFLOOR
      LitRoundingHalfUp -> pureLit $ P.BuiltinLitSumRoundingMode $ P.Enumerated $ Right P.BuiltinLit_RoundingModeHALF_UP
      LitRoundingHalfDown -> pureLit $ P.BuiltinLitSumRoundingMode $ P.Enumerated $ Right P.BuiltinLit_RoundingModeHALF_DOWN
      LitRoundingHalfEven -> pureLit $ P.BuiltinLitSumRoundingMode $ P.Enumerated $ Right P.BuiltinLit_RoundingModeHALF_EVEN
      LitRoundingUnnecessary -> pureLit $ P.BuiltinLitSumRoundingMode $ P.Enumerated $ Right P.BuiltinLit_RoundingModeUNNECESSARY

    BEFailureCategory fc -> case fc of
      LitInvalidIndependentOfSystemState -> pureLit $ P.BuiltinLitSumFailureCategory $ P.Enumerated $ Right P.BuiltinLit_FailureCategoryINVALID_INDEPENDENT_OF_SYSTEM_STATE
      LitInvalidGivenCurrentSystemStateOther -> pureLit $ P.BuiltinLitSumFailureCategory $ P.Enumerated $ Right P.BuiltinLit_FailureCategoryINVALID_GIVEN_CURRENT_SYSTEM_STATE_OTHER

    BEEqual -> builtin P.BuiltinFunctionEQUAL
    BELess -> builtin P.BuiltinFunctionLESS
    BELessEq -> builtin P.BuiltinFunctionLESS_EQ
    BEGreater -> builtin P.BuiltinFunctionGREATER
    BEGreaterEq -> builtin P.BuiltinFunctionGREATER_EQ

    BEToText typ -> case typ of
      BTInt64 -> builtin P.BuiltinFunctionINT64_TO_TEXT
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
    BETextToNumeric -> builtin P.BuiltinFunctionTEXT_TO_NUMERIC
    BETextToCodePoints -> builtin P.BuiltinFunctionTEXT_TO_CODE_POINTS

    BEAddNumeric -> builtin P.BuiltinFunctionADD_NUMERIC
    BESubNumeric -> builtin P.BuiltinFunctionSUB_NUMERIC
    BEMulNumeric -> builtin P.BuiltinFunctionMUL_NUMERIC
    BEDivNumeric -> builtin P.BuiltinFunctionDIV_NUMERIC
    BERoundNumeric -> builtin P.BuiltinFunctionROUND_NUMERIC
    BECastNumeric -> builtin P.BuiltinFunctionCAST_NUMERIC
    BEShiftNumeric -> builtin P.BuiltinFunctionSHIFT_NUMERIC

    BEScaleBigNumeric -> builtin P.BuiltinFunctionSCALE_BIGNUMERIC
    BEPrecisionBigNumeric -> builtin P.BuiltinFunctionPRECISION_BIGNUMERIC
    BEAddBigNumeric -> builtin P.BuiltinFunctionADD_BIGNUMERIC
    BESubBigNumeric -> builtin P.BuiltinFunctionSUB_BIGNUMERIC
    BEMulBigNumeric -> builtin P.BuiltinFunctionMUL_BIGNUMERIC
    BEDivBigNumeric -> builtin P.BuiltinFunctionDIV_BIGNUMERIC
    BEShiftRightBigNumeric -> builtin P.BuiltinFunctionSHIFT_RIGHT_BIGNUMERIC
    BEBigNumericToNumeric -> builtin P.BuiltinFunctionBIGNUMERIC_TO_NUMERIC
    BENumericToBigNumeric -> builtin P.BuiltinFunctionNUMERIC_TO_BIGNUMERIC

    BEAddInt64 -> builtin P.BuiltinFunctionADD_INT64
    BESubInt64 -> builtin P.BuiltinFunctionSUB_INT64
    BEMulInt64 -> builtin P.BuiltinFunctionMUL_INT64
    BEDivInt64 -> builtin P.BuiltinFunctionDIV_INT64
    BEModInt64 -> builtin P.BuiltinFunctionMOD_INT64
    BEExpInt64 -> builtin P.BuiltinFunctionEXP_INT64

    BEInt64ToNumeric -> builtin P.BuiltinFunctionINT64_TO_NUMERIC
    BENumericToInt64 -> builtin P.BuiltinFunctionNUMERIC_TO_INT64

    BEFoldl -> builtin P.BuiltinFunctionFOLDL
    BEFoldr -> builtin P.BuiltinFunctionFOLDR
    BEEqualList -> builtin P.BuiltinFunctionEQUAL_LIST
    BEExplodeText -> builtin P.BuiltinFunctionEXPLODE_TEXT
    BEAppendText -> builtin P.BuiltinFunctionAPPEND_TEXT
    BEImplodeText -> builtin P.BuiltinFunctionIMPLODE_TEXT
    BESha256Text -> builtin P.BuiltinFunctionSHA256_TEXT
    BESha256Hex -> builtin P.BuiltinFunctionSHA256_HEX
    BEKecCak256Text -> builtin P.BuiltinFunctionKECCAK256_TEXT
    BEEncodeHex -> builtin P.BuiltinFunctionTEXT_TO_HEX
    BEDecodeHex -> builtin P.BuiltinFunctionHEX_TO_TEXT
    BESecp256k1Bool -> builtin P.BuiltinFunctionSECP256K1_BOOL
    BESecp256k1WithEcdsaBool -> builtin P.BuiltinFunctionSECP256K1_WITH_ECDSA_BOOL
    BESecp256k1ValidateKey -> builtin P.BuiltinFunctionSECP256K1_VALIDATE_KEY

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
    BECoerceContractId -> builtin P.BuiltinFunctionCOERCE_CONTRACT_ID

    BETypeRepTyConName -> builtin P.BuiltinFunctionTYPE_REP_TYCON_NAME

    BEFailWithStatus -> builtin P.BuiltinFunctionFAIL_WITH_STATUS

    where
      builtin = pure . P.ExprSumBuiltin . P.Enumerated . Right
      lit = P.ExprSumBuiltinLit . P.BuiltinLit . Just
      pureLit = pure . lit

encodeExpr' :: Expr -> Encode P.Expr
encodeExpr' e = case e of
  -- we must process locations first, since they are not interned separatately
  -- (because in proto, locations are no longer explicit ast nodes, but rather
  -- optional attributes of Expr constructors)
  ELocation loc e -> do
      P.Expr{..} <- encodeExpr' e
      exprLocation <- Just <$> encodeSourceLoc loc
      pure P.Expr{..}
  --else if expr is not an location:
  _ -> internExpr $ case e of
    EVar v -> expr . P.ExprSumVarInternedStr <$> encodeNameId unExprVarName v
    EVal (Qualified pkgRef modName val) -> do
        valueIdModule <- encodeModuleId pkgRef modName
        valueIdNameInternedDname <- encodeValueName val
        pureExpr $ P.ExprSumVal P.ValueId{..}
    EBuiltinFun bi -> expr <$> encodeBuiltinExpr bi
    ERecCon{..} -> do
        expr_RecConTycon <- encodeTypeConApp recTypeCon
        expr_RecConFields <- encodeFieldsWithExprs unFieldName recFields
        pureExpr $ P.ExprSumRecCon P.Expr_RecCon{..}
    ERecProj{..} -> do
        expr_RecProjTycon <- encodeTypeConApp recTypeCon
        expr_RecProjFieldInternedStr <- encodeNameId unFieldName recField
        expr_RecProjRecord <- encodeExpr recExpr
        pureExpr $ P.ExprSumRecProj P.Expr_RecProj{..}
    ERecUpd{..} -> do
        expr_RecUpdTycon <- encodeTypeConApp recTypeCon
        expr_RecUpdFieldInternedStr <- encodeNameId unFieldName recField
        expr_RecUpdRecord <- encodeExpr recExpr
        expr_RecUpdUpdate <- encodeExpr recUpdate
        pureExpr $ P.ExprSumRecUpd P.Expr_RecUpd{..}
    EVariantCon{..} -> do
        expr_VariantConTycon <- encodeTypeConApp varTypeCon
        expr_VariantConVariantConInternedStr <- encodeNameId unVariantConName varVariant
        expr_VariantConVariantArg <- encodeExpr varArg
        pureExpr $ P.ExprSumVariantCon P.Expr_VariantCon{..}
    EEnumCon{..} -> do
        expr_EnumConTycon <- encodeQualTypeConId enumTypeCon
        expr_EnumConEnumConInternedStr <- encodeNameId unVariantConName enumDataCon
        pureExpr $ P.ExprSumEnumCon P.Expr_EnumCon{..}
    EStructCon{..} -> do
        expr_StructConFields <- encodeFieldsWithExprs unFieldName structFields
        pureExpr $ P.ExprSumStructCon P.Expr_StructCon{..}
    EStructProj{..} -> do
        expr_StructProjFieldInternedStr <- encodeNameId unFieldName structField
        expr_StructProjStruct <- encodeExpr structExpr
        pureExpr $ P.ExprSumStructProj P.Expr_StructProj{..}
    EStructUpd{..} -> do
        expr_StructUpdFieldInternedStr <- encodeNameId unFieldName structField
        expr_StructUpdStruct <- encodeExpr structExpr
        expr_StructUpdUpdate <- encodeExpr structUpdate
        pureExpr $ P.ExprSumStructUpd P.Expr_StructUpd{..}
    e@(ETmApp e1 e2) -> do
        (fun, args) <- ifSupportsFlattening_
            {-then-} (e1, [e2])
            {-else-} (e ^. _ETmApps)
        expr_AppFun <- encodeExpr fun
        expr_AppArgs <- encodeList encodeExpr' args
        pureExpr $ P.ExprSumApp P.Expr_App{..}
    e@(ETyApp inner t) -> do
        (fun, args) <- ifSupportsFlattening_
            {-then-} (inner, [t])
            {-else-} (e ^. _ETyApps)
        expr_TyAppExpr <- encodeExpr fun
        expr_TyAppTypes <- encodeList encodeType' args
        pureExpr $ P.ExprSumTyApp P.Expr_TyApp{..}
    e@(ETmLam bnd bdy) -> do
        (params, body) <- ifSupportsFlattening_
            {-then-} ([bnd], bdy)
            {-else-} (e ^. _ETmLams)
        expr_AbsParam <- encodeList encodeExprVarWithType params
        expr_AbsBody <- encodeExpr body
        pureExpr $ P.ExprSumAbs P.Expr_Abs{..}
    e@(ETyLam bnd bdy) -> do
        (params, body) <- ifSupportsFlattening_
            {-then-} ([bnd], bdy)
            {-else-} (e ^. _ETyLams)
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
    ENone typ -> do
        expr_OptionalNoneType <- encodeType typ
        pureExpr $ P.ExprSumOptionalNone P.Expr_OptionalNone{..}
    ESome typ body -> do
        expr_OptionalSomeType <- encodeType typ
        expr_OptionalSomeValue <- encodeExpr body
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
        expr_ToInterfaceInterfaceType <- encodeQualTypeConId ty1
        expr_ToInterfaceTemplateType <- encodeQualTypeConId ty2
        expr_ToInterfaceTemplateExpr <- encodeExpr val
        pureExpr $ P.ExprSumToInterface P.Expr_ToInterface{..}
    EFromInterface ty1 ty2 val -> do
        expr_FromInterfaceInterfaceType <- encodeQualTypeConId ty1
        expr_FromInterfaceTemplateType <- encodeQualTypeConId ty2
        expr_FromInterfaceInterfaceExpr <- encodeExpr val
        pureExpr $ P.ExprSumFromInterface P.Expr_FromInterface{..}
    EUnsafeFromInterface ty1 ty2 cid val -> do
        expr_UnsafeFromInterfaceInterfaceType <- encodeQualTypeConId ty1
        expr_UnsafeFromInterfaceTemplateType <- encodeQualTypeConId ty2
        expr_UnsafeFromInterfaceContractIdExpr <- encodeExpr cid
        expr_UnsafeFromInterfaceInterfaceExpr <- encodeExpr val
        pureExpr $ P.ExprSumUnsafeFromInterface P.Expr_UnsafeFromInterface{..}
    ECallInterface ty mth val -> do
        expr_CallInterfaceInterfaceType <- encodeQualTypeConId ty
        expr_CallInterfaceMethodInternedName <- encodeMethodName mth
        expr_CallInterfaceInterfaceExpr <- encodeExpr val
        pureExpr $ P.ExprSumCallInterface P.Expr_CallInterface{..}
    EToRequiredInterface ty1 ty2 val -> do
        expr_ToRequiredInterfaceRequiredInterface <- encodeQualTypeConId ty1
        expr_ToRequiredInterfaceRequiringInterface <- encodeQualTypeConId ty2
        expr_ToRequiredInterfaceExpr <- encodeExpr val
        pureExpr $ P.ExprSumToRequiredInterface P.Expr_ToRequiredInterface{..}
    EFromRequiredInterface ty1 ty2 val -> do
        expr_FromRequiredInterfaceRequiredInterface <- encodeQualTypeConId ty1
        expr_FromRequiredInterfaceRequiringInterface <- encodeQualTypeConId ty2
        expr_FromRequiredInterfaceExpr <- encodeExpr val
        pureExpr $ P.ExprSumFromRequiredInterface P.Expr_FromRequiredInterface{..}
    EUnsafeFromRequiredInterface ty1 ty2 cid val -> do
        expr_UnsafeFromRequiredInterfaceRequiredInterface <- encodeQualTypeConId ty1
        expr_UnsafeFromRequiredInterfaceRequiringInterface <- encodeQualTypeConId ty2
        expr_UnsafeFromRequiredInterfaceContractIdExpr <- encodeExpr cid
        expr_UnsafeFromRequiredInterfaceInterfaceExpr <- encodeExpr val
        pureExpr $ P.ExprSumUnsafeFromRequiredInterface P.Expr_UnsafeFromRequiredInterface{..}
    EInterfaceTemplateTypeRep ty val -> do
        expr_InterfaceTemplateTypeRepInterface <- encodeQualTypeConId ty
        expr_InterfaceTemplateTypeRepExpr <- encodeExpr val
        pureExpr $ P.ExprSumInterfaceTemplateTypeRep P.Expr_InterfaceTemplateTypeRep{..}
    ESignatoryInterface ty val -> do
        expr_SignatoryInterfaceInterface <- encodeQualTypeConId ty
        expr_SignatoryInterfaceExpr <- encodeExpr val
        pureExpr $ P.ExprSumSignatoryInterface P.Expr_SignatoryInterface{..}
    EObserverInterface ty val -> do
        expr_ObserverInterfaceInterface <- encodeQualTypeConId ty
        expr_ObserverInterfaceExpr <- encodeExpr val
        pureExpr $ P.ExprSumObserverInterface P.Expr_ObserverInterface{..}
    EViewInterface iface expr -> do
        expr_ViewInterfaceInterface <- encodeQualTypeConId iface
        expr_ViewInterfaceExpr <- encodeExpr expr
        pureExpr $ P.ExprSumViewInterface P.Expr_ViewInterface{..}
    EChoiceController tpl ch expr1 expr2 -> do
        expr_ChoiceControllerTemplate <- encodeQualTypeConId tpl
        expr_ChoiceControllerChoiceInternedStr <- encodeNameId unChoiceName ch
        expr_ChoiceControllerContractExpr <- encodeExpr expr1
        expr_ChoiceControllerChoiceArgExpr <- encodeExpr expr2
        pureExpr $ P.ExprSumChoiceController P.Expr_ChoiceController{..}
    EChoiceObserver tpl ch expr1 expr2 -> do
        expr_ChoiceObserverTemplate <- encodeQualTypeConId tpl
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

internExpr :: Encode P.Expr -> Encode P.Expr
internExpr f = do
  e <- f
  ifSupportsFlattening
    {-then-}
      (case e of
        (P.Expr _ (Just (P.ExprSumInternedExpr _))) ->
            error "not allowed to add interned to interning table"
        (P.Expr l (Just e')) -> do
            n <- lift $ zoom internedExprsMapLens $ I.internState e'
            return $ (P.Expr l . Just . P.ExprSumInternedExpr) n
        (P.Expr _ Nothing) -> error "nothing expr during encoding")
    {-else-}
      (return e)

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
        update_CreateTemplate <- encodeQualTypeConId creTemplate
        update_CreateExpr <- encodeExpr creArg
        pure $ P.UpdateSumCreate P.Update_Create{..}
    UCreateInterface{..} -> do
        update_CreateInterfaceInterface <- encodeQualTypeConId creInterface
        update_CreateInterfaceExpr <- encodeExpr creArg
        pure $ P.UpdateSumCreateInterface P.Update_CreateInterface{..}
    UExercise{..} -> do
        update_ExerciseTemplate <- encodeQualTypeConId exeTemplate
        update_ExerciseChoiceInternedStr <- encodeNameId unChoiceName exeChoice
        update_ExerciseCid <- encodeExpr exeContractId
        update_ExerciseArg <- encodeExpr exeArg
        pure $ P.UpdateSumExercise P.Update_Exercise{..}
    UExerciseInterface{..} -> do
        update_ExerciseInterfaceInterface <- encodeQualTypeConId exeInterface
        update_ExerciseInterfaceChoiceInternedStr <- encodeNameId unChoiceName exeChoice
        update_ExerciseInterfaceCid <- encodeExpr exeContractId
        update_ExerciseInterfaceArg <- encodeExpr exeArg
        update_ExerciseInterfaceGuard <- traverse encodeExpr' exeGuard
        pure $ P.UpdateSumExerciseInterface P.Update_ExerciseInterface{..}
    UExerciseByKey{..} -> do
        update_ExerciseByKeyTemplate <- encodeQualTypeConId exeTemplate
        update_ExerciseByKeyChoiceInternedStr <- encodeNameId unChoiceName exeChoice
        update_ExerciseByKeyKey <- encodeExpr exeKey
        update_ExerciseByKeyArg <- encodeExpr exeArg
        pure $ P.UpdateSumExerciseByKey P.Update_ExerciseByKey{..}
    UFetch{..} -> do
        update_FetchTemplate <- encodeQualTypeConId fetTemplate
        update_FetchCid <- encodeExpr fetContractId
        pure $ P.UpdateSumFetch P.Update_Fetch{..}
    UFetchInterface{..} -> do
        update_FetchInterfaceInterface <- encodeQualTypeConId fetInterface
        update_FetchInterfaceCid <- encodeExpr fetContractId
        pure $ P.UpdateSumFetchInterface P.Update_FetchInterface{..}
    UGetTime -> pure $ P.UpdateSumGetTime P.Unit
    ULedgerTimeLT e -> do
        update_LedgerTimeLt <- encodeExpr e
        pure $ P.UpdateSumLedgerTimeLt (fromJust update_LedgerTimeLt)
    UEmbedExpr typ e -> do
        update_EmbedExprType <- encodeType typ
        update_EmbedExprBody <- encodeExpr e
        pure $ P.UpdateSumEmbedExpr P.Update_EmbedExpr{..}
    UFetchByKey tmplId ->
        P.UpdateSumFetchByKey <$> encodeRetrieveByKey tmplId
    ULookupByKey tmplId ->
        P.UpdateSumLookupByKey <$> encodeRetrieveByKey tmplId
    UQueryNByKey tmplId -> do
        update_QueryNByKeyTemplate <- encodeQualTypeConId tmplId
        pure $ P.UpdateSumQueryNByKey $ P.Update_QueryNByKey{..}
    UTryCatch{..} -> do
        update_TryCatchReturnType <- encodeType tryCatchType
        update_TryCatchTryExpr <- encodeExpr tryCatchExpr
        update_TryCatchVarInternedStr <- encodeNameId unExprVarName tryCatchVar
        update_TryCatchCatchExpr <- encodeExpr tryCatchHandler
        pure $ P.UpdateSumTryCatch P.Update_TryCatch{..}

encodeRetrieveByKey :: Qualified TypeConName -> Encode P.Update_RetrieveByKey
encodeRetrieveByKey tmplId = do
    update_RetrieveByKeyTemplate <- encodeQualTypeConId tmplId
    pure P.Update_RetrieveByKey{..}

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
            caseAlt_VariantCon <- encodeQualTypeConId patTypeCon
            caseAlt_VariantVariantInternedStr <- encodeNameId unVariantConName patVariant
            caseAlt_VariantBinderInternedStr <- encodeNameId unExprVarName patBinder
            pure $ P.CaseAltSumVariant P.CaseAlt_Variant{..}
        CPEnum{..} -> do
            caseAlt_EnumCon <- encodeQualTypeConId patTypeCon
            caseAlt_EnumConstructorInternedStr <- encodeNameId unVariantConName patDataCon
            pure $ P.CaseAltSumEnum P.CaseAlt_Enum{..}
        CPUnit -> pure $ P.CaseAltSumBuiltinCon $ P.Enumerated $ Right P.BuiltinConCON_UNIT
        CPBool b -> pure $ P.CaseAltSumBuiltinCon $ P.Enumerated $ Right $ case b of
            False -> P.BuiltinConCON_FALSE
            True -> P.BuiltinConCON_TRUE
        CPNil -> pure $ P.CaseAltSumNil P.Unit
        CPCons{..} -> do
            caseAlt_ConsVarHeadInternedStr <- encodeNameId unExprVarName patHeadBinder
            caseAlt_ConsVarTailInternedStr <- encodeNameId unExprVarName patTailBinder
            pure $ P.CaseAltSumCons P.CaseAlt_Cons{..}
        CPNone -> pure $ P.CaseAltSumOptionalNone P.Unit
        CPSome{..} -> do
            caseAlt_OptionalSomeVarBodyInternedStr <- encodeNameId unExprVarName patBodyBinder
            pure $ P.CaseAltSumOptionalSome P.CaseAlt_OptionalSome{..}
    caseAltBody <- encodeExpr altExpr
    pure P.CaseAlt{..}

encodeDefTypeSyn :: DefTypeSyn -> Encode P.DefTypeSyn
encodeDefTypeSyn DefTypeSyn{..} = do
    defTypeSynNameInternedDname <- encodeDottedName unTypeSynName synName
    defTypeSynParams <- encodeTypeVarsWithKinds synParams
    defTypeSynType <- encodeType synType
    defTypeSynLocation <- traverse encodeSourceLoc synLocation
    pure P.DefTypeSyn{..}

encodeDefDataType :: DefDataType -> Encode P.DefDataType
encodeDefDataType DefDataType{..} = do
    defDataTypeNameInternedDname <- encodeDottedName unTypeConName dataTypeCon
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
            let defDataType_EnumConstructorsConstructorsInternedStr = V.fromList mangledIds
            pure $ P.DefDataTypeDataConsEnum P.DefDataType_EnumConstructors{..}
        DataInterface -> pure $ P.DefDataTypeDataConsInterface P.Unit
    let defDataTypeSerializable = getIsSerializable dataSerializable
    defDataTypeLocation <- traverse encodeSourceLoc dataLocation
    pure P.DefDataType{..}

encodeDefValue :: DefValue -> Encode P.DefValue
encodeDefValue DefValue{..} = do
    defValue_NameWithTypeNameInternedDname <- encodeValueName (fst dvalBinder)
    defValue_NameWithTypeType <- encodeType (snd dvalBinder)
    let defValueNameWithType = Just P.DefValue_NameWithType{..}
    defValueExpr <- encodeExpr dvalBody
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
    defTemplateTyconInternedDname <- encodeDottedName unTypeConName tplTypeCon
    defTemplateParamInternedStr <- encodeNameId unExprVarName tplParam
    defTemplatePrecond <- encodeExpr tplPrecondition
    defTemplateSignatories <- encodeExpr tplSignatories
    defTemplateObservers <- encodeExpr tplObservers
    defTemplateChoices <- encodeNameMap encodeTemplateChoice tplChoices
    defTemplateLocation <- traverse encodeSourceLoc tplLocation
    defTemplateKey <- traverse encodeTemplateKey tplKey
    defTemplateImplements <- encodeNameMap encodeTemplateImplements tplImplements
    pure P.DefTemplate{..}

encodeTemplateImplements :: TemplateImplements -> Encode P.DefTemplate_Implements
encodeTemplateImplements TemplateImplements{..} = do
    defTemplate_ImplementsInterface <- encodeQualTypeConId tpiInterface
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
        Just <$> encodeExpr' tplKeyBody
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
    templateChoiceNameInternedStr <- encodeNameId unChoiceName chcName
    let templateChoiceConsuming = chcConsuming
    templateChoiceControllers <- encodeExpr chcControllers
    templateChoiceObservers <- encodeChoiceObservers chcObservers
    templateChoiceAuthorizers <- encodeChoiceAuthorizers chcAuthorizers
    templateChoiceSelfBinderInternedStr <- encodeNameId unExprVarName chcSelfBinder
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

-- each script module is wrapped in a proto package
encodeSinglePackageModule :: Version -> ModuleWithImports -> P.Package
encodeSinglePackageModule version (mod, imports) =
    encodePackage (Package version (NM.insert mod NM.empty) metadata imports)
  where
    metadata = PackageMetadata
      { packageName = PackageName "single-module-package"
      , packageVersion = PackageVersion "0.0.0"
      , upgradedPackageId = Nothing
      }

encodeModule :: Module -> Encode P.Module
encodeModule Module{..} = do
    moduleNameInternedDname <- encodeDottedName unModuleName moduleName
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
    defInterfaceRequires <- encodeSet encodeQualTypeConId' intRequires
    defInterfaceMethods <- encodeNameMap encodeInterfaceMethod intMethods
    defInterfaceParamInternedStr <- encodeNameId unExprVarName intParam
    defInterfaceChoices <- encodeNameMap encodeTemplateChoice intChoices
    defInterfaceView <- encodeType intView
    pure $ P.DefInterface{..}

encodeInterfaceMethod :: InterfaceMethod -> Encode P.InterfaceMethod
encodeInterfaceMethod InterfaceMethod {..} = do
    interfaceMethodLocation <- traverse encodeSourceLoc ifmLocation
    interfaceMethodMethodInternedName <- encodeMethodName ifmName
    interfaceMethodType <- encodeType ifmType
    pure $ P.InterfaceMethod{..}

encodeUpgradedPackageId :: UpgradedPackageId -> Encode P.UpgradedPackageId
encodeUpgradedPackageId upgradedPackageId = do
  upgradedPackageIdUpgradedPackageIdInternedStr <- encodeInternableString (unPackageId (unUpgradedPackageId upgradedPackageId))
  pure P.UpgradedPackageId{..}

encodePackageMetadata :: PackageMetadata -> Encode P.PackageMetadata
encodePackageMetadata PackageMetadata{..} = do
    packageMetadataNameInternedStr <- encodeInternableString (unPackageName packageName)
    packageMetadataVersionInternedStr <- encodeInternableString (unPackageVersion packageVersion)
    packageMetadataUpgradedPackageId <- traverse encodeUpgradedPackageId upgradedPackageId
    pure P.PackageMetadata{..}

packInternedStrings :: HMS.HashMap T.Text Int32 -> V.Vector TL.Text
packInternedStrings =
  V.fromList . map (encodeString . fst) . L.sortOn snd . HMS.toList

packInternedKinds :: InternedKindsMap -> V.Vector P.Kind
packInternedKinds = V.map (P.Kind . Just) . I.toVec

packInternedTypes :: InternedTypesMap -> V.Vector P.Type
packInternedTypes = V.map (P.Type . Just) . I.toVec

packInternedExprs :: InternedExprsMap -> V.Vector P.Expr
packInternedExprs = V.map (P.Expr Nothing . Just) . I.toVec

encodeImports :: [PackageId] -> P.PackageImportsSum
encodeImports = P.PackageImportsSumPackageImports . P.PackageImports . V.fromList . map toTlText
  where
    toTlText :: PackageId -> TL.Text
    toTlText = TL.fromStrict . unPackageId

encodeReasons :: NoPkgImportsReasons -> Maybe P.PackageImportsSum
encodeReasons rsns =
  case NE.toList $ unNoPkgImportsReasons rsns of
    [StablePackage] -> Nothing
    lursns -> Just $ P.PackageImportsSumNoImportedPackagesReason $ TL.pack $ show lursns

-- | This may be the casue of (future) round-trip test errors: we discard the
-- reasons here. If we rtt packages that set a reason for unsupported lf and
-- there are errors, the fix is to never set the field in the first place.
encodePkgImports :: Version -> Either NoPkgImportsReasons [PackageId] -> Maybe P.PackageImportsSum
encodePkgImports version importList =
  if version `supports` featurePackageImports
    then either encodeReasons (Just . encodeImports) importList
    else Nothing

mkImportMap :: [PackageId] -> ImportMap
mkImportMap = M.fromList . flip zip [0..]

encodePackage :: Package -> P.Package
encodePackage (Package version mods metadata imports) =
-- Whenever we fix the order of the set of package imports, we always sort. This
-- way, every list in the encode/decode chain (... -> set -> list -> set -> list
-- -> ...) will always be in the same order
    let importList :: Either NoPkgImportsReasons [PackageId]
        importList = L.sort . S.toList <$> imports
        st = initEncodeState
        conf = EncodeConfig version $ mkImportMap <$> importList
        ( (packageModules, packageMetadata),
          EncodeState{internedStrings, internedDottedNames, internedKindsMap, internedTypesMap, internedExprsMap}) =
            runEncode conf st ((,) <$> encodeNameMap encodeModule mods <*> fmap Just (encodePackageMetadata metadata))
        packageInternedStrings = packInternedStrings internedStrings
        packageInternedDottedNames =
            V.fromList $ map (P.InternedDottedName . V.fromList . fst) $ L.sortOn snd $ HMS.toList internedDottedNames
        packageInternedKinds = packInternedKinds internedKindsMap
        packageInternedTypes = packInternedTypes internedTypesMap
        packageInternedExprs = packInternedExprs internedExprsMap
        packageImportsSum = encodePkgImports version importList
        package = P.Package{..}
    in
      case packageWellInterned package of
        Left msg | version `supports` featureFlatArchive -> error msg
        _ -> package

-- | NOTE(MH): This functions is used for sanity checking. The actual checks
-- are done in the conversion to Daml-LF.
_checkFeature :: Feature -> Version -> a -> a
_checkFeature feature version x
    | version `supports` feature = x
    | otherwise = error $ "Daml-LF " ++ renderPretty version ++ " cannot encode feature: " ++ T.unpack (featureName feature)
