-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE TypeFamilies #-}

module DA.Daml.LF.Proto3.DecodeV1
    ( decodePackage
    , Error(..)
    ) where

import           DA.Daml.LF.Ast as LF
import           DA.Daml.LF.Proto3.Error
import qualified DA.Daml.LF.Proto3.Util as Util
import Control.Monad
import Control.Monad.Except
import Control.Monad.Reader
import Data.Int
import Text.Read
import           Data.List
import           DA.Daml.LF.Mangling
import qualified Da.DamlLf1 as LF1
import qualified Data.NameMap as NM
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import qualified Data.Vector as V
import qualified Proto3.Suite as Proto


data DecodeEnv = DecodeEnv
    { internedStrings :: !(V.Vector T.Text)
    , internedDottedNames :: !(V.Vector [T.Text])
    }

newtype Decode a = Decode{unDecode :: ReaderT DecodeEnv (Except Error) a}
    deriving (Functor, Applicative, Monad, MonadError Error, MonadReader DecodeEnv)

runDecode :: DecodeEnv -> Decode a -> Either Error a
runDecode env act = runExcept $ runReaderT (unDecode act) env

lookupInterned :: V.Vector a -> (Int32 -> Error) -> Int32 -> Decode a
lookupInterned interned mkError id = do
    case interned V.!? fromIntegral id of
          Nothing -> throwError $ mkError id
          Just x -> pure x

lookupString :: Int32 -> Decode T.Text
lookupString strId = do
    DecodeEnv{internedStrings} <- ask
    lookupInterned internedStrings BadStringId strId

lookupDottedName :: Int32 -> Decode [T.Text]
lookupDottedName id = do
    DecodeEnv{internedDottedNames} <- ask
    lookupInterned internedDottedNames BadDottedNameId id

------------------------------------------------------------------------
-- Decodings of things related to string interning
------------------------------------------------------------------------

-- | Decode of a string that cannot be interned, e.g, the entries of the
-- interning table itself.
decodeString :: TL.Text -> T.Text
decodeString = TL.toStrict

-- | Decode a string that will be interned in DAML-LF 1.7 and onwards.
-- At the protobuf level, we represent internable non-empty lists of strings
-- by a repeatable string and a number. If there's at least one string,
-- then the number must not be set, i.e. zero. If there are no strings,
-- then the number is treated as an index into the interning table.
decodeInternableStrings :: V.Vector TL.Text -> Int32 -> Decode [T.Text]
decodeInternableStrings strs id
    | V.null strs = lookupDottedName id
    | id == 0 = pure $ map decodeString (V.toList strs)
    | otherwise = throwError $ ParseError "items and interned id both set for string list"

-- | Decode the name of a syntactic object, e.g., a variable or a data
-- constructor. These strings are mangled to escape special characters. All
-- names will be interned in DAML-LF 1.7 and onwards.
decodeName
    :: Util.EitherLike TL.Text Int32 e
    => (T.Text -> a) -> Maybe e -> Decode a
decodeName wrapName mbStrOrId = mayDecode "name" mbStrOrId $ \strOrId -> do
    mangled <- case Util.toEither strOrId of
        Left str -> pure $ decodeString str
        Right strId -> lookupString strId
    decodeNameString wrapName mangled

decodeNameString :: (T.Text -> a) -> T.Text -> Decode a
decodeNameString wrapName mangled =
    case unmangleIdentifier mangled of
        Left err -> throwError $ ParseError $ "Could not unmangle name " ++ show mangled ++ ": " ++ err
        Right unmangled -> pure $ wrapName unmangled

-- | Decode the multi-component name of a syntactic object, e.g., a type
-- constructor. All compononents are mangled. Dotted names will be interned
-- in DAML-LF 1.7 and onwards.
decodeDottedName :: ([T.Text] -> a) -> LF1.DottedName -> Decode a
decodeDottedName wrapDottedName (LF1.DottedName mangled dnId) =
    wrapDottedName <$> (decodeInternableStrings mangled dnId >>= mapM (decodeNameString id))

-- | Decode the name of a top-level value. The name is mangled and will be
-- interned in DAML-LF 1.7 and onwards.
decodeValueName :: String -> V.Vector TL.Text -> Int32 -> Decode ExprValName
decodeValueName ident mangledV dnId = do
    mangled <- decodeInternableStrings mangledV dnId
    case mangled of
        [] -> throwError $ MissingField ident
        [unmangled] -> do
            mangled <- decodeNameString id unmangled
            case unmangleIdentifier mangled of
                Right unmangled -> pure $ ExprValName unmangled
                -- NOTE(MH): This is an ugly hack to keep backwards compatibility.
                -- We need to fix this in DAML-LF 2.
                Left _ -> pure $ ExprValName mangled
        _ -> throwError $ ParseError $ "Unexpected multi-segment def name: " ++ show mangledV ++ "//" ++ show mangled

-- | Decode a reference to a top-level value. The name is mangled and will be
-- interned in DAML-LF 1.7 and onwards.
decodeValName :: LF1.ValName -> Decode (Qualified ExprValName)
decodeValName LF1.ValName{..} = do
  (pref, mname) <- mayDecode "valNameModule" valNameModule decodeModuleRef
  name <- decodeValueName "valNameName" valNameName valNameNameInternedId
  pure $ Qualified pref mname name

-- | Decode a reference to a package. Package names are not mangled. Package
-- name are interned since DAML-LF 1.6.
decodePackageRef :: LF1.PackageRef -> Decode PackageRef
decodePackageRef (LF1.PackageRef pref) =
    mayDecode "packageRefSum" pref $ \case
        LF1.PackageRefSumSelf _ -> pure PRSelf
        LF1.PackageRefSumPackageId pkgId -> pure $ PRImport $ PackageId $ decodeString pkgId
        LF1.PackageRefSumInternedId strId -> PRImport . PackageId <$> lookupString strId

------------------------------------------------------------------------
-- Decodings of everything else
------------------------------------------------------------------------

decodeVersion :: T.Text -> Either Error Version
decodeVersion minorText = do
  let unsupported :: Either Error a
      unsupported = throwError (UnsupportedMinorVersion minorText)
  -- we translate "no version" to minor version 0, since we introduced
  -- minor versions once DAML-LF v1 was already out, and we want to be
  -- able to parse packages that were compiled before minor versions
  -- were a thing. DO NOT replicate this code bejond major version 1!
  minor <- if
    | T.null minorText -> pure $ LF.PointStable 0
    | Just minor <- LF.parseMinorVersion (T.unpack minorText) -> pure minor
    | otherwise -> unsupported
  let version = V1 minor
  if version `elem` LF.supportedInputVersions then pure version else unsupported

decodeInternedDottedName :: LF1.InternedDottedName -> Decode [T.Text]
decodeInternedDottedName (LF1.InternedDottedName ids) =
    mapM lookupString $ V.toList ids

decodePackage :: TL.Text -> LF1.Package -> Either Error Package
decodePackage minorText (LF1.Package mods internedStringsV internedDottedNamesV) = do
  version <- decodeVersion (decodeString minorText)
  let internedStrings = V.map decodeString internedStringsV
  let internedDottedNames = V.empty
  let env0 = DecodeEnv{..}
  internedDottedNames <- runDecode env0 $ mapM decodeInternedDottedName internedDottedNamesV
  let env = DecodeEnv{..}
  runDecode env $ do
    Package version <$> decodeNM DuplicateModule decodeModule mods

decodeModule :: LF1.Module -> Decode Module
decodeModule (LF1.Module name flags dataTypes values templates) =
  Module
    <$> mayDecode "moduleName" name (decodeDottedName ModuleName)
    <*> pure Nothing
    <*> mayDecode "flags" flags decodeFeatureFlags
    <*> decodeNM DuplicateDataType decodeDefDataType dataTypes
    <*> decodeNM DuplicateValue decodeDefValue values
    <*> decodeNM EDuplicateTemplate decodeDefTemplate templates

decodeFeatureFlags :: LF1.FeatureFlags -> Decode FeatureFlags
decodeFeatureFlags LF1.FeatureFlags{..} =
  if not featureFlagsDontDivulgeContractIdsInCreateArguments || not featureFlagsDontDiscloseNonConsumingChoicesToObservers
    -- We do not support these anymore -- see #157
    then throwError (ParseError "Package uses unsupported flags dontDivulgeContractIdsInCreateArguments or dontDiscloseNonConsumingChoicesToObservers")
    else pure FeatureFlags
      { forbidPartyLiterals = featureFlagsForbidPartyLiterals
      }

decodeDefDataType :: LF1.DefDataType -> Decode DefDataType
decodeDefDataType LF1.DefDataType{..} =
  DefDataType
    <$> traverse decodeLocation defDataTypeLocation
    <*> mayDecode "dataTypeName" defDataTypeName (decodeDottedName TypeConName)
    <*> pure (IsSerializable defDataTypeSerializable)
    <*> traverse decodeTypeVarWithKind (V.toList defDataTypeParams)
    <*> mayDecode "dataTypeDataCons" defDataTypeDataCons decodeDataCons

decodeDataCons :: LF1.DefDataTypeDataCons -> Decode DataCons
decodeDataCons = \case
  LF1.DefDataTypeDataConsRecord (LF1.DefDataType_Fields fs) ->
    DataRecord <$> mapM (decodeFieldWithType FieldName) (V.toList fs)
  LF1.DefDataTypeDataConsVariant (LF1.DefDataType_Fields fs) ->
    DataVariant <$> mapM (decodeFieldWithType VariantConName) (V.toList fs)
  LF1.DefDataTypeDataConsEnum (LF1.DefDataType_EnumConstructors cs cIds) -> do
    mangled <- if
      | V.null cIds -> pure $ map decodeString (V.toList cs)
      | V.null cs -> mapM lookupString (V.toList cIds)
      | otherwise -> throwError $ ParseError "strings and interned string ids both set for enum constructor"
    DataEnum <$> mapM (decodeNameString VariantConName) mangled

decodeDefValueNameWithType :: LF1.DefValue_NameWithType -> Decode (ExprValName, Type)
decodeDefValueNameWithType LF1.DefValue_NameWithType{..} = (,)
  <$> decodeValueName "defValueName" defValue_NameWithTypeName defValue_NameWithTypeNameInternedId
  <*> mayDecode "defValueType" defValue_NameWithTypeType decodeType

decodeDefValue :: LF1.DefValue -> Decode DefValue
decodeDefValue (LF1.DefValue mbBinder mbBody noParties isTest mbLoc) =
  DefValue
    <$> traverse decodeLocation mbLoc
    <*> mayDecode "defValueName" mbBinder decodeDefValueNameWithType
    <*> pure (HasNoPartyLiterals noParties)
    <*> pure (IsTest isTest)
    <*> mayDecode "defValueExpr" mbBody decodeExpr

decodeDefTemplate :: LF1.DefTemplate -> Decode Template
decodeDefTemplate LF1.DefTemplate{..} = do
  tplParam <- decodeName ExprVarName defTemplateParam
  Template
    <$> traverse decodeLocation defTemplateLocation
    <*> mayDecode "defTemplateTycon" defTemplateTycon (decodeDottedName TypeConName)
    <*> pure tplParam
    <*> mayDecode "defTemplatePrecond" defTemplatePrecond decodeExpr
    <*> mayDecode "defTemplateSignatories" defTemplateSignatories decodeExpr
    <*> mayDecode "defTemplateObservers" defTemplateObservers decodeExpr
    <*> mayDecode "defTemplateAgreement" defTemplateAgreement decodeExpr
    <*> decodeNM DuplicateChoice decodeChoice defTemplateChoices
    <*> mapM (decodeDefTemplateKey tplParam) defTemplateKey


decodeDefTemplateKey :: ExprVarName -> LF1.DefTemplate_DefKey -> Decode TemplateKey
decodeDefTemplateKey templateParam LF1.DefTemplate_DefKey{..} = do
  typ <- mayDecode "defTemplate_DefKeyType" defTemplate_DefKeyType decodeType
  key <- mayDecode "defTemplate_DefKeyKeyExpr" defTemplate_DefKeyKeyExpr (decodeKeyExpr templateParam)
  maintainers <- mayDecode "defTemplate_DefKeyMaintainers" defTemplate_DefKeyMaintainers decodeExpr
  return (TemplateKey typ key maintainers)

decodeKeyExpr :: ExprVarName -> LF1.DefTemplate_DefKeyKeyExpr -> Decode Expr
decodeKeyExpr templateParam = \case
    LF1.DefTemplate_DefKeyKeyExprKey simpleKeyExpr ->
        decodeSimpleKeyExpr templateParam simpleKeyExpr
    LF1.DefTemplate_DefKeyKeyExprComplexKey keyExpr ->
        decodeExpr keyExpr

decodeSimpleKeyExpr :: ExprVarName -> LF1.KeyExpr -> Decode Expr
decodeSimpleKeyExpr templateParam LF1.KeyExpr{..} = mayDecode "keyExprSum" keyExprSum $ \case
  LF1.KeyExprSumProjections LF1.KeyExpr_Projections{..} ->
    foldM
      (\rec_ LF1.KeyExpr_Projection{..} ->
        ERecProj
          <$> mayDecode "KeyExpr_ProjectionTyCon" keyExpr_ProjectionTycon decodeTypeConApp
          <*> decodeName FieldName keyExpr_ProjectionField
          <*> pure rec_)
      (EVar templateParam) keyExpr_ProjectionsProjections
  LF1.KeyExprSumRecord LF1.KeyExpr_Record{..} ->
    ERecCon
      <$> mayDecode "keyExpr_RecordTycon" keyExpr_RecordTycon decodeTypeConApp
      <*> mapM (decodeFieldWithSimpleKeyExpr templateParam) (V.toList keyExpr_RecordFields)

decodeFieldWithSimpleKeyExpr :: ExprVarName -> LF1.KeyExpr_RecordField -> Decode (FieldName, Expr)
decodeFieldWithSimpleKeyExpr templateParam LF1.KeyExpr_RecordField{..} =
  (,)
  <$> decodeName FieldName keyExpr_RecordFieldField
  <*> mayDecode "keyExpr_RecordFieldExpr" keyExpr_RecordFieldExpr (decodeSimpleKeyExpr templateParam)

decodeChoice :: LF1.TemplateChoice -> Decode TemplateChoice
decodeChoice LF1.TemplateChoice{..} =
  TemplateChoice
    <$> traverse decodeLocation templateChoiceLocation
    <*> decodeName ChoiceName templateChoiceName
    <*> pure templateChoiceConsuming
    <*> mayDecode "templateChoiceControllers" templateChoiceControllers decodeExpr
    <*> decodeName ExprVarName templateChoiceSelfBinder
    <*> mayDecode "templateChoiceArgBinder" templateChoiceArgBinder decodeVarWithType
    <*> mayDecode "templateChoiceRetType" templateChoiceRetType decodeType
    <*> mayDecode "templateChoiceUpdate" templateChoiceUpdate decodeExpr

decodeBuiltinFunction :: LF1.BuiltinFunction -> Decode BuiltinExpr
decodeBuiltinFunction = pure . \case
  LF1.BuiltinFunctionEQUAL_INT64 -> BEEqual BTInt64
  LF1.BuiltinFunctionEQUAL_DECIMAL -> BEEqual BTDecimal
  LF1.BuiltinFunctionEQUAL_NUMERIC -> BEEqualNumeric
  LF1.BuiltinFunctionEQUAL_TEXT -> BEEqual BTText
  LF1.BuiltinFunctionEQUAL_TIMESTAMP -> BEEqual BTTimestamp
  LF1.BuiltinFunctionEQUAL_DATE -> BEEqual BTDate
  LF1.BuiltinFunctionEQUAL_PARTY -> BEEqual BTParty
  LF1.BuiltinFunctionEQUAL_BOOL -> BEEqual BTBool

  LF1.BuiltinFunctionLEQ_INT64 -> BELessEq BTInt64
  LF1.BuiltinFunctionLEQ_DECIMAL -> BELessEq BTDecimal
  LF1.BuiltinFunctionLEQ_NUMERIC -> BELessEqNumeric
  LF1.BuiltinFunctionLEQ_TEXT -> BELessEq BTText
  LF1.BuiltinFunctionLEQ_TIMESTAMP -> BELessEq BTTimestamp
  LF1.BuiltinFunctionLEQ_DATE -> BELessEq BTDate
  LF1.BuiltinFunctionLEQ_PARTY -> BELessEq BTParty

  LF1.BuiltinFunctionLESS_INT64 -> BELess BTInt64
  LF1.BuiltinFunctionLESS_DECIMAL -> BELess BTDecimal
  LF1.BuiltinFunctionLESS_NUMERIC -> BELessNumeric
  LF1.BuiltinFunctionLESS_TEXT -> BELess BTText
  LF1.BuiltinFunctionLESS_TIMESTAMP -> BELess BTTimestamp
  LF1.BuiltinFunctionLESS_DATE -> BELess BTDate
  LF1.BuiltinFunctionLESS_PARTY -> BELess BTParty

  LF1.BuiltinFunctionGEQ_INT64 -> BEGreaterEq BTInt64
  LF1.BuiltinFunctionGEQ_DECIMAL -> BEGreaterEq BTDecimal
  LF1.BuiltinFunctionGEQ_NUMERIC -> BEGreaterEqNumeric
  LF1.BuiltinFunctionGEQ_TEXT -> BEGreaterEq BTText
  LF1.BuiltinFunctionGEQ_TIMESTAMP -> BEGreaterEq BTTimestamp
  LF1.BuiltinFunctionGEQ_DATE -> BEGreaterEq BTDate
  LF1.BuiltinFunctionGEQ_PARTY -> BEGreaterEq BTParty

  LF1.BuiltinFunctionGREATER_INT64 -> BEGreater BTInt64
  LF1.BuiltinFunctionGREATER_DECIMAL -> BEGreater BTDecimal
  LF1.BuiltinFunctionGREATER_NUMERIC -> BEGreaterNumeric
  LF1.BuiltinFunctionGREATER_TEXT -> BEGreater BTText
  LF1.BuiltinFunctionGREATER_TIMESTAMP -> BEGreater BTTimestamp
  LF1.BuiltinFunctionGREATER_DATE -> BEGreater BTDate
  LF1.BuiltinFunctionGREATER_PARTY -> BEGreater BTParty

  LF1.BuiltinFunctionTO_TEXT_INT64 -> BEToText BTInt64
  LF1.BuiltinFunctionTO_TEXT_DECIMAL -> BEToText BTDecimal
  LF1.BuiltinFunctionTO_TEXT_NUMERIC -> BEToTextNumeric
  LF1.BuiltinFunctionTO_TEXT_TEXT -> BEToText BTText
  LF1.BuiltinFunctionTO_TEXT_TIMESTAMP -> BEToText BTTimestamp
  LF1.BuiltinFunctionTO_TEXT_PARTY -> BEToText BTParty
  LF1.BuiltinFunctionTO_TEXT_DATE -> BEToText BTDate
  LF1.BuiltinFunctionTEXT_FROM_CODE_POINTS -> BETextFromCodePoints
  LF1.BuiltinFunctionFROM_TEXT_PARTY -> BEPartyFromText
  LF1.BuiltinFunctionFROM_TEXT_INT64 -> BEInt64FromText
  LF1.BuiltinFunctionFROM_TEXT_DECIMAL -> BEDecimalFromText
  LF1.BuiltinFunctionFROM_TEXT_NUMERIC -> BENumericFromText
  LF1.BuiltinFunctionTEXT_TO_CODE_POINTS -> BETextToCodePoints
  LF1.BuiltinFunctionTO_QUOTED_TEXT_PARTY -> BEPartyToQuotedText

  LF1.BuiltinFunctionADD_DECIMAL   -> BEAddDecimal
  LF1.BuiltinFunctionSUB_DECIMAL   -> BESubDecimal
  LF1.BuiltinFunctionMUL_DECIMAL   -> BEMulDecimal
  LF1.BuiltinFunctionDIV_DECIMAL   -> BEDivDecimal
  LF1.BuiltinFunctionROUND_DECIMAL -> BERoundDecimal
  LF1.BuiltinFunctionADD_NUMERIC   -> BEAddNumeric
  LF1.BuiltinFunctionSUB_NUMERIC   -> BESubNumeric
  LF1.BuiltinFunctionMUL_NUMERIC   -> BEMulNumeric
  LF1.BuiltinFunctionDIV_NUMERIC   -> BEDivNumeric
  LF1.BuiltinFunctionROUND_NUMERIC -> BERoundNumeric
  LF1.BuiltinFunctionCAST_NUMERIC  -> BECastNumeric
  LF1.BuiltinFunctionSHIFT_NUMERIC -> BEShiftNumeric

  LF1.BuiltinFunctionADD_INT64 -> BEAddInt64
  LF1.BuiltinFunctionSUB_INT64 -> BESubInt64
  LF1.BuiltinFunctionMUL_INT64 -> BEMulInt64
  LF1.BuiltinFunctionDIV_INT64 -> BEDivInt64
  LF1.BuiltinFunctionMOD_INT64 -> BEModInt64
  LF1.BuiltinFunctionEXP_INT64 -> BEExpInt64

  LF1.BuiltinFunctionFOLDL          -> BEFoldl
  LF1.BuiltinFunctionFOLDR          -> BEFoldr
  LF1.BuiltinFunctionEQUAL_LIST     -> BEEqualList
  LF1.BuiltinFunctionAPPEND_TEXT    -> BEAppendText
  LF1.BuiltinFunctionERROR          -> BEError

  LF1.BuiltinFunctionMAP_EMPTY      -> BEMapEmpty
  LF1.BuiltinFunctionMAP_INSERT     -> BEMapInsert
  LF1.BuiltinFunctionMAP_LOOKUP     -> BEMapLookup
  LF1.BuiltinFunctionMAP_DELETE     -> BEMapDelete
  LF1.BuiltinFunctionMAP_TO_LIST    -> BEMapToList
  LF1.BuiltinFunctionMAP_SIZE       -> BEMapSize

  LF1.BuiltinFunctionEXPLODE_TEXT -> BEExplodeText
  LF1.BuiltinFunctionIMPLODE_TEXT -> BEImplodeText
  LF1.BuiltinFunctionSHA256_TEXT  -> BESha256Text

  LF1.BuiltinFunctionDATE_TO_UNIX_DAYS -> BEDateToUnixDays
  LF1.BuiltinFunctionUNIX_DAYS_TO_DATE -> BEUnixDaysToDate
  LF1.BuiltinFunctionTIMESTAMP_TO_UNIX_MICROSECONDS -> BETimestampToUnixMicroseconds
  LF1.BuiltinFunctionUNIX_MICROSECONDS_TO_TIMESTAMP -> BEUnixMicrosecondsToTimestamp

  LF1.BuiltinFunctionINT64_TO_DECIMAL -> BEInt64ToDecimal
  LF1.BuiltinFunctionDECIMAL_TO_INT64 -> BEDecimalToInt64
  LF1.BuiltinFunctionINT64_TO_NUMERIC -> BEInt64ToNumeric
  LF1.BuiltinFunctionNUMERIC_TO_INT64 -> BENumericToInt64

  LF1.BuiltinFunctionTRACE -> BETrace
  LF1.BuiltinFunctionEQUAL_CONTRACT_ID -> BEEqualContractId
  LF1.BuiltinFunctionCOERCE_CONTRACT_ID -> BECoerceContractId

decodeLocation :: LF1.Location -> Decode SourceLoc
decodeLocation (LF1.Location mbModRef mbRange) = do
  mbModRef' <- traverse decodeModuleRef mbModRef
  LF1.Location_Range sline scol eline ecol <- mayDecode "Location_Range" mbRange pure
  pure $ SourceLoc
    mbModRef'
    (fromIntegral sline) (fromIntegral scol)
    (fromIntegral eline) (fromIntegral ecol)

decodeExpr :: LF1.Expr -> Decode Expr
decodeExpr (LF1.Expr mbLoc exprSum) = case mbLoc of
  Nothing -> decodeExprSum exprSum
  Just loc -> ELocation <$> decodeLocation loc <*> decodeExprSum exprSum

decodeExprSum :: Maybe LF1.ExprSum -> Decode Expr
decodeExprSum exprSum = mayDecode "exprSum" exprSum $ \case
  LF1.ExprSumVar var -> EVar <$> decodeNameString ExprVarName (decodeString var)
  LF1.ExprSumVarInternedId strId -> EVar <$> (lookupString strId >>= decodeNameString ExprVarName)
  LF1.ExprSumVal val -> EVal <$> decodeValName val
  LF1.ExprSumBuiltin (Proto.Enumerated (Right bi)) -> EBuiltin <$> decodeBuiltinFunction bi
  LF1.ExprSumBuiltin (Proto.Enumerated (Left num)) -> throwError (UnknownEnum "ExprSumBuiltin" num)
  LF1.ExprSumPrimCon (Proto.Enumerated (Right con)) -> pure $ EBuiltin $ case con of
    LF1.PrimConCON_UNIT -> BEUnit
    LF1.PrimConCON_TRUE -> BEBool True
    LF1.PrimConCON_FALSE -> BEBool False

  LF1.ExprSumPrimCon (Proto.Enumerated (Left num)) -> throwError (UnknownEnum "ExprSumPrimCon" num)
  LF1.ExprSumPrimLit lit ->
    EBuiltin <$> decodePrimLit lit
  LF1.ExprSumRecCon (LF1.Expr_RecCon mbTycon fields) ->
    ERecCon
      <$> mayDecode "Expr_RecConTycon" mbTycon decodeTypeConApp
      <*> mapM decodeFieldWithExpr (V.toList fields)
  LF1.ExprSumRecProj (LF1.Expr_RecProj mbTycon field mbRecord) ->
    ERecProj
      <$> mayDecode "Expr_RecProjTycon" mbTycon decodeTypeConApp
      <*> decodeName FieldName field
      <*> mayDecode "Expr_RecProjRecord" mbRecord decodeExpr
  LF1.ExprSumRecUpd (LF1.Expr_RecUpd mbTycon field mbRecord mbUpdate) ->
    ERecUpd
      <$> mayDecode "Expr_RecUpdTycon" mbTycon decodeTypeConApp
      <*> decodeName FieldName field
      <*> mayDecode "Expr_RecUpdRecord" mbRecord decodeExpr
      <*> mayDecode "Expr_RecUpdUpdate" mbUpdate decodeExpr
  LF1.ExprSumVariantCon (LF1.Expr_VariantCon mbTycon variant mbArg) ->
    EVariantCon
      <$> mayDecode "Expr_VariantConTycon" mbTycon decodeTypeConApp
      <*> decodeName VariantConName variant
      <*> mayDecode "Expr_VariantConVariantArg" mbArg decodeExpr
  LF1.ExprSumEnumCon (LF1.Expr_EnumCon mbTypeCon dataCon) ->
    EEnumCon
      <$> mayDecode "Expr_EnumConTycon" mbTypeCon decodeTypeConName
      <*> decodeName VariantConName dataCon
  LF1.ExprSumTupleCon (LF1.Expr_TupleCon fields) ->
    ETupleCon
      <$> mapM decodeFieldWithExpr (V.toList fields)
  LF1.ExprSumTupleProj (LF1.Expr_TupleProj field mbTuple) ->
    ETupleProj
      <$> decodeName FieldName field
      <*> mayDecode "Expr_TupleProjTuple" mbTuple decodeExpr
  LF1.ExprSumTupleUpd (LF1.Expr_TupleUpd field mbTuple mbUpdate) ->
    ETupleUpd
      <$> decodeName FieldName field
      <*> mayDecode "Expr_TupleUpdTuple" mbTuple decodeExpr
      <*> mayDecode "Expr_TupleUpdUpdate" mbUpdate decodeExpr
  LF1.ExprSumApp (LF1.Expr_App mbFun args) -> do
    fun <- mayDecode "Expr_AppFun" mbFun decodeExpr
    foldl' ETmApp fun <$> mapM decodeExpr (V.toList args)
  LF1.ExprSumTyApp (LF1.Expr_TyApp mbFun args) -> do
    fun <- mayDecode "Expr_TyAppFun" mbFun decodeExpr
    foldl' ETyApp fun <$> mapM decodeType (V.toList args)
  LF1.ExprSumAbs (LF1.Expr_Abs params mbBody) -> do
    body <- mayDecode "Expr_AbsBody" mbBody decodeExpr
    foldr ETmLam body <$> mapM decodeVarWithType (V.toList params)
  LF1.ExprSumTyAbs (LF1.Expr_TyAbs params mbBody) -> do
    body <- mayDecode "Expr_TyAbsBody" mbBody decodeExpr
    foldr ETyLam body <$> traverse decodeTypeVarWithKind (V.toList params)
  LF1.ExprSumCase (LF1.Case mbScrut alts) ->
    ECase
      <$> mayDecode "Case_caseScrut" mbScrut decodeExpr
      <*> mapM decodeCaseAlt (V.toList alts)
  LF1.ExprSumLet (LF1.Block lets mbBody) -> do
    body <- mayDecode "blockBody" mbBody decodeExpr
    foldr ELet body <$> mapM decodeBinding (V.toList lets)
  LF1.ExprSumNil (LF1.Expr_Nil mbType) ->
    ENil <$> mayDecode "expr_NilType" mbType decodeType
  LF1.ExprSumCons (LF1.Expr_Cons mbType front mbTail) -> do
    ctype <- mayDecode "expr_ConsType" mbType decodeType
    ctail <- mayDecode "expr_ConsTail" mbTail decodeExpr
    foldr (ECons ctype) ctail <$> mapM decodeExpr (V.toList front)
  LF1.ExprSumUpdate upd ->
    decodeUpdate upd
  LF1.ExprSumScenario scen ->
    decodeScenario scen
  LF1.ExprSumOptionalNone (LF1.Expr_OptionalNone mbType) -> do
    bodyType <- mayDecode "expr_OptionalNoneType" mbType decodeType
    return (ENone bodyType)
  LF1.ExprSumOptionalSome (LF1.Expr_OptionalSome mbType mbBody) -> do
    bodyType <- mayDecode "expr_OptionalSomeType" mbType decodeType
    bodyExpr <- mayDecode "expr_OptionalSomeBody" mbBody decodeExpr
    return (ESome bodyType bodyExpr)
  LF1.ExprSumToAny (LF1.Expr_ToAny mbType mbExpr) -> do
    type' <- mayDecode "expr_ToAnyType" mbType decodeType
    case type' of
        TCon con -> do
            expr <- mayDecode "expr_ToAnyExpr" mbExpr decodeExpr
            return (EToAnyTemplate con expr)
        _ -> throwError (ExpectedTCon type')
  LF1.ExprSumFromAny (LF1.Expr_FromAny mbType mbExpr) -> do
    type' <- mayDecode "expr_FromAnyType" mbType decodeType
    case type' of
        TCon con -> do
            expr <- mayDecode "expr_FromAnyExpr" mbExpr decodeExpr
            return (EFromAnyTemplate con expr)
        _ -> throwError (ExpectedTCon type')

decodeUpdate :: LF1.Update -> Decode Expr
decodeUpdate LF1.Update{..} = mayDecode "updateSum" updateSum $ \case
  LF1.UpdateSumPure (LF1.Pure mbType mbExpr) ->
    fmap EUpdate $ UPure
      <$> mayDecode "pureType" mbType decodeType
      <*> mayDecode "pureExpr" mbExpr decodeExpr
  LF1.UpdateSumBlock (LF1.Block binds mbBody) -> do
    body <- mayDecode "blockBody" mbBody decodeExpr
    foldr (\b e -> EUpdate $ UBind b e) body <$> mapM decodeBinding (V.toList binds)
  LF1.UpdateSumCreate (LF1.Update_Create mbTycon mbExpr) ->
    fmap EUpdate $ UCreate
      <$> mayDecode "update_CreateTemplate" mbTycon decodeTypeConName
      <*> mayDecode "update_CreateExpr" mbExpr decodeExpr
  LF1.UpdateSumExercise LF1.Update_Exercise{..} ->
    fmap EUpdate $ UExercise
      <$> mayDecode "update_ExerciseTemplate" update_ExerciseTemplate decodeTypeConName
      <*> decodeName ChoiceName update_ExerciseChoice
      <*> mayDecode "update_ExerciseCid" update_ExerciseCid decodeExpr
      <*> traverse decodeExpr update_ExerciseActor
      <*> mayDecode "update_ExerciseArg" update_ExerciseArg decodeExpr
  LF1.UpdateSumFetch LF1.Update_Fetch{..} ->
    fmap EUpdate $ UFetch
      <$> mayDecode "update_FetchTemplate" update_FetchTemplate decodeTypeConName
      <*> mayDecode "update_FetchCid" update_FetchCid decodeExpr
  LF1.UpdateSumGetTime LF1.Unit ->
    pure (EUpdate UGetTime)
  LF1.UpdateSumEmbedExpr LF1.Update_EmbedExpr{..} ->
    fmap EUpdate $ UEmbedExpr
      <$> mayDecode "update_EmbedExprType" update_EmbedExprType decodeType
      <*> mayDecode "update_EmbedExprBody" update_EmbedExprBody decodeExpr
  LF1.UpdateSumLookupByKey retrieveByKey ->
    fmap (EUpdate . ULookupByKey) (decodeRetrieveByKey retrieveByKey)
  LF1.UpdateSumFetchByKey retrieveByKey ->
    fmap (EUpdate . UFetchByKey) (decodeRetrieveByKey retrieveByKey)

decodeRetrieveByKey :: LF1.Update_RetrieveByKey -> Decode RetrieveByKey
decodeRetrieveByKey LF1.Update_RetrieveByKey{..} = RetrieveByKey
  <$> mayDecode "update_RetrieveByKeyTemplate" update_RetrieveByKeyTemplate decodeTypeConName
  <*> mayDecode "update_RetrieveByKeyKey" update_RetrieveByKeyKey decodeExpr

decodeScenario :: LF1.Scenario -> Decode Expr
decodeScenario LF1.Scenario{..} = mayDecode "scenarioSum" scenarioSum $ \case
  LF1.ScenarioSumPure (LF1.Pure mbType mbExpr) ->
    fmap EScenario $ SPure
      <$> mayDecode "pureType" mbType decodeType
      <*> mayDecode "pureExpr" mbExpr decodeExpr
  LF1.ScenarioSumBlock (LF1.Block binds mbBody) -> do
    body <- mayDecode "blockBody" mbBody decodeExpr
    foldr (\b e -> EScenario $ SBind b e) body <$> mapM decodeBinding (V.toList binds)
  LF1.ScenarioSumCommit LF1.Scenario_Commit{..} ->
    fmap EScenario $ SCommit
      <$> mayDecode "scenario_CommitRetType" scenario_CommitRetType decodeType
      <*> mayDecode "scenario_CommitParty" scenario_CommitParty decodeExpr
      <*> mayDecode "scenario_CommitExpr" scenario_CommitExpr decodeExpr
  LF1.ScenarioSumMustFailAt LF1.Scenario_Commit{..} ->
    fmap EScenario $ SMustFailAt
      <$> mayDecode "scenario_CommitRetType" scenario_CommitRetType decodeType
      <*> mayDecode "scenario_CommitParty" scenario_CommitParty decodeExpr
      <*> mayDecode "scenario_CommitExpr" scenario_CommitExpr decodeExpr
  LF1.ScenarioSumPass delta ->
    EScenario . SPass <$> decodeExpr delta
  LF1.ScenarioSumGetTime LF1.Unit ->
    pure (EScenario SGetTime)
  LF1.ScenarioSumGetParty name ->
    EScenario . SGetParty <$> decodeExpr name
  LF1.ScenarioSumEmbedExpr LF1.Scenario_EmbedExpr{..} ->
    fmap EScenario $ SEmbedExpr
      <$> mayDecode "scenario_EmbedExprType" scenario_EmbedExprType decodeType
      <*> mayDecode "scenario_EmbedExprBody" scenario_EmbedExprBody decodeExpr

decodeCaseAlt :: LF1.CaseAlt -> Decode CaseAlternative
decodeCaseAlt LF1.CaseAlt{..} = do
  pat <- mayDecode "caseAltSum" caseAltSum $ \case
    LF1.CaseAltSumDefault LF1.Unit -> pure CPDefault
    LF1.CaseAltSumVariant LF1.CaseAlt_Variant{..} ->
      CPVariant
        <$> mayDecode "caseAlt_VariantCon" caseAlt_VariantCon decodeTypeConName
        <*> decodeName VariantConName caseAlt_VariantVariant
        <*> decodeName ExprVarName caseAlt_VariantBinder
    LF1.CaseAltSumEnum LF1.CaseAlt_Enum{..} ->
      CPEnum
        <$> mayDecode "caseAlt_DataCon" caseAlt_EnumCon decodeTypeConName
        <*> decodeName VariantConName caseAlt_EnumConstructor
    LF1.CaseAltSumPrimCon (Proto.Enumerated (Right pcon)) -> pure $ case pcon of
      LF1.PrimConCON_UNIT -> CPUnit
      LF1.PrimConCON_TRUE -> CPBool True
      LF1.PrimConCON_FALSE -> CPBool False
    LF1.CaseAltSumPrimCon (Proto.Enumerated (Left idx)) ->
      throwError (UnknownEnum "CaseAltSumPrimCon" idx)
    LF1.CaseAltSumNil LF1.Unit -> pure CPNil
    LF1.CaseAltSumCons LF1.CaseAlt_Cons{..} ->
      CPCons <$> decodeName ExprVarName caseAlt_ConsVarHead <*> decodeName ExprVarName caseAlt_ConsVarTail
    LF1.CaseAltSumOptionalNone LF1.Unit -> pure CPNone
    LF1.CaseAltSumOptionalSome LF1.CaseAlt_OptionalSome{..} ->
      CPSome <$> decodeName ExprVarName caseAlt_OptionalSomeVarBody
  body <- mayDecode "caseAltBody" caseAltBody decodeExpr
  pure $ CaseAlternative pat body

decodeBinding :: LF1.Binding -> Decode Binding
decodeBinding (LF1.Binding mbBinder mbBound) =
  Binding
    <$> mayDecode "bindingBinder" mbBinder decodeVarWithType
    <*> mayDecode "bindingBound" mbBound decodeExpr

decodeTypeVarWithKind :: LF1.TypeVarWithKind -> Decode (TypeVarName, Kind)
decodeTypeVarWithKind LF1.TypeVarWithKind{..} =
  (,)
    <$> decodeName TypeVarName typeVarWithKindVar
    <*> mayDecode "typeVarWithKindKind" typeVarWithKindKind decodeKind

decodeVarWithType :: LF1.VarWithType -> Decode (ExprVarName, Type)
decodeVarWithType LF1.VarWithType{..} =
  (,)
    <$> decodeName ExprVarName varWithTypeVar
    <*> mayDecode "varWithTypeType" varWithTypeType decodeType

decodePrimLit :: LF1.PrimLit -> Decode BuiltinExpr
decodePrimLit (LF1.PrimLit mbSum) = mayDecode "primLitSum" mbSum $ \case
  LF1.PrimLitSumInt64 sInt -> pure $ BEInt64 sInt
  LF1.PrimLitSumDecimal sDec -> decodeDecimalLit $ decodeString sDec
  LF1.PrimLitSumDecimalInternedId strId -> lookupString strId >>= decodeDecimalLit
  LF1.PrimLitSumNumeric sNum -> decodeNumericLit $ decodeString sNum
  LF1.PrimLitSumNumericInternedId strId -> lookupString strId >>= decodeNumericLit
  LF1.PrimLitSumTimestamp sTime -> pure $ BETimestamp sTime
  LF1.PrimLitSumText x -> pure $ BEText $ decodeString x
  LF1.PrimLitSumTextInternedId strId ->  BEText <$> lookupString strId
  LF1.PrimLitSumParty p -> pure $ BEParty $ PartyLiteral $ decodeString p
  LF1.PrimLitSumPartyInternedId strId -> BEParty . PartyLiteral <$> lookupString strId
  LF1.PrimLitSumDate days -> pure $ BEDate days

decodeDecimalLit :: T.Text -> Decode BuiltinExpr
decodeDecimalLit (T.unpack -> str) = case readMaybe str of
    Nothing -> throwError $ ParseError $ "bad fixed while decoding Decimal: " ++ show str
    Just dec -> pure $ BEDecimal dec

decodeNumericLit :: T.Text -> Decode BuiltinExpr
decodeNumericLit (T.unpack -> str) = case readMaybe str of
    Nothing -> throwError $ ParseError $ "bad Numeric literal: " ++ show str
    Just n -> pure $ BENumeric n

decodeKind :: LF1.Kind -> Decode Kind
decodeKind LF1.Kind{..} = mayDecode "kindSum" kindSum $ \case
  LF1.KindSumStar LF1.Unit -> pure KStar
  LF1.KindSumNat LF1.Unit -> pure KNat
  LF1.KindSumArrow (LF1.Kind_Arrow params mbResult) -> do
    result <- mayDecode "kind_ArrowResult" mbResult decodeKind
    foldr KArrow result <$> traverse decodeKind (V.toList params)

decodePrim :: LF1.PrimType -> Decode BuiltinType
decodePrim = pure . \case
  LF1.PrimTypeINT64 -> BTInt64
  LF1.PrimTypeDECIMAL -> BTDecimal
  LF1.PrimTypeNUMERIC -> BTNumeric
  LF1.PrimTypeTEXT    -> BTText
  LF1.PrimTypeTIMESTAMP -> BTTimestamp
  LF1.PrimTypePARTY   -> BTParty
  LF1.PrimTypeUNIT    -> BTUnit
  LF1.PrimTypeBOOL    -> BTBool
  LF1.PrimTypeLIST    -> BTList
  LF1.PrimTypeUPDATE  -> BTUpdate
  LF1.PrimTypeSCENARIO -> BTScenario
  LF1.PrimTypeDATE -> BTDate
  LF1.PrimTypeCONTRACT_ID -> BTContractId
  LF1.PrimTypeOPTIONAL -> BTOptional
  LF1.PrimTypeMAP -> BTMap
  LF1.PrimTypeARROW -> BTArrow
  LF1.PrimTypeANY -> BTAnyTemplate

decodeType :: LF1.Type -> Decode Type
decodeType LF1.Type{..} = mayDecode "typeSum" typeSum $ \case
  LF1.TypeSumVar (LF1.Type_Var var args) ->
    decodeWithArgs args $ TVar <$> decodeName TypeVarName var
  LF1.TypeSumNat n ->
    pure $ TNat (fromIntegral n)
    -- TODO (#2289): determine if some bounds check should be made here.
  LF1.TypeSumCon (LF1.Type_Con mbCon args) ->
    decodeWithArgs args $ TCon <$> mayDecode "type_ConTycon" mbCon decodeTypeConName
  LF1.TypeSumPrim (LF1.Type_Prim (Proto.Enumerated (Right prim)) args) -> do
    decodeWithArgs args $ TBuiltin <$> decodePrim prim
  LF1.TypeSumPrim (LF1.Type_Prim (Proto.Enumerated (Left idx)) _args) ->
    throwError (UnknownEnum "Prim" idx)
  LF1.TypeSumFun (LF1.Type_Fun params mbResult) -> do
    mkTFuns
      <$> mapM decodeType (V.toList params)
      <*> mayDecode "type_FunResult" mbResult decodeType
  LF1.TypeSumForall (LF1.Type_Forall binders mbBody) -> do
    body <- mayDecode "type_ForAllBody" mbBody decodeType
    foldr TForall body <$> traverse decodeTypeVarWithKind (V.toList binders)
  LF1.TypeSumTuple (LF1.Type_Tuple flds) ->
    TTuple <$> mapM (decodeFieldWithType FieldName) (V.toList flds)
  where
    decodeWithArgs :: V.Vector LF1.Type -> Decode Type -> Decode Type
    decodeWithArgs args fun = foldl TApp <$> fun <*> traverse decodeType args


decodeFieldWithType :: (T.Text -> a) -> LF1.FieldWithType -> Decode (a, Type)
decodeFieldWithType wrapName (LF1.FieldWithType name mbType) =
  (,)
    <$> decodeName wrapName name
    <*> mayDecode "fieldWithTypeType" mbType decodeType

decodeFieldWithExpr :: LF1.FieldWithExpr -> Decode (FieldName, Expr)
decodeFieldWithExpr (LF1.FieldWithExpr name mbExpr) =
  (,)
    <$> decodeName FieldName name
    <*> mayDecode "fieldWithExprExpr" mbExpr decodeExpr

decodeTypeConApp :: LF1.Type_Con -> Decode TypeConApp
decodeTypeConApp LF1.Type_Con{..} =
  TypeConApp
    <$> mayDecode "typeConAppTycon" type_ConTycon decodeTypeConName
    <*> mapM decodeType (V.toList type_ConArgs)

decodeTypeConName :: LF1.TypeConName -> Decode (Qualified TypeConName)
decodeTypeConName LF1.TypeConName{..} = do
  (pref, mname) <- mayDecode "typeConNameModule" typeConNameModule decodeModuleRef
  con <- mayDecode "typeConNameName" typeConNameName (decodeDottedName TypeConName)
  pure $ Qualified pref mname con

decodeModuleRef :: LF1.ModuleRef -> Decode (PackageRef, ModuleName)
decodeModuleRef LF1.ModuleRef{..} =
  (,)
    <$> mayDecode "moduleRefPackageRef" moduleRefPackageRef decodePackageRef
    <*> mayDecode "moduleRefModuleName" moduleRefModuleName (decodeDottedName ModuleName)

------------------------------------------------------------------------
-- Helpers
------------------------------------------------------------------------

mayDecode :: String -> Maybe a -> (a -> Decode b) -> Decode b
mayDecode fieldName mb f =
  case mb of
    Nothing -> throwError (MissingField fieldName)
    Just x  -> f x

decodeNM
  :: NM.Named b
  => (NM.Name b -> Error) -> (a -> Decode b) -> V.Vector a -> Decode (NM.NameMap b)
decodeNM mkDuplicateError decode1 xs = do
  ys <- traverse decode1 (V.toList xs)
  either (throwError . mkDuplicateError) pure $ NM.fromListEither ys
