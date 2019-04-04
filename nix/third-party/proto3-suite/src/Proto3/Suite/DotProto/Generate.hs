-- | This module provides functions to generate Haskell declarations for proto buf messages

{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE MultiWayIf        #-}
{-# LANGUAGE NamedFieldPuns    #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes       #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE TupleSections     #-}
{-# LANGUAGE ViewPatterns      #-}

module Proto3.Suite.DotProto.Generate
  ( CompileResult
  , CompileError(..)
  , TypeContext

  , compileDotProtoFile
  , compileDotProtoFileOrDie
  , hsModuleForDotProto
  , renderHsModuleForDotProto
  , readDotProtoWithContext

  -- * Utilities
  , isPackable

  -- * Exposed for unit-testing
  , fieldLikeName
  , prefixedEnumFieldName
  , typeLikeName
  ) where

import           Control.Applicative
import           Control.Arrow                  ((&&&), first)
import           Control.Monad.Except
import           Data.Char
import           Data.Coerce
import           Data.List                      (find, intercalate, nub, sortOn,
                                                 stripPrefix)
import qualified Data.Map                       as M
import           Data.Maybe                     (catMaybes, fromMaybe)
import           Data.Monoid
import qualified Data.Set                       as S
import           Data.String                    (fromString)
import qualified Data.Text                      as T
import           Filesystem.Path.CurrentOS      ((</>))
import qualified Filesystem.Path.CurrentOS      as FP
import           Language.Haskell.Pretty
import           Language.Haskell.Syntax
import           Language.Haskell.Parser        (ParseResult(..), parseModule)
import qualified NeatInterpolation              as Neat
import           Prelude                        hiding (FilePath)
import           Proto3.Suite.DotProto
import           Proto3.Suite.DotProto.Internal
import           Proto3.Wire.Types              (FieldNumber (..))
import           System.IO                      (writeFile, readFile)
import           Text.Parsec                    (ParseError)
import           Turtle                         (FilePath)
import qualified Turtle
import           Turtle.Format                  ((%))
import qualified Turtle.Format                  as F

-- * Public interface

data CompileError
  = CircularImport          FilePath
  | CompileParseError       ParseError
  | InternalEmptyModulePath
  | InternalError           String
  | InvalidMethodName       DotProtoIdentifier
  | InvalidTypeName         String
  | NoPackageDeclaration
  | NoSuchType              DotProtoIdentifier
  | Unimplemented           String
    deriving (Show, Eq)

-- | Result of a compilation. 'Left err' on error, where 'err' is a
--   'String' describing the error. Otherwise, the result of the
--   compilation.
type CompileResult = Either CompileError

-- | @compileDotProtoFile out includeDir dotProtoPath@ compiles the .proto file
-- at @dotProtoPath@ into a new Haskell module in @out/@, using the ordered
-- @paths@ list to determine which paths to be searched for the .proto file and
-- its transitive includes. 'compileDotProtoFileOrDie' provides a wrapper around
-- this function which terminates the program with an error message.
-- Instances declared in @extrainstances@ will take precedence over those that
-- would be otherwise generated.
compileDotProtoFile ::  [FilePath] -> FilePath -> [FilePath] -> FilePath -> IO (CompileResult ())
compileDotProtoFile extrainstances out paths dotProtoPath = runExceptT $ do
  (dp, tc) <- ExceptT $ readDotProtoWithContext paths dotProtoPath
  let DotProtoMeta (Path mp) = protoMeta dp
      mkHsModPath            = (out </>) . (FP.<.> "hs") . FP.concat . (fromString <$>)
  when (null mp) $ throwError InternalEmptyModulePath
  mp' <- mkHsModPath <$> mapM (ExceptT . pure . typeLikeName) mp
  eis <- fmap mconcat . mapM (ExceptT . getExtraInstances) $ extrainstances
  hs  <- ExceptT . pure $ renderHsModuleForDotProto eis dp tc
  Turtle.mktree (FP.directory mp')
  liftIO $ writeFile (FP.encodeString mp') hs

-- | As 'compileDotProtoFile', except terminates the program with an error
-- message on failure.
compileDotProtoFileOrDie :: [FilePath] -> FilePath -> [FilePath] -> FilePath -> IO ()
compileDotProtoFileOrDie extrainstances out paths dotProtoPath =
  compileDotProtoFile extrainstances out paths dotProtoPath >>= \case
    Left e -> do
      let errText          = T.pack (show e) -- TODO: pretty print the error messages
          dotProtoPathText = Turtle.format F.fp dotProtoPath
      dieLines [Neat.text|
        Error: failed to compile "${dotProtoPathText}":

        ${errText}
      |]
    _ -> pure ()

getExtraInstances :: FilePath -> IO (CompileResult ([HsImportDecl], [HsDecl]))
getExtraInstances fp = do
  parseRes <- parseModule <$> readFile (FP.encodeString fp)
  case parseRes of
         ParseOk (HsModule _srcloc _mod _es idecls decls) ->
             let isInstDecl HsInstDecl{} = True
                 isInstDecl _ = False
             in return . Right $ (idecls, filter isInstDecl decls) --TODO give compile result
         ParseFailed src err -> return . Left . InternalError $ "extra instance parse failed\n" ++ show src ++ ": " ++ show err


-- | Compile a 'DotProto' AST into a 'String' representing the Haskell
--   source of a module implementing types and instances for the .proto
--   messages and enums.
renderHsModuleForDotProto :: ([HsImportDecl],[HsDecl]) -> DotProto -> TypeContext -> CompileResult String
renderHsModuleForDotProto eis dp importCtxt =
    fmap (("{-# LANGUAGE DeriveGeneric #-}\n" ++) .
          ("{-# LANGUAGE DeriveAnyClass #-}\n" ++) .
          ("{-# LANGUAGE DataKinds #-}\n" ++) .
          ("{-# LANGUAGE GADTs #-}\n" ++) .
          ("{-# LANGUAGE OverloadedStrings #-}\n" ++) .
          ("{-# OPTIONS_GHC -fno-warn-unused-imports #-}\n" ++) .
          ("{-# OPTIONS_GHC -fno-warn-name-shadowing #-}\n" ++) .
          ("{-# OPTIONS_GHC -fno-warn-unused-matches #-}\n" ++) .

          ("-- | Generated by Haskell protocol buffer compiler. " ++) .
          ("DO NOT EDIT!\n" ++) .
          prettyPrint) $
    hsModuleForDotProto eis dp importCtxt

-- | Compile a Haskell module AST given a 'DotProto' package AST.
-- Instances given in @eis@ override those otherwise generated.
hsModuleForDotProto :: ([HsImportDecl], [HsDecl]) -> DotProto -> TypeContext -> CompileResult HsModule
hsModuleForDotProto _eis (DotProto{ protoMeta = DotProtoMeta (Path []) }) _importCtxt
  = Left InternalEmptyModulePath
hsModuleForDotProto
  (extraimports, extrainstances)
  dp@DotProto{ protoPackage     = DotProtoPackageSpec pkgIdent
             , protoMeta        = DotProtoMeta modulePath
             , protoDefinitions = defs
             }
  importCtxt
  = do
     mname <- modulePathModName modulePath
     module_ mname
        <$> pure Nothing
        <*> do mappend (defaultImports hasService `mappend` extraimports) <$> ctxtImports importCtxt
        <*> do tc <- dotProtoTypeContext dp
               replaceHsInstDecls (instancesForModule mname extrainstances) . mconcat  <$> mapM (dotProtoDefinitionD pkgIdent (tc <> importCtxt)) defs

  where hasService = not (null [ () | DotProtoService {} <- defs ])

hsModuleForDotProto _ _ _
  = Left NoPackageDeclaration

-- This very specific function will only work for the qualification on the very first type
-- in the object of an instance declaration. Those are the only sort of instance declarations
-- generated within this code, so it suffices.
instancesForModule :: Module -> [HsDecl] -> [HsDecl]
instancesForModule m = foldr go []
   where go x xs = case x of
             HsInstDecl a b c (HsTyCon (Qual tm  i):ts) d ->
                        if m == tm then HsInstDecl a b c (HsTyCon (UnQual i):ts) d:xs else xs
             _ -> xs

-- | For each thing in @base@ replaces it if it finds a matching @override@
replaceHsInstDecls :: [HsDecl] -> [HsDecl] -> [HsDecl]
replaceHsInstDecls overrides base = map mbReplace base
  where mbReplace hid@(HsInstDecl _ _ qn tys _) = fromMaybe hid $ find (\x -> Just (unQual qn,tys) == getSig x) overrides
        mbReplace hid = hid

        getSig (HsInstDecl _ _ qn tys _) = Just (unQual qn,tys)
        getSig _ = Nothing

        unQual (Qual _ n) = Just n
        unQual (UnQual n) = Just n
        unQual (Special _) = Nothing

-- | Parses the file at the given path and produces an AST along with a
-- 'TypeContext' representing all types from imported @.proto@ files, using the
-- first parameter as a list of paths to search for imported files. Terminates
-- with exit code 1 when an included file cannot be found in the search path.
readDotProtoWithContext :: [FilePath] -> FilePath -> IO (CompileResult (DotProto, TypeContext))

readDotProtoWithContext [] dotProtoPath = do
  -- If we're not given a search path, default to using the current working
  -- directory, as `protoc` does
  cwd <- Turtle.pwd
  readDotProtoWithContext [cwd] dotProtoPath

readDotProtoWithContext searchPaths toplevelProto = runExceptT $ do
  findProto searchPaths toplevelProto >>= \case
    Found mp fp     -> parse mp fp
    BadModulePath e -> fatalBadModulePath toplevelProto e
    NotFound        -> dieLines [Neat.text|
      Error: failed to find file "${toplevelProtoText}", after looking in
      the following locations (controlled via the --includeDir switch(es)):

      $searchPathsText
    |]
  where
    parse mp fp = parseProtoFile mp fp >>= \case
      Right dp -> do
        let importIt = readImportTypeContext searchPaths toplevelProto (S.singleton toplevelProto)
        tc <- mconcat <$> mapM importIt (protoImports dp)
        pure (dp, tc)
      Left err -> throwError (CompileParseError err)

    searchPathsText   = T.unlines (Turtle.format ("  "%F.fp) . (</> toplevelProto) <$> searchPaths)
    toplevelProtoText = Turtle.format F.fp toplevelProto

readImportTypeContext :: [FilePath] -> FilePath -> S.Set FilePath -> DotProtoImport
                      -> ExceptT CompileError IO TypeContext
readImportTypeContext searchPaths toplevelFP alreadyRead (DotProtoImport _ path)
  | path `S.member` alreadyRead = throwError (CircularImport path)
  | otherwise =
      do import_ <- wrapError CompileParseError =<< importProto searchPaths toplevelFP path
         case protoPackage import_ of
           DotProtoPackageSpec importPkg ->
             do importTypeContext <- wrapError id (dotProtoTypeContext import_)
                let importTypeContext' = flip fmap importTypeContext $ \tyInfo ->
                      tyInfo { dotProtoTypeInfoPackage    = DotProtoPackageSpec importPkg
                             , dotProtoTypeInfoModulePath = metaModulePath . protoMeta $ import_
                             }
                    qualifiedTypeContext = M.fromList <$>
                        mapM (\(nm, tyInfo) -> (,tyInfo) <$> concatDotProtoIdentifier importPkg nm)
                            (M.assocs importTypeContext')

                importTypeContext'' <- wrapError id ((importTypeContext' <>) <$> qualifiedTypeContext)
                (importTypeContext'' <>) . mconcat <$> sequence
                    [ readImportTypeContext searchPaths toplevelFP (S.insert path alreadyRead) importImport
                    | importImport@(DotProtoImport DotProtoImportPublic _) <- protoImports import_]
           _ -> throwError NoPackageDeclaration
  where
    wrapError :: (err' -> err) -> Either err' a -> ExceptT err IO a
    wrapError f = either (throwError . f) pure

-- * Type-tracking data structures

-- | Whether a definition is an enumeration or a message
data DotProtoKind = DotProtoKindEnum
                  | DotProtoKindMessage
                  deriving (Show, Eq, Ord, Enum, Bounded)

-- | Information about messages and enumerations
data DotProtoTypeInfo = DotProtoTypeInfo
  { dotProtoTypeInfoPackage    :: DotProtoPackageSpec
     -- ^ The package this type is defined in
  , dotProtoTypeInfoParent     :: DotProtoIdentifier
    -- ^ The message this type is nested under, or 'Anonymous' if it's top-level
  , dotProtoTypeChildContext   :: TypeContext
    -- ^ The context that should be used for declarations within the
    --   scope of this type
  , dotProtoTypeInfoKind       :: DotProtoKind
    -- ^ Whether this type is an enumeration or message
  , dotProtoTypeInfoModulePath :: Path
    -- ^ The include-relative module path used when importing this module
  } deriving Show

-- | A mapping from .proto type identifiers to their type information
type TypeContext = M.Map DotProtoIdentifier DotProtoTypeInfo

-- ** Generating type contexts from ASTs

dotProtoTypeContext :: DotProto -> CompileResult TypeContext
dotProtoTypeContext DotProto { protoDefinitions
                             , protoMeta = DotProtoMeta modulePath
                             }
  = mconcat <$> mapM (definitionTypeContext modulePath) protoDefinitions

definitionTypeContext :: Path -> DotProtoDefinition -> CompileResult TypeContext
definitionTypeContext modulePath (DotProtoMessage msgIdent parts) =
    do childTyContext <-
          mapM updateDotProtoTypeInfoParent =<<
          (mconcat <$> sequenceA
               [ definitionTypeContext modulePath def
               | DotProtoMessageDefinition def <- parts ])

       qualifiedChildTyContext <- M.fromList <$>
          mapM (\(nm, tyInfo) -> (,tyInfo) <$>
                                 concatDotProtoIdentifier msgIdent nm)
               (M.assocs childTyContext)

       pure (M.singleton msgIdent
                 (DotProtoTypeInfo DotProtoNoPackage Anonymous
                      childTyContext DotProtoKindMessage modulePath) <>
               qualifiedChildTyContext)
  where updateDotProtoTypeInfoParent tyInfo =
            do dotProtoTypeInfoParent <-
                     concatDotProtoIdentifier msgIdent (dotProtoTypeInfoParent tyInfo)
               pure tyInfo { dotProtoTypeInfoParent }
definitionTypeContext modulePath (DotProtoEnum enumIdent _) =
  pure (M.singleton enumIdent
            (DotProtoTypeInfo DotProtoNoPackage Anonymous mempty DotProtoKindEnum modulePath))
definitionTypeContext _ _ = pure mempty

concatDotProtoIdentifier :: DotProtoIdentifier -> DotProtoIdentifier
                         -> CompileResult DotProtoIdentifier
concatDotProtoIdentifier Qualified {} _ =
    internalError "concatDotProtoIdentifier: Qualified"
concatDotProtoIdentifier _ Qualified {} =
    internalError "concatDotProtoIdentifier Qualified"
concatDotProtoIdentifier Anonymous Anonymous = pure Anonymous
concatDotProtoIdentifier Anonymous b = pure b
concatDotProtoIdentifier a Anonymous = pure a
concatDotProtoIdentifier (Single a) b = concatDotProtoIdentifier (Dots (Path [a])) b
concatDotProtoIdentifier a (Single b) = concatDotProtoIdentifier a (Dots (Path [b]))
concatDotProtoIdentifier (Dots (Path a)) (Dots (Path b)) = pure . Dots . Path $ a ++ b

-- | Given a type context, generates the import statements necessary
--   to import all the required types.
ctxtImports :: TypeContext -> CompileResult [HsImportDecl]
ctxtImports tyCtxt =
  do imports <- nub <$> sequence
                          [ modulePathModName modulePath
                          | DotProtoTypeInfo
                            { dotProtoTypeInfoModulePath = modulePath
                            } <- M.elems tyCtxt
                          ]
     pure [ importDecl_ modName True Nothing Nothing | modName <- imports ]

-- * Functions to convert 'DotProtoType' into Haskell types

-- | Produce the Haskell type for the given 'DotProtoType' in the
--   given 'TypeContext'
hsTypeFromDotProto :: TypeContext -> DotProtoType -> CompileResult HsType
hsTypeFromDotProto ctxt (Prim (Named msgName))
    | Just DotProtoKindMessage <-
          dotProtoTypeInfoKind <$> M.lookup msgName ctxt =
        HsTyApp (primType_ "Maybe") <$>
            hsTypeFromDotProtoPrim ctxt (Named msgName)
hsTypeFromDotProto ctxt (Prim pType) =
    hsTypeFromDotProtoPrim ctxt pType
hsTypeFromDotProto ctxt (Optional (Named nm)) =
    hsTypeFromDotProto ctxt (Prim (Named nm))
hsTypeFromDotProto ctxt (Optional pType) =
    HsTyApp (primType_ "Maybe") <$> hsTypeFromDotProtoPrim ctxt pType
hsTypeFromDotProto ctxt (Repeated pType) =
    HsTyApp (primType_ "Vector") <$> hsTypeFromDotProtoPrim ctxt pType
hsTypeFromDotProto ctxt (NestedRepeated pType) =
    HsTyApp (primType_ "Vector") <$> hsTypeFromDotProtoPrim ctxt pType
hsTypeFromDotProto _    (Map _ _) =
    internalError "No support for protobuf mappings"

hsTypeFromDotProtoPrim :: TypeContext -> DotProtoPrimType -> CompileResult HsType
hsTypeFromDotProtoPrim _    Int32           = pure $ primType_ "Int32"
hsTypeFromDotProtoPrim _    Int64           = pure $ primType_ "Int64"
hsTypeFromDotProtoPrim _    SInt32          = pure $ primType_ "Int32"
hsTypeFromDotProtoPrim _    SInt64          = pure $ primType_ "Int64"
hsTypeFromDotProtoPrim _    UInt32          = pure $ primType_ "Word32"
hsTypeFromDotProtoPrim _    UInt64          = pure $ primType_ "Word64"
hsTypeFromDotProtoPrim _    Fixed32         = pure $ HsTyApp (protobufType_ "Fixed")
                                                             (primType_ "Word32")
hsTypeFromDotProtoPrim _    Fixed64         = pure $ HsTyApp (protobufType_ "Fixed")
                                                             (primType_ "Word64")
hsTypeFromDotProtoPrim _    SFixed32        = pure $ HsTyApp (protobufType_ "Fixed")
                                                             (primType_ "Int32")
hsTypeFromDotProtoPrim _    SFixed64        = pure $ HsTyApp (protobufType_ "Fixed")
                                                             (primType_ "Int64")
hsTypeFromDotProtoPrim _    String          = pure $ primType_ "Text"
hsTypeFromDotProtoPrim _    Bytes           = pure $ primType_ "ByteString"
hsTypeFromDotProtoPrim _    Bool            = pure $ primType_ "Bool"
hsTypeFromDotProtoPrim _    Float           = pure $ primType_ "Float"
hsTypeFromDotProtoPrim _    Double          = pure $ primType_ "Double"
hsTypeFromDotProtoPrim ctxt (Named msgName) =
    case M.lookup msgName ctxt of
      Just ty@(DotProtoTypeInfo { dotProtoTypeInfoKind = DotProtoKindEnum }) ->
          HsTyApp (protobufType_ "Enumerated") <$> msgTypeFromDpTypeInfo ty msgName
      Just ty -> msgTypeFromDpTypeInfo ty msgName
      Nothing -> noSuchTypeError msgName

-- | Generate the Haskell type name for a 'DotProtoTypeInfo' for a message /
--   enumeration being compiled. NB: We ignore the 'dotProtoTypeInfoPackage'
--   field of the 'DotProtoTypeInfo' parameter, instead demanding that we have
--   been provided with a valid module path in its 'dotProtoTypeInfoModulePath'
--   field. The latter describes the name of the Haskell module being generated.
msgTypeFromDpTypeInfo :: DotProtoTypeInfo -> DotProtoIdentifier
                      -> CompileResult HsType
msgTypeFromDpTypeInfo
  DotProtoTypeInfo{ dotProtoTypeInfoModulePath = Path [] }
  _ident
  = Left InternalEmptyModulePath
msgTypeFromDpTypeInfo
  DotProtoTypeInfo { dotProtoTypeInfoParent     = p
                   , dotProtoTypeInfoModulePath = modulePath
                   }
  ident
  = HsTyCon <$> do Qual <$> modulePathModName modulePath
                        <*> do HsIdent <$> do
                                 nestedTypeName p =<< dpIdentUnqualName ident

-- | Given a 'DotProtoIdentifier' for the parent type and the unqualified name of this type, generate the corresponding Haskell name
nestedTypeName :: DotProtoIdentifier -> String -> CompileResult String
nestedTypeName Anonymous       nm = typeLikeName nm
nestedTypeName (Single parent) nm =
    intercalate "_" <$> sequenceA [ typeLikeName parent
                                  , typeLikeName nm ]
nestedTypeName (Dots (Path parents)) nm =
    (<> ("_" <> nm)) <$> (intercalate "_" <$> mapM typeLikeName parents)
nestedTypeName (Qualified {})  _  = internalError "nestedTypeName: Qualified"

haskellName, jsonpbName, grpcName, protobufName :: String -> HsQName
haskellName  name = Qual (Module "Hs") (HsIdent name)
jsonpbName   name = Qual (Module "HsJSONPB") (HsIdent name)
grpcName     name = Qual (Module "HsGRPC") (HsIdent name)
protobufName name = Qual (Module "HsProtobuf") (HsIdent name)

camelCased :: String -> String
camelCased s = do (prev, cur) <- zip (Nothing:map Just s) (map Just s ++ [Nothing])
                  case (prev, cur) of
                    (Just '_', Just x) | isAlpha x -> pure (toUpper x)
                    (Just '_', Nothing) -> pure '_'
                    (Just '_', Just '_') -> pure '_'
                    (_, Just '_') -> empty
                    (_, Just x) -> pure x
                    (_, _) -> empty

typeLikeName :: String -> CompileResult String
typeLikeName ident@(firstChar:remainingChars)
  | isUpper firstChar = pure (camelCased ident)
  | isLower firstChar = pure (camelCased (toUpper firstChar:remainingChars))
  | firstChar == '_'  = pure (camelCased ('X':ident))
typeLikeName ident = invalidTypeNameError ident

fieldLikeName :: String -> String
fieldLikeName ident@(firstChar:_)
  | isUpper firstChar = let (prefix, suffix) = span isUpper ident
                        in map toLower prefix ++ suffix
fieldLikeName ident = ident

prefixedEnumFieldName :: String -> String -> CompileResult String
prefixedEnumFieldName enumName fieldName = pure (enumName <> fieldName)

prefixedConName :: String -> String -> CompileResult String
prefixedConName msgName conName =
  (msgName ++) <$> typeLikeName conName

-- TODO: This should be ~:: MessageName -> FieldName -> ...; same elsewhere, the
-- String types are a bit of a hassle.
prefixedFieldName :: String -> String -> CompileResult String
prefixedFieldName msgName fieldName =
  (fieldLikeName msgName ++) <$> typeLikeName fieldName

dpIdentUnqualName :: DotProtoIdentifier -> CompileResult String
dpIdentUnqualName (Single name)       = pure name
dpIdentUnqualName (Dots (Path names)) = pure (last names)
dpIdentUnqualName (Qualified _ next)  = dpIdentUnqualName next
dpIdentUnqualName Anonymous           = internalError "dpIdentUnqualName: Anonymous"

dpIdentQualName :: DotProtoIdentifier -> CompileResult String
dpIdentQualName (Single name)       = pure name
dpIdentQualName (Dots (Path names)) = pure (intercalate "." names)
dpIdentQualName (Qualified _ _)     = internalError "dpIdentQualName: Qualified"
dpIdentQualName Anonymous           = internalError "dpIdentQualName: Anonymous"

modulePathModName :: Path -> CompileResult Module
modulePathModName (Path [])    = Left InternalEmptyModulePath
modulePathModName (Path comps) = Module <$> (intercalate "." <$> mapM typeLikeName comps)

_pkgIdentModName :: DotProtoIdentifier -> CompileResult Module
_pkgIdentModName (Single s)          = Module <$> typeLikeName s
_pkgIdentModName (Dots (Path paths)) = Module <$> (intercalate "." <$> mapM typeLikeName paths)
_pkgIdentModName _                   = internalError "pkgIdentModName: Malformed package name"

-- * Generate instances for a 'DotProto' package

dotProtoDefinitionD :: DotProtoIdentifier -> TypeContext -> DotProtoDefinition
                    -> CompileResult [HsDecl]
dotProtoDefinitionD _ ctxt (DotProtoMessage messageName dotProtoMessage) =
  dotProtoMessageD ctxt Anonymous messageName dotProtoMessage
dotProtoDefinitionD _ _ (DotProtoEnum messageName dotProtoEnum) =
  dotProtoEnumD Anonymous messageName dotProtoEnum
dotProtoDefinitionD pkgIdent ctxt (DotProtoService serviceName dotProtoService) =
  dotProtoServiceD pkgIdent ctxt serviceName dotProtoService

-- | Generate 'Named' instance for a type in this package
namedInstD :: String -> HsDecl
namedInstD messageName =
  instDecl_ (protobufName "Named")
      [ type_ messageName ]
      [ HsFunBind [nameOfDecl] ]
  where
    nameOfDecl = match_ (HsIdent "nameOf") [HsPWildCard]
                        (HsUnGuardedRhs (apply fromStringE
                                               [ HsLit (HsString messageName) ]))
                        []

-- ** Generate types and instances for .proto messages

-- | Generate data types, 'Bounded', 'Enum', 'FromJSONPB', 'Named', 'Message',
--   'ToJSONPB' instances as appropriate for the given 'DotProtoMessagePart's
dotProtoMessageD :: TypeContext -> DotProtoIdentifier -> DotProtoIdentifier
                 -> [DotProtoMessagePart] -> CompileResult [HsDecl]
dotProtoMessageD ctxt parentIdent messageIdent message =
    do messageName <- nestedTypeName parentIdent =<<
                      dpIdentUnqualName messageIdent

       let ctxt' = maybe mempty dotProtoTypeChildContext (M.lookup messageIdent ctxt) <>
                   ctxt

           messagePartFieldD (DotProtoMessageField (DotProtoField _ ty fieldName _ _)) =
               do fullName <- prefixedFieldName messageName =<<
                              dpIdentUnqualName fieldName
                  fullTy <- hsTypeFromDotProto ctxt' ty
                  pure [ ([HsIdent fullName], HsUnBangedTy fullTy ) ]
           messagePartFieldD (DotProtoMessageOneOf fieldName _) =
               do fullName <- prefixedFieldName messageName =<<
                              dpIdentUnqualName fieldName
                  fullTy <- HsTyApp (HsTyCon (haskellName "Maybe")) . type_
                            <$> do prefixedConName messageName
                                     =<< dpIdentUnqualName fieldName
                  pure [ ([HsIdent fullName], HsUnBangedTy fullTy) ]
           messagePartFieldD _ = pure []

           nestedDecls :: DotProtoDefinition -> CompileResult [HsDecl]
           nestedDecls (DotProtoMessage subMsgName subMessageDef) =
               do parentIdent' <- concatDotProtoIdentifier parentIdent messageIdent
                  dotProtoMessageD ctxt' parentIdent' subMsgName subMessageDef
           nestedDecls (DotProtoEnum subEnumName subEnumDef) =
               do parentIdent' <- concatDotProtoIdentifier parentIdent messageIdent
                  dotProtoEnumD parentIdent' subEnumName subEnumDef
           nestedDecls _ = pure []

           nestedOneOfDecls :: DotProtoIdentifier -> [DotProtoField] -> CompileResult [HsDecl]
           nestedOneOfDecls identifier fields =
               do fullName <- prefixedConName messageName =<< dpIdentUnqualName identifier
                  let oneOfCons (DotProtoField _ ty fieldName _ _) =
                        do consTy <- case ty of
                             Prim msg@(Named msgName)
                               | Just DotProtoKindMessage <- dotProtoTypeInfoKind <$> M.lookup msgName ctxt'
                                 -> -- Do not wrap message summands with Maybe.
                                    hsTypeFromDotProtoPrim ctxt' msg
                             _   -> hsTypeFromDotProto ctxt' ty
                           consName <- prefixedConName fullName =<< dpIdentUnqualName fieldName
                           pure $ conDecl_ (HsIdent consName) [HsUnBangedTy consTy]
                      oneOfCons DotProtoEmptyField =
                          internalError "field type : empty field"
                  cons <- mapM oneOfCons fields
                  pure [ dataDecl_ fullName cons defaultMessageDeriving
                       , namedInstD fullName
                       , toSchemaInstDecl fullName
                       ]

       conDecl <- recDecl_ (HsIdent messageName) . mconcat <$>
                  mapM messagePartFieldD message

       nestedDecls_ <- mconcat <$>
           sequence [ nestedDecls def | DotProtoMessageDefinition def <- message]
       nestedOneofs_ <- mconcat <$>
           sequence [ nestedOneOfDecls ident fields
                    | DotProtoMessageOneOf ident fields <- message ]

       messageInst <- messageInstD ctxt' parentIdent messageIdent message

       toJSONPBInst   <- toJSONPBMessageInstD   ctxt' parentIdent messageIdent message
       fromJSONPBInst <- fromJSONPBMessageInstD ctxt' parentIdent messageIdent message

       pure $ [ dataDecl_ messageName [ conDecl ] defaultMessageDeriving
              , namedInstD messageName
              , messageInst
              , toJSONPBInst
              , fromJSONPBInst
                -- Generate Aeson instances in terms of JSONPB instances
              , toJSONInstDecl messageName
              , fromJSONInstDecl messageName
              -- And the Swagger ToSchema instance corresponding to JSONPB encodings
              , toSchemaInstDecl messageName
              ]
              <> nestedOneofs_
              <> nestedDecls_

messageInstD :: TypeContext -> DotProtoIdentifier -> DotProtoIdentifier
             -> [DotProtoMessagePart] -> CompileResult HsDecl
messageInstD ctxt parentIdent msgIdent messageParts =
  do msgName <- nestedTypeName parentIdent =<<
                dpIdentUnqualName msgIdent
     qualifiedFields <- getQualifiedFields msgName messageParts
     encodeMessagePartEs <- forM qualifiedFields $ \QualifiedField{recordFieldName, fieldInfo} ->
        let recordFieldName' = HsVar (unqual_ (coerce recordFieldName)) in
        case fieldInfo of
            FieldNormal _fieldName fieldNum dpType options ->
                do fieldE <- wrapE ctxt dpType options recordFieldName'
                   pure (apply encodeMessageFieldE [ fieldNumberE fieldNum, fieldE ])
            FieldOneOf OneofField{subfields} ->
                do -- Create all pattern match & expr for each constructor:
                   --    Constructor y -> encodeMessageField num (Nested (Just y)) -- for embedded messages
                   --    Constructor y -> encodeMessageField num (ForceEmit y)     -- for everything else
                   alts <- sequence $
                     [ do
                       xE <- case dpType of
                                 Prim (Named tyName)
                                   | Just DotProtoKindMessage <- dotProtoTypeInfoKind <$> M.lookup tyName ctxt
                                     -> wrapE ctxt dpType options . HsParen . HsApp (HsVar (haskellName "Just"))
                                 _ -> fmap forceEmitE . wrapE ctxt dpType options
                             $ HsVar (unqual_ "y")
                       pure $
                        alt_ (HsPApp (unqual_ conName) [patVar "y"])
                             (HsUnGuardedAlt (apply encodeMessageFieldE [fieldNumberE fieldNum, xE]))
                             []
                     | OneofSubfield fieldNum conName _ dpType options <- subfields
                     ]
                   pure $ HsCase recordFieldName'
                        $ [ alt_ (HsPApp (haskellName "Nothing") [])
                                 (HsUnGuardedAlt memptyE)
                                 []
                          , alt_ (HsPApp (haskellName "Just") [patVar "x"])
                                 (HsUnGuardedAlt (HsCase (HsVar (unqual_ "x")) alts))
                                 []
                          ]

     decodeMessagePartEs <- sequence
        [ case fieldInfo of
            FieldNormal _fieldName fieldNum dpType options ->
                unwrapE ctxt dpType options $ apply atE [ decodeMessageFieldE, fieldNumberE fieldNum ]
            FieldOneOf OneofField{subfields} -> do
                -- create a list of (fieldNumber, Cons <$> parser)
                let subfieldParserE (OneofSubfield fieldNumber consName _ dpType options) = do
                      decodeMessageFieldE' <- unwrapE ctxt dpType options decodeMessageFieldE
                      let fE = case dpType of
                                 Prim (Named tyName)
                                   | Just DotProtoKindMessage <- dotProtoTypeInfoKind <$> M.lookup tyName ctxt
                                     -> HsParen (HsApp fmapE (HsVar (unqual_ consName)))
                                 _ -> HsParen (HsInfixApp (HsVar (haskellName "Just"))
                                                          composeOp
                                                          (HsVar (unqual_ consName)))
                      pure $ HsTuple
                               [ fieldNumberE fieldNumber
                               , HsInfixApp (apply pureE [ fE ])
                                            apOp
                                            decodeMessageFieldE'
                               ]
                subfieldParserEs <- mapM subfieldParserE subfields
                pure $ apply oneofE [ HsVar (haskellName "Nothing")
                                    , HsList subfieldParserEs
                                    ]
        | QualifiedField _ fieldInfo <- qualifiedFields ]

     dotProtoE <- HsList <$> sequence
         [ dpTypeE dpType >>= \typeE ->
             pure (apply dotProtoFieldC [ fieldNumberE fieldNum, typeE
                                        , dpIdentE fieldIdent
                                        , HsList (map optionE options)
                                        , maybeE (HsLit . HsString) comments ])
         | DotProtoMessageField (DotProtoField fieldNum dpType fieldIdent options comments)
             <- messageParts ]

     let encodeMessageDecl = match_ (HsIdent "encodeMessage")
                                    [HsPWildCard, HsPRec (unqual_ msgName) punnedFieldsP]
                                    (HsUnGuardedRhs encodeMessageE) []
         decodeMessageDecl = match_ (HsIdent "decodeMessage") [ HsPWildCard ]
                                    (HsUnGuardedRhs decodeMessageE) []
         dotProtoDecl = match_ (HsIdent "dotProto") [HsPWildCard]
                               (HsUnGuardedRhs dotProtoE) []

         punnedFieldsP =
             [ HsPFieldPat (unqual_ fieldName) (HsPVar (HsIdent fieldName))
             | QualifiedField (coerce -> fieldName) _ <- qualifiedFields ]

         encodeMessageE = apply mconcatE [ HsList encodeMessagePartEs ]
         decodeMessageE = foldl (\f -> HsInfixApp f apOp)
                                (apply pureE [ HsVar (unqual_ msgName) ])
                                decodeMessagePartEs

     pure (instDecl_ (protobufName "Message")
                     [ type_ msgName ]
                     (map (HsFunBind . (: []))
                      [ encodeMessageDecl
                      , decodeMessageDecl
                      , dotProtoDecl ]))

toJSONPBMessageInstD :: TypeContext
                     -> DotProtoIdentifier
                     -> DotProtoIdentifier
                     -> [DotProtoMessagePart]
                     -> CompileResult HsDecl
toJSONPBMessageInstD _ctxt parentIdent msgIdent messageParts = do
  msgName    <- nestedTypeName parentIdent =<< dpIdentUnqualName msgIdent
  qualFields <- getQualifiedFields msgName messageParts

  -- E.g.
  -- "another" .= f2 -- always succeeds (produces default value on missing field)
  let defPairE fldName fldNum =
        HsInfixApp (HsLit (HsString (coerce fldName)))
                   toJSONPBOp
                   (HsVar (unqual_ (fieldBinder fldNum)))
  -- E.g.
  -- HsJSONPB.pair "name" f4 -- fails on missing field
  let pairE fldNm varNm =
        apply (HsVar (jsonpbName "pair"))
              [ HsLit (HsString (coerce fldNm))
              , HsVar (unqual_ varNm)
              ]

  -- E.g.
  -- case f4_or_f9 of
  --   Just (SomethingPickOneName f4)
  --     -> HsJSONPB.pair "name" f4
  --   Just (SomethingPickOneSomeid f9)
  --     -> HsJSONPB.pair "someid" f9
  --   Nothing
  --     -> mempty
  let oneofCaseE (OneofField{subfields}) =
        HsCase disjunctName (altEs <> pure fallThruE)
        where
          altEs =
            [ let patVarNm = oneofSubBinder sub in
              alt_ (HsPApp (haskellName "Just")
                           [ HsPParen
                             $ HsPApp (unqual_ conName) [patVar patVarNm]
                           ]
                   )
                   (HsUnGuardedAlt (pairE pbFldNm patVarNm))
                   []
            | sub@(OneofSubfield _ conName pbFldNm _ _) <- subfields
            ]
          disjunctName = HsVar (unqual_ (oneofSubDisjunctBinder subfields))
          fallThruE =
            alt_ (HsPApp (haskellName "Nothing") [])
                 (HsUnGuardedAlt memptyE)
                 []

  let patBinder = onQF (const fieldBinder) (oneofSubDisjunctBinder . subfields)
  let applyE nm = apply (HsVar (jsonpbName nm)) [ HsList (onQF defPairE oneofCaseE <$> qualFields) ]

  let matchE nm appNm = match_ (HsIdent nm)
                          [ HsPApp (unqual_ msgName)
                                   (patVar . patBinder  <$> qualFields) ]
                          (HsUnGuardedRhs (applyE appNm))
                          []

  pure (instDecl_ (jsonpbName "ToJSONPB")
                  [ type_ msgName ]
                  [ HsFunBind [matchE "toJSONPB" "object"]
                  , HsFunBind [matchE "toEncodingPB" "pairs"]
                  ])

fromJSONPBMessageInstD :: TypeContext
                       -> DotProtoIdentifier
                       -> DotProtoIdentifier
                       -> [DotProtoMessagePart]
                       -> CompileResult HsDecl
fromJSONPBMessageInstD _ctxt parentIdent msgIdent messageParts = do
  msgName    <- nestedTypeName parentIdent =<< dpIdentUnqualName msgIdent
  qualFields <- getQualifiedFields msgName messageParts

  -- E.g., for message Something{ oneof name_or_id { string name = _; int32 someid = _; } }:
  -- Hs.msum
  --   [ Just . SomethingPickOneName   <$> (HsJSONPB.parseField obj "name")
  --   , Just . SomethingPickOneSomeid <$> (HsJSONPB.parseField obj "someid")
  --   , pure Nothing
  --   ]
  let oneofParserE fld =
        HsApp msumE (HsList ((subParserEs <> fallThruE) fld))
        where
          fallThruE OneofField{}
            = [ HsApp pureE (HsVar (haskellName "Nothing")) ]
          subParserEs OneofField{subfields}
            = subParserE <$> subfields
          subParserE OneofSubfield{subfieldConsName, subfieldName}
            = HsInfixApp (HsInfixApp
                            (HsVar (haskellName "Just"))
                            composeOp
                            (HsVar (unqual_ subfieldConsName)))
                         fmapOp
                         (apply (HsVar (jsonpbName "parseField"))
                                [ HsVar (unqual_ "obj")
                                , HsLit (HsString (coerce subfieldName))
                                ])

  -- E.g. obj .: "someid"
  let normalParserE fldNm _ =
        HsInfixApp (HsVar (unqual_ "obj"))
                   parseJSONPBOp
                   (HsLit (HsString (coerce fldNm)))

  let parseJSONPBE =
        apply (HsVar (jsonpbName "withObject"))
              [ HsLit (HsString msgName)
              , HsParen (HsLambda l [patVar "obj"] fieldAps)
              ]
        where
          fieldAps = foldl (\f -> HsInfixApp f apOp)
                           (apply pureE [ HsVar (unqual_ msgName) ])
                           (onQF normalParserE oneofParserE <$> qualFields)

  let parseJSONPBDecl =
        match_ (HsIdent "parseJSONPB") [] (HsUnGuardedRhs parseJSONPBE) []

  pure (instDecl_ (jsonpbName "FromJSONPB")
                 [ type_ msgName ]
                 [ HsFunBind [ parseJSONPBDecl ] ])

-- ** Codegen bookkeeping helpers

-- | Bookeeping for qualified fields
data QualifiedField = QualifiedField
  { recordFieldName :: FieldName
  , fieldInfo       :: FieldInfo
  } deriving Show

-- | Bookkeeping for fields
data FieldInfo
  = FieldOneOf OneofField
  | FieldNormal FieldName FieldNumber DotProtoType [DotProtoOption]
  deriving Show

-- | Bookkeeping for oneof fields
data OneofField = OneofField
  { subfields :: [OneofSubfield]
  } deriving Show

-- | Bookkeeping for oneof subfields
data OneofSubfield = OneofSubfield
  { subfieldNumber   :: FieldNumber
  , subfieldConsName :: String
  , subfieldName     :: FieldName
  , subfieldType     :: DotProtoType
  , subfieldOptions  :: [DotProtoOption]
  } deriving Show

getQualifiedFields :: String
                   -> [DotProtoMessagePart]
                   -> CompileResult [QualifiedField]
getQualifiedFields msgName msgParts = fmap catMaybes . forM msgParts $ \case
  DotProtoMessageField (DotProtoField fieldNum dpType fieldIdent options _) -> do
    fieldName <- dpIdentUnqualName fieldIdent
    qualName  <- prefixedFieldName msgName fieldName
    pure $ Just $
      QualifiedField (coerce qualName) (FieldNormal (coerce fieldName) fieldNum dpType options)
  DotProtoMessageOneOf _ [] ->
    Left (InternalError "getQualifiedFields: encountered oneof with no oneof fields")
  DotProtoMessageOneOf fieldIdent fields -> do
    fieldName  <- dpIdentUnqualName fieldIdent >>= prefixedFieldName msgName
    consName   <- dpIdentUnqualName fieldIdent >>= prefixedConName msgName
    fieldElems <- sequence
                    [ do s <- dpIdentUnqualName subFieldName
                         c <- prefixedConName consName s
                         pure (OneofSubfield fieldNum c (coerce s) dpType options)
                    | DotProtoField fieldNum dpType subFieldName options _ <- fields
                    ]
    pure $ Just $ QualifiedField (coerce fieldName) (FieldOneOf (OneofField fieldElems))
  _ ->
    pure Nothing

-- | Project qualified fields, given a projection function per field type.
onQF :: (FieldName -> FieldNumber -> a) -- ^ projection for normal fields
     -> (OneofField -> a)               -- ^ projection for oneof fields
     -> QualifiedField
     -> a
onQF f _ (QualifiedField _ (FieldNormal fldName fldNum _ _)) = f fldName fldNum
onQF _ g (QualifiedField _ (FieldOneOf fld))                 = g fld

fieldBinder :: FieldNumber -> String
fieldBinder = ("f" ++) . show

oneofSubBinder :: OneofSubfield -> String
oneofSubBinder = fieldBinder . subfieldNumber

oneofSubDisjunctBinder :: [OneofSubfield] -> String
oneofSubDisjunctBinder = intercalate "_or_" . fmap oneofSubBinder

-- ** Helpers to wrap/unwrap types for protobuf (de-)serialization

wrapE :: TypeContext -> DotProtoType -> [DotProtoOption] -> HsExp -> CompileResult HsExp
wrapE ctxt dpt opts e = case dpt of
  Prim ty
    -> pure (wrapPrimE ctxt ty e)
  Optional ty
    -> pure (wrapPrimE ctxt ty e)
  Repeated (Named tyName)
    | Just DotProtoKindMessage <- dotProtoTypeInfoKind <$> M.lookup tyName ctxt
      -> pure (wrapWithFuncE "NestedVec" e)
  Repeated ty
    | isUnpacked opts                     -> wrapVE "UnpackedVec" ty
    | isPacked opts || isPackable ctxt ty -> wrapVE "PackedVec"   ty
    | otherwise                           -> wrapVE "UnpackedVec" ty
  _ -> internalError "wrapE: unimplemented"
  where
    wrapVE nm ty = pure . wrapWithFuncE nm . wrapPrimVecE ty $ e

unwrapE :: TypeContext -> DotProtoType -> [DotProtoOption] -> HsExp -> CompileResult HsExp
unwrapE ctxt dpt opts e = case dpt of
 Prim ty
   -> pure (unwrapPrimE ctxt ty e)
 Optional ty
   -> pure (unwrapPrimE ctxt ty e)
 Repeated (Named tyName)
   | Just DotProtoKindMessage <- dotProtoTypeInfoKind <$> M.lookup tyName ctxt
     -> pure (unwrapWithFuncE "nestedvec" e)
 Repeated ty
   | isUnpacked opts                     -> unwrapVE ty "unpackedvec"
   | isPacked opts || isPackable ctxt ty -> unwrapVE ty "packedvec"
   | otherwise                           -> unwrapVE ty "unpackedvec"
 _ -> internalError "unwrapE: unimplemented"
 where
   unwrapVE ty nm = pure . unwrapPrimVecE ty . unwrapWithFuncE nm $ e

wrapPrimVecE, unwrapPrimVecE :: DotProtoPrimType -> HsExp -> HsExp
wrapPrimVecE SFixed32 = apply fmapE . (HsVar (protobufName "Signed"):) . (:[])
wrapPrimVecE SFixed64 = apply fmapE . (HsVar (protobufName "Signed"):) . (:[])
wrapPrimVecE _ = id

unwrapPrimVecE SFixed32 = HsParen .
    HsInfixApp (apply pureE [ apply fmapE [ HsVar (protobufName "signed") ] ]) apOp
unwrapPrimVecE SFixed64 = HsParen .
    HsInfixApp (apply pureE [ apply fmapE [ HsVar (protobufName "signed") ] ]) apOp
unwrapPrimVecE _ = id

wrapPrimE, unwrapPrimE :: TypeContext -> DotProtoPrimType -> HsExp -> HsExp
wrapPrimE ctxt (Named tyName)
    | Just DotProtoKindMessage <- dotProtoTypeInfoKind <$> M.lookup tyName ctxt
        = wrapWithFuncE "Nested"
wrapPrimE _ SInt32   = wrapWithFuncE "Signed"
wrapPrimE _ SInt64   = wrapWithFuncE "Signed"
wrapPrimE _ SFixed32 = wrapWithFuncE "Signed"
wrapPrimE _ SFixed64 = wrapWithFuncE "Signed"
wrapPrimE _ _        = id

unwrapPrimE ctxt (Named tyName)
    | Just DotProtoKindMessage <- dotProtoTypeInfoKind <$> M.lookup tyName ctxt
        = unwrapWithFuncE "nested"
unwrapPrimE _ SInt32   = unwrapWithFuncE "signed"
unwrapPrimE _ SInt64   = unwrapWithFuncE "signed"
unwrapPrimE _ SFixed32 = unwrapWithFuncE "signed"
unwrapPrimE _ SFixed64 = unwrapWithFuncE "signed"
unwrapPrimE _ _        = id

wrapWithFuncE, unwrapWithFuncE :: String -> HsExp -> HsExp
wrapWithFuncE wrappingFunc = HsParen . HsApp (HsVar (protobufName wrappingFunc))
unwrapWithFuncE unwrappingFunc = HsParen . HsInfixApp funcE apOp
  where funcE = HsParen (HsApp pureE (HsVar (protobufName unwrappingFunc)))

isPacked, isUnpacked :: [DotProtoOption] -> Bool
isPacked opts =
    case find (\(DotProtoOption name _) -> name == Single "packed") opts of
        Just (DotProtoOption _ (BoolLit x)) -> x
        _ -> False
isUnpacked opts =
    case find (\(DotProtoOption name _) -> name == Single "packed") opts of
        Just (DotProtoOption _ (BoolLit x)) -> not x
        _ -> False

-- | Returns 'True' if the given primitive type is packable. The 'TypeContext'
-- is used to distinguish Named enums and messages, only the former of which are
-- packable.
isPackable :: TypeContext -> DotProtoPrimType -> Bool
isPackable _ Bytes    = False
isPackable _ String   = False
isPackable _ Int32    = True
isPackable _ Int64    = True
isPackable _ SInt32   = True
isPackable _ SInt64   = True
isPackable _ UInt32   = True
isPackable _ UInt64   = True
isPackable _ Fixed32  = True
isPackable _ Fixed64  = True
isPackable _ SFixed32 = True
isPackable _ SFixed64 = True
isPackable _ Bool     = True
isPackable _ Float    = True
isPackable _ Double   = True
isPackable ctxt (Named tyName)
  | Just DotProtoKindEnum <- dotProtoTypeInfoKind <$> M.lookup tyName ctxt
    = True
  | otherwise
    = False

internalError, invalidTypeNameError, _unimplementedError
    :: String -> CompileResult a
internalError = Left . InternalError
invalidTypeNameError = Left . InvalidTypeName
_unimplementedError = Left . Unimplemented

invalidMethodNameError, noSuchTypeError :: DotProtoIdentifier -> CompileResult a
noSuchTypeError = Left . NoSuchType
invalidMethodNameError = Left . InvalidMethodName

-- ** Generate types and instances for .proto enums

dotProtoEnumD :: DotProtoIdentifier -> DotProtoIdentifier -> [DotProtoEnumPart]
              -> CompileResult [HsDecl]
dotProtoEnumD parentIdent enumIdent enumParts =
  do enumName <- nestedTypeName parentIdent =<<
                 dpIdentUnqualName enumIdent

     enumCons <- sortOn fst <$>
                 sequence [ (i,) <$> (prefixedEnumFieldName enumName =<<
                                      dpIdentUnqualName conIdent)
                          | DotProtoEnumField conIdent i _options <- enumParts ]

     let enumNameE = HsLit (HsString enumName)
         -- TODO assert that there is more than one enumeration constructor
         ((minEnumVal, maxEnumVal), enumConNames) = first (minimum &&& maximum) $ unzip enumCons

         boundsE = HsTuple
                     [ HsExpTypeSig l (intE minEnumVal) (HsQualType [] (HsTyCon (haskellName "Int")))
                     , intE maxEnumVal
                     ]

         toEnumD = toEnumDPatterns <> [ toEnumFailure ]
         fromEnumD =
             [ match_ (HsIdent "fromEnum") [ HsPApp (unqual_ conName) [] ]
                      (HsUnGuardedRhs (intE conIdx)) []
             | (conIdx, conName) <- enumCons ]
         succD = zipWith succDPattern enumConNames (tail enumConNames) <> [ succFailure ]
         predD = zipWith predDPattern (tail enumConNames) enumConNames <> [ predFailure ]

         toEnumDPatterns =
             [ match_ (HsIdent "toEnum")
                      [ intP conIdx ]
                      (HsUnGuardedRhs (HsVar (unqual_ conName))) []
             | (conIdx, conName) <- enumCons ]

         succDPattern thisCon nextCon =
             match_ (HsIdent "succ") [ HsPApp (unqual_ thisCon) [] ]
                    (HsUnGuardedRhs (HsVar (unqual_ nextCon))) []
         predDPattern thisCon prevCon =
             match_ (HsIdent "pred") [ HsPApp (unqual_ thisCon) [] ]
                    (HsUnGuardedRhs (HsVar (unqual_ prevCon))) []

         toEnumFailure   = match_ (HsIdent "toEnum") [ HsPVar (HsIdent "i") ]
                                  (HsUnGuardedRhs (apply toEnumErrorE
                                                          [enumNameE
                                                          , HsVar (unqual_ "i")
                                                          , boundsE])) []
         succFailure     = match_ (HsIdent "succ") [ HsPWildCard ]
                                  (HsUnGuardedRhs (HsApp succErrorE enumNameE)) []
         predFailure     = match_ (HsIdent "pred") [ HsPWildCard ]
                                  (HsUnGuardedRhs (HsApp predErrorE enumNameE)) []

         parseJSONPBDecls :: [HsMatch]
         parseJSONPBDecls =
           [ let pat nm =
                   HsPApp (jsonpbName "String")
                     [ HsPLit (HsString (fromMaybe <*> stripPrefix enumName $ nm)) ]
             in
             match_ (HsIdent "parseJSONPB") [pat conName]
                    (HsUnGuardedRhs
                       (HsApp pureE (HsVar (unqual_ conName))))
                    []
           | conName <- enumConNames
           ]
           <> [ match_ (HsIdent "parseJSONPB") [patVar "v"]
                       (HsUnGuardedRhs
                          (apply (HsVar (jsonpbName "typeMismatch"))
                                 [ HsLit (HsString enumName)
                                 , HsVar (unqual_ "v")
                                 ]))
                       []
              ]

         toJSONPBDecl =
           match_ (HsIdent "toJSONPB") [ patVar "x", HsPWildCard ]
             (HsUnGuardedRhs
                (HsApp (HsVar (jsonpbName "enumFieldString"))
                       (HsVar (unqual_ "x"))))
             []

         toEncodingPBDecl =
           match_ (HsIdent "toEncodingPB") [ patVar "x", HsPWildCard ]
             (HsUnGuardedRhs
                (HsApp (HsVar (jsonpbName "enumFieldEncoding"))
                       (HsVar (unqual_ "x"))))
             []

     pure [ dataDecl_ enumName
                      [ conDecl_ (HsIdent con) [] | con <- enumConNames ]
                      defaultEnumDeriving
          , namedInstD enumName
          , instDecl_ (haskellName "Enum") [ type_ enumName ]
                      [ HsFunBind toEnumD, HsFunBind fromEnumD
                      , HsFunBind succD, HsFunBind predD ]
          , instDecl_ (jsonpbName "ToJSONPB") [ type_ enumName ]
                      [ HsFunBind [toJSONPBDecl]
                      , HsFunBind [toEncodingPBDecl]
                      ]
          , instDecl_ (jsonpbName "FromJSONPB") [ type_ enumName ]
                      [ HsFunBind parseJSONPBDecls ]
          -- Generate Aeson instances in terms of JSONPB instances
          , toJSONInstDecl enumName
          , fromJSONInstDecl enumName
          -- And the Finite instance, used to infer a Swagger ToSchema instance
          -- for this enumerated type.
          , instDecl_ (protobufName "Finite") [ type_ enumName ] []
          ]

-- ** Generate code for dot proto services

dotProtoServiceD :: DotProtoIdentifier -> TypeContext -> DotProtoIdentifier
                 -> [DotProtoServicePart] -> CompileResult [HsDecl]
dotProtoServiceD pkgIdent ctxt serviceIdent service =
  do serviceNameUnqual <- dpIdentUnqualName serviceIdent
     packageName <- dpIdentQualName pkgIdent

     serviceName <- typeLikeName serviceNameUnqual

     let endpointPrefix = "/" ++ packageName ++ "." ++ serviceName ++ "/"

         serviceFieldD (DotProtoServiceRPC rpcName (request, requestStreaming)
                            (response, responseStreaming) _) =
           do fullName <- prefixedFieldName serviceName =<<
                          dpIdentUnqualName rpcName

              methodName <- case rpcName of
                              Single nm -> pure nm
                              _ -> invalidMethodNameError rpcName

              requestTy <- hsTypeFromDotProtoPrim ctxt  (Named request)
              responseTy <- hsTypeFromDotProtoPrim ctxt (Named response)

              let streamingType =
                    case (requestStreaming, responseStreaming) of
                      (Streaming, Streaming)       -> biDiStreamingC
                      (Streaming, NonStreaming)    -> clientStreamingC
                      (NonStreaming, Streaming)    -> serverStreamingC
                      (NonStreaming, NonStreaming) -> normalC

              pure [ ( endpointPrefix ++ methodName
                     , fullName, requestStreaming, responseStreaming
                     , HsUnBangedTy $
                       HsTyFun (tyApp (HsTyVar (HsIdent "request")) [streamingType, requestTy, responseTy])
                               (tyApp ioT [tyApp (HsTyVar (HsIdent "response")) [streamingType, responseTy]]))]
         serviceFieldD _ = pure []

     fieldsD <- mconcat <$> mapM serviceFieldD service

     serverFuncName <- prefixedFieldName serviceName "server"
     clientFuncName <- prefixedFieldName serviceName "client"

     let conDecl = recDecl_ (HsIdent serviceName)
                            [ ([HsIdent hsName], ty)
                            | (_, hsName, _, _, ty) <- fieldsD ]

         serverT = tyApp (HsTyCon (unqual_ serviceName))
                         [ serverRequestT, serverResponseT ]
         serviceServerTypeD = HsTypeSig l [ HsIdent serverFuncName ]
             (HsQualType [] (HsTyFun serverT (HsTyFun serviceOptionsC ioActionT)))

         serviceServerD =
             let serverFuncD = match_ (HsIdent serverFuncName)
                                      [ HsPRec (unqual_ serviceName)
                                            [ HsPFieldPat (unqual_ methodName)
                                                  (HsPVar (HsIdent methodName))
                                            | (_, methodName, _, _, _)
                                                  <- fieldsD ]
                                      , HsPApp (unqual_ "ServiceOptions")
                                               [ patVar "serverHost"
                                               , patVar "serverPort"
                                               , patVar "useCompression"
                                               , patVar "userAgentPrefix"
                                               , patVar "userAgentSuffix"
                                               , patVar "initialMetadata"
                                               , patVar "sslConfig"
                                               , patVar "logger"
                                               ]
                                      ]
                                      (HsUnGuardedRhs
                                           (apply serverLoopE [ serverOptsE ])) []

                 handlerE handlerC adapterE methodName hsName =
                     apply handlerC [ apply methodNameC [ HsLit (HsString methodName) ]
                                    , apply adapterE [ HsVar (unqual_ hsName) ] ]

                 update u v = HsFieldUpdate (unqual_ u) (HsVar (unqual_ v))

                 serverOptsE = HsRecUpdate defaultOptionsE
                     [ HsFieldUpdate (grpcName "optNormalHandlers")
                           (HsList [ handlerE unaryHandlerC convertServerHandlerE
                                              endpointName hsName
                                   | (endpointName, hsName, NonStreaming
                                     , NonStreaming, _) <- fieldsD ])

                     , HsFieldUpdate (grpcName "optClientStreamHandlers")
                           (HsList [ handlerE clientStreamHandlerC
                                              convertServerReaderHandlerE
                                              endpointName hsName
                                   | (endpointName, hsName, Streaming
                                     , NonStreaming, _) <- fieldsD ])

                     , HsFieldUpdate (grpcName "optServerStreamHandlers")
                           (HsList [ handlerE serverStreamHandlerC
                                              convertServerWriterHandlerE
                                              endpointName hsName
                                   | (endpointName, hsName, NonStreaming
                                     , Streaming, _) <- fieldsD ])

                     , HsFieldUpdate (grpcName "optBiDiStreamHandlers")
                           (HsList [ handlerE biDiStreamHandlerC
                                              convertServerRWHandlerE
                                              endpointName hsName
                                   | (endpointName, hsName, Streaming
                                     , Streaming, _) <- fieldsD ])
                     , update "optServerHost" "serverHost"
                     , update "optServerPort" "serverPort"
                     , update "optUseCompression" "useCompression"
                     , update "optUserAgentPrefix" "userAgentPrefix"
                     , update "optUserAgentSuffix" "userAgentSuffix"
                     , update "optInitialMetadata" "initialMetadata"
                     , update "optSSLConfig" "sslConfig"
                     , update "optLogger" "logger"
                     ]
             in HsFunBind [serverFuncD]

         clientT = tyApp (HsTyCon (unqual_ serviceName))
                         [ clientRequestT, clientResultT ]
         serviceClientTypeD = HsTypeSig l [ HsIdent clientFuncName ]
             (HsQualType [] (HsTyFun grpcClientT (HsTyApp ioT clientT)))

         serviceClientD =
             let clientFuncD = match_ (HsIdent clientFuncName)
                                      [ HsPVar (HsIdent "client") ]
                                      ( HsUnGuardedRhs clientRecE ) []
                 clientRecE = foldl (\f -> HsInfixApp f apOp)
                                    (apply pureE [ HsVar (unqual_ serviceName) ])
                                    [ HsParen $ HsInfixApp clientRequestE' apOp
                                        (registerClientMethodE endpointName)
                                    | (endpointName, _, _, _, _) <- fieldsD ]
                 clientRequestE' = apply pureE [ apply clientRequestE [ HsVar (unqual_ "client") ] ]
                 registerClientMethodE endpoint =
                   apply clientRegisterMethodE [ HsVar (unqual_ "client")
                                               , apply methodNameC
                                                   [ HsLit (HsString endpoint) ] ]
             in HsFunBind [ clientFuncD ]

     pure [ HsDataDecl l  [] (HsIdent serviceName)
                [ HsIdent "request", HsIdent "response" ]
                [ conDecl ] defaultServiceDeriving

          , serviceServerTypeD
          , serviceServerD

          , serviceClientTypeD
          , serviceClientD ]

-- * Common Haskell expressions, constructors, and operators

dotProtoFieldC, primC, optionalC, repeatedC, nestedRepeatedC, namedC,
  fieldNumberC, singleC, dotsC, pathC, nestedC, anonymousC, dotProtoOptionC,
  identifierC, stringLitC, intLitC, floatLitC, boolLitC, trueC, falseC,
  unaryHandlerC, clientStreamHandlerC, serverStreamHandlerC, biDiStreamHandlerC,
  methodNameC, nothingC, justC, forceEmitC, mconcatE, encodeMessageFieldE,
  fromStringE, decodeMessageFieldE, pureE, memptyE, msumE, atE, oneofE,
  succErrorE, predErrorE, toEnumErrorE, fmapE, defaultOptionsE, serverLoopE,
  convertServerHandlerE, convertServerReaderHandlerE, convertServerWriterHandlerE,
  convertServerRWHandlerE, clientRegisterMethodE, clientRequestE :: HsExp

dotProtoFieldC       = HsVar (protobufName "DotProtoField")
primC                = HsVar (protobufName "Prim")
optionalC            = HsVar (protobufName "Optional")
repeatedC            = HsVar (protobufName "Repeated")
nestedRepeatedC      = HsVar (protobufName "NestedRepeated")
namedC               = HsVar (protobufName "Named")
fieldNumberC         = HsVar (protobufName "FieldNumber")
singleC              = HsVar (protobufName "Single")
pathC                = HsVar (protobufName "Path")
dotsC                = HsVar (protobufName "Dots")
nestedC              = HsVar (protobufName "Nested")
anonymousC           = HsVar (protobufName "Anonymous")
dotProtoOptionC      = HsVar (protobufName "DotProtoOption")
identifierC          = HsVar (protobufName "Identifier")
stringLitC           = HsVar (protobufName "StringLit")
intLitC              = HsVar (protobufName "IntLit")
floatLitC            = HsVar (protobufName "FloatLit")
boolLitC             = HsVar (protobufName "BoolLit")
trueC                = HsVar (haskellName "True")
falseC               = HsVar (haskellName "False")
unaryHandlerC        = HsVar (grpcName "UnaryHandler")
clientStreamHandlerC = HsVar (grpcName "ClientStreamHandler")
serverStreamHandlerC = HsVar (grpcName "ServerStreamHandler")
biDiStreamHandlerC   = HsVar (grpcName "BiDiStreamHandler")
methodNameC          = HsVar (grpcName "MethodName")
nothingC             = HsVar (haskellName "Nothing")
justC                = HsVar (haskellName "Just")
forceEmitC           = HsVar (protobufName "ForceEmit")

encodeMessageFieldE         = HsVar (protobufName "encodeMessageField")
decodeMessageFieldE         = HsVar (protobufName "decodeMessageField")
atE                         = HsVar (protobufName "at")
oneofE                      = HsVar (protobufName "oneof")
mconcatE                    = HsVar (haskellName "mconcat")
fromStringE                 = HsVar (haskellName "fromString")
pureE                       = HsVar (haskellName "pure")
memptyE                     = HsVar (haskellName "mempty")
msumE                       = HsVar (haskellName "msum")
succErrorE                  = HsVar (haskellName "succError")
predErrorE                  = HsVar (haskellName "predError")
toEnumErrorE                = HsVar (haskellName "toEnumError")
fmapE                       = HsVar (haskellName "fmap")
defaultOptionsE             = HsVar (grpcName "defaultOptions")
serverLoopE                 = HsVar (grpcName "serverLoop")
convertServerHandlerE       = HsVar (grpcName "convertGeneratedServerHandler")
convertServerReaderHandlerE = HsVar (grpcName "convertGeneratedServerReaderHandler")
convertServerWriterHandlerE = HsVar (grpcName "convertGeneratedServerWriterHandler")
convertServerRWHandlerE     = HsVar (grpcName "convertGeneratedServerRWHandler")
clientRegisterMethodE       = HsVar (grpcName "clientRegisterMethod")
clientRequestE              = HsVar (grpcName "clientRequest")

biDiStreamingC, serverStreamingC, clientStreamingC, normalC, serviceOptionsC,
  ioActionT, serverRequestT, serverResponseT, clientRequestT, clientResultT,
  ioT, grpcClientT :: HsType
biDiStreamingC   = HsTyCon (Qual (Module "'HsGRPC") (HsIdent "BiDiStreaming"))
serverStreamingC = HsTyCon (Qual (Module "'HsGRPC") (HsIdent "ServerStreaming"))
clientStreamingC = HsTyCon (Qual (Module "'HsGRPC") (HsIdent "ClientStreaming"))
normalC          = HsTyCon (Qual (Module "'HsGRPC") (HsIdent "Normal"))
serviceOptionsC  = HsTyCon (Qual (Module "HsGRPC") (HsIdent "ServiceOptions"))
serverRequestT   = HsTyCon (grpcName "ServerRequest")
serverResponseT  = HsTyCon (grpcName "ServerResponse")
clientRequestT   = HsTyCon (grpcName "ClientRequest")
clientResultT    = HsTyCon (grpcName "ClientResult")
ioActionT        = tyApp ioT [ HsTyTuple [] ]
ioT              = HsTyCon (haskellName "IO")
grpcClientT      = HsTyCon (grpcName "Client")

apOp :: HsQOp
apOp  = HsQVarOp (UnQual (HsSymbol "<*>"))

fmapOp :: HsQOp
fmapOp  = HsQVarOp (UnQual (HsSymbol "<$>"))

composeOp :: HsQOp
composeOp = HsQVarOp (Qual haskellNS (HsSymbol "."))

toJSONPBOp :: HsQOp
toJSONPBOp = HsQVarOp (UnQual (HsSymbol ".="))

parseJSONPBOp :: HsQOp
parseJSONPBOp = HsQVarOp (UnQual (HsSymbol ".:"))

intE :: Integral a => a -> HsExp
intE x = (if x < 0 then HsParen else id) . HsLit . HsInt . fromIntegral $ x

intP :: Integral a => a -> HsPat
intP x = (if x < 0 then HsPParen else id) . HsPLit . HsInt . fromIntegral $ x

toJSONInstDecl :: String -> HsDecl
toJSONInstDecl typeName =
  instDecl_ (jsonpbName "ToJSON")
            [ type_ typeName ]
            [ HsFunBind [ match_ (HsIdent "toJSON") []
                                 (HsUnGuardedRhs (HsVar (jsonpbName "toAesonValue"))) []
                        ]
            , HsFunBind [ match_ (HsIdent "toEncoding") []
                                 (HsUnGuardedRhs (HsVar (jsonpbName "toAesonEncoding"))) []
                        ]
            ]


fromJSONInstDecl :: String -> HsDecl
fromJSONInstDecl typeName =
  instDecl_ (jsonpbName "FromJSON")
            [ type_ typeName ]
            [ HsFunBind [match_ (HsIdent "parseJSON") [] (HsUnGuardedRhs (HsVar (jsonpbName "parseJSONPB"))) []
                        ]
            ]

toSchemaInstDecl :: String -> HsDecl
toSchemaInstDecl typeName =
  instDecl_ (jsonpbName "ToSchema")
            [ type_ typeName ]
            [ HsFunBind [match_ (HsIdent "declareNamedSchema") []
                                (HsUnGuardedRhs (HsVar (jsonpbName "genericDeclareNamedSchemaJSONPB"))) []
                        ]
            ]

-- ** Expressions for protobuf-wire types

forceEmitE :: HsExp -> HsExp
forceEmitE = HsParen . HsApp forceEmitC

fieldNumberE :: FieldNumber -> HsExp
fieldNumberE = HsParen . HsApp fieldNumberC . intE . getFieldNumber

maybeE :: (a -> HsExp) -> Maybe a -> HsExp
maybeE _ Nothing = nothingC
maybeE f (Just a) = HsApp justC (f a)

dpIdentE :: DotProtoIdentifier -> HsExp
dpIdentE (Single n)       = apply singleC [ HsLit (HsString n) ]
dpIdentE (Dots (Path ns)) = apply dotsC [apply pathC [ HsList (map (HsLit . HsString) ns) ] ]
dpIdentE (Qualified a b)  = apply nestedC [ dpIdentE a, dpIdentE b ]
dpIdentE Anonymous        = anonymousC

dpValueE :: DotProtoValue -> HsExp
dpValueE (Identifier nm) = apply identifierC [ dpIdentE nm ]
dpValueE (StringLit s)   = apply stringLitC  [ HsLit (HsString s) ]
dpValueE (IntLit i)      = apply intLitC     [ HsLit (HsInt (fromIntegral i)) ]
dpValueE (FloatLit f)    = apply floatLitC   [ HsLit (HsFrac (toRational f)) ]
dpValueE (BoolLit True)  = apply boolLitC    [ trueC ]
dpValueE (BoolLit False) = apply boolLitC    [ falseC ]

optionE :: DotProtoOption -> HsExp
optionE (DotProtoOption name value) = apply dotProtoOptionC [ dpIdentE name
                                                            , dpValueE value ]

dpTypeE :: DotProtoType -> CompileResult HsExp
dpTypeE (Prim p)           = pure (apply primC           [ dpPrimTypeE p ])
dpTypeE (Optional p)       = pure (apply optionalC       [ dpPrimTypeE p ])
dpTypeE (Repeated p)       = pure (apply repeatedC       [ dpPrimTypeE p ])
dpTypeE (NestedRepeated p) = pure (apply nestedRepeatedC [ dpPrimTypeE p ])
dpTypeE (Map _ _)          = internalError "dpTypeE: Map"

dpPrimTypeE :: DotProtoPrimType -> HsExp
dpPrimTypeE (Named named) = apply namedC [ dpIdentE named ]
dpPrimTypeE ty            =
    HsVar . protobufName $
    case ty of
        Int32    -> "Int32"
        Int64    -> "Int64"
        SInt32   -> "SInt32"
        SInt64   -> "SInt64"
        UInt32   -> "UInt32"
        UInt64   -> "UInt64"
        Fixed32  -> "Fixed32"
        Fixed64  -> "Fixed64"
        SFixed32 -> "SFixed32"
        SFixed64 -> "SFixed64"
        String   -> "String"
        Bytes    -> "Bytes"
        Bool     -> "Bool"
        Float    -> "Float"
        Double   -> "Double"

        -- 'error' okay because this is literally impossible
        Named _  -> error "dpPrimTypeE: impossible"

defaultImports :: Bool -> [HsImportDecl]
defaultImports usesGrpc =
  [ importDecl_ preludeM                  True  (Just haskellNS) Nothing
  , importDecl_ dataProtobufWireDotProtoM True  (Just protobufNS) Nothing
  , importDecl_ dataProtobufWireTypesM    True  (Just protobufNS) Nothing
  , importDecl_ dataProtobufWireClassM    True  (Just protobufNS) Nothing
  , importDecl_ proto3SuiteJSONPBM        True  (Just jsonpbNS) Nothing
  , importDecl_ proto3SuiteJSONPBM        False  Nothing
                (Just (False, [ HsIAbs (HsSymbol ".=")
                              , HsIAbs (HsSymbol ".:") ]))
  , importDecl_ proto3WireM               True  (Just protobufNS) Nothing
  , importDecl_ controlApplicativeM       False Nothing
                (Just (False, [ HsIAbs (HsSymbol "<*>")
                              , HsIAbs (HsSymbol "<|>")
                              , HsIAbs (HsSymbol "<$>")
                              ]
                      )
                )
  , importDecl_ controlDeepSeqM           True  (Just haskellNS) Nothing
  , importDecl_ controlMonadM             True  (Just haskellNS) Nothing
  , importDecl_ dataTextM                 True
                (Just haskellNS) (Just (False, [ importSym "Text" ]))
  , importDecl_ dataByteStringM           True  (Just haskellNS) Nothing
  , importDecl_ dataStringM               True  (Just haskellNS)
                (Just (False, [ importSym "fromString" ]))
  , importDecl_ dataVectorM               True  (Just haskellNS)
                (Just (False, [ importSym "Vector" ]))
  , importDecl_ dataIntM                  True  (Just haskellNS)
                (Just (False, [ importSym "Int16", importSym "Int32"
                              , importSym "Int64" ]))
  , importDecl_ dataWordM                 True  (Just haskellNS)
                (Just (False, [ importSym "Word16", importSym "Word32"
                              , importSym "Word64" ]))
  , importDecl_ ghcGenericsM              True (Just haskellNS) Nothing
  , importDecl_ ghcEnumM                  True (Just haskellNS) Nothing
  ] <>
  if usesGrpc
    then [ importDecl_ networkGrpcHighLevelGeneratedM   False (Just grpcNS) Nothing
         , importDecl_ networkGrpcHighLevelClientM      False (Just grpcNS) Nothing
         , importDecl_ networkGrpcHighLevelServerM      False (Just grpcNS)
               (Just (True, [ importSym "serverLoop" ]))
         , importDecl_ networkGrpcHighLevelServerUnregM False (Just grpcNS)
               (Just (False, [ importSym "serverLoop" ]))
         , importDecl_ networkGrpcLowLevelCallM         False (Just grpcNS) Nothing  ]
    else []
  where preludeM                  = Module "Prelude"
        dataProtobufWireDotProtoM = Module "Proto3.Suite.DotProto"
        dataProtobufWireClassM    = Module "Proto3.Suite.Class"
        dataProtobufWireTypesM    = Module "Proto3.Suite.Types"
        proto3SuiteJSONPBM        = Module "Proto3.Suite.JSONPB"
        proto3WireM               = Module "Proto3.Wire"
        controlApplicativeM       = Module "Control.Applicative"
        controlDeepSeqM           = Module "Control.DeepSeq"
        controlMonadM             = Module "Control.Monad"
        dataTextM                 = Module "Data.Text.Lazy"
        dataByteStringM           = Module "Data.ByteString"
        dataStringM               = Module "Data.String"
        dataIntM                  = Module "Data.Int"
        dataVectorM               = Module "Data.Vector"
        dataWordM                 = Module "Data.Word"
        ghcGenericsM              = Module "GHC.Generics"
        ghcEnumM                  = Module "GHC.Enum"
        networkGrpcHighLevelGeneratedM   = Module "Network.GRPC.HighLevel.Generated"
        networkGrpcHighLevelServerM      = Module "Network.GRPC.HighLevel.Server"
        networkGrpcHighLevelClientM      = Module "Network.GRPC.HighLevel.Client"
        networkGrpcHighLevelServerUnregM = Module "Network.GRPC.HighLevel.Server.Unregistered"
        networkGrpcLowLevelCallM         = Module "Network.GRPC.LowLevel.Call"

        grpcNS                    = Module "HsGRPC"
        jsonpbNS                  = Module "HsJSONPB"
        protobufNS                = Module "HsProtobuf"

        importSym = HsIAbs . HsIdent

haskellNS :: Module
haskellNS = Module "Hs"

defaultMessageDeriving, defaultEnumDeriving, defaultServiceDeriving :: [HsQName]
defaultMessageDeriving = map haskellName [ "Show"
                                         , "Eq",   "Ord"
                                         , "Generic"
                                         , "NFData" ]

defaultEnumDeriving = map haskellName [ "Show", "Bounded"
                                      , "Eq",   "Ord"
                                      , "Generic"
                                      , "NFData" ]

defaultServiceDeriving = map haskellName [ "Generic" ]

-- * Wrappers around haskell-src-exts constructors

apply :: HsExp -> [HsExp] -> HsExp
apply f = HsParen . foldl HsApp f

tyApp :: HsType -> [HsType] -> HsType
tyApp = foldl HsTyApp

module_ :: Module -> Maybe [HsExportSpec] -> [HsImportDecl] -> [HsDecl]
        -> HsModule
module_ = HsModule l

importDecl_ :: Module -> Bool -> Maybe Module
            -> Maybe (Bool, [HsImportSpec]) -> HsImportDecl
importDecl_ = HsImportDecl l

dataDecl_ :: String -> [HsConDecl] -> [HsQName] -> HsDecl
dataDecl_ messageName = HsDataDecl l [] (HsIdent messageName) []

recDecl_ :: HsName -> [([HsName], HsBangType)] -> HsConDecl
recDecl_ = HsRecDecl l

conDecl_ :: HsName -> [HsBangType] -> HsConDecl
conDecl_ = HsConDecl l

instDecl_ :: HsQName -> [HsType] -> [HsDecl] -> HsDecl
instDecl_ = HsInstDecl l []

match_ :: HsName -> [HsPat] -> HsRhs -> [HsDecl] -> HsMatch
match_ = HsMatch l

unqual_ :: String -> HsQName
unqual_ = UnQual . HsIdent

protobufType_, primType_ :: String -> HsType
protobufType_ = HsTyCon . protobufName
primType_ = HsTyCon . haskellName

type_ :: String -> HsType
type_ = HsTyCon . unqual_

patVar :: String -> HsPat
patVar =  HsPVar . HsIdent

alt_ :: HsPat -> HsGuardedAlts -> [HsDecl] -> HsAlt
alt_ = HsAlt l

-- | For some reason, haskell-src-exts needs this 'SrcLoc' parameter
--   for some data constructors. Its value does not affect
--   pretty-printed output
l :: SrcLoc
l = SrcLoc "<generated>" 0 0

__nowarn_unused :: a
__nowarn_unused = subfieldType `undefined` subfieldOptions
