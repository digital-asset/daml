{ lib, runCommand, jre, openssl, cacert, addnCerts ? [], ... }:

let
  addnCertsScript = addnCertPaths: lib.concatMapStringsSep "\n" (certPath: ''
    alias=$(openssl x509 -noout -in ${certPath} -subject| cut -d "/" -f 2 | cut -d "=" -f 2)
    echo "Adding $alias"
    ${jre}/bin/keytool -import -noprompt -alias "$alias" -file ${certPath} -keystore $out -storepass 'changeit'
  '') addnCertPaths;

in runCommand "java-truststore" {
  nativeBuildInputs = [ jre openssl ];
} ''
  if [ -f ${jre}/lib/openjdk/jre/lib/security/cacerts ]; then
    # When using openJDK
    cp ${jre}/lib/openjdk/jre/lib/security/cacerts $out
    chmod 644 $out
  elif [ -f ${jre}/jre/lib/security/cacerts ]; then
    # When using Oracle JDK
    cp ${jre}/jre/lib/security/cacerts $out
    chmod 644 $out
  else
    echo "ERROR: JDK has no cacerts" >&2
    exit 1
  fi
  ${addnCertsScript addnCerts}
  chmod 444 $out
''
