
{ stdenv, buildPythonPackage, fetchPypi, lib
, pkgconfig, runCommand
, six, protobuf3_5_1, enum34, futures, isPy26, isPy27, isPy34 }:


let
  version = "1.9.1";
  grpcio-src = fetchPypi {
    pname = "grpcio";
    inherit version;
    sha256 = "0p8ag6pmnp83qbin1w8hz6bjgsxyqhc0c6vw2z64izyy35b3pi77";
  };
  grpcio-tools-src = fetchPypi {
    pname = "grpcio-tools";
    inherit version;
    sha256 = "0gv7a0jy2waa1jc32bvqahpm6pmbgvzpnp28lw0gz5csabbrndhh";
  };

in rec {

  grpcio = buildPythonPackage rec {
    name = "${pname}-${version}";
    pname = "grpcio";
    inherit version;
    src = grpcio-src;

    preBuild = if stdenv.isDarwin then ''export AR="/usr/bin/libtool -o"'' else "";

    buildInputs = [ pkgconfig ];
    propagatedBuildInputs = [ six protobuf3_5_1 ]
                          ++ lib.optionals (isPy26 || isPy27 || isPy34) [ enum34 ]
                          ++ lib.optionals (isPy26 || isPy27) [ futures ];

    meta = with stdenv.lib; {
      description = "HTTP/2-based RPC framework";
      license = lib.licenses.bsd3;
      homepage = "https://grpc.io/grpc/python/";
      maintainers = with maintainers; [ vanschelven ];
    };
  };

  grpcio-tools = buildPythonPackage rec {
    name = "${pname}-${version}";
    pname = "grpcio-tools";
    inherit version;
    src = grpcio-tools-src;

    preBuild =
      if stdenv.isDarwin
      then ''
        export AR="/usr/bin/libtool -o"
        export CC=clang++
      '' else "";

    buildInputs = [ pkgconfig ];
    propagatedBuildInputs = [ protobuf3_5_1 grpcio ];

    meta = with stdenv.lib; {
      description = "gRPC Python Tools";
      license = lib.licenses.bsd3;
      homepage = "https://grpc.io/grpc/python/";
    };
  };
}
