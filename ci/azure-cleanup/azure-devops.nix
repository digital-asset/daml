{ pkgs, python3 }:

python3.pkgs.buildPythonPackage rec {
  pname = "azure-devops";
  version = "5.0.0b4";

  src = pkgs.fetchFromGitHub {
    owner = "Microsoft";
    repo = "azure-devops-python-api";
    rev = version;
    sha256 = "0g6p839ssn75ly4n4a2vdjfivb37yfv1328k28azm0r4grz3l694";
  };

  postUnpack = ''
    rm -R source/scripts
    mv source/azure-devops/* source
    rmdir source/azure-devops
  '';

  propagatedBuildInputs = [
    python3.pkgs.msrest
  ];

  doCheck = false;

  meta = with pkgs.stdenv.lib; {
    description = "Azure DevOps Python API";
    homepage = "https://github.com/Microsoft/azure-devops-python-api";
    license = licenses.mit;
  };
}
