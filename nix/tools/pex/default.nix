# This has been submitted as a PR for upstream, please see:
# https://github.com/NixOS/nixpkgs/pull/45497.
{ stdenv, python3Packages }:
with python3Packages; buildPythonApplication rec {
    name = "${pname}-${version}";
    pname  = "pex";
    version = "1.5.3";

    src = fetchPypi {
      sha256 = "1gf4a1cyih6mqc895ncmbib06d8k5wm1xdiwlzy9h98p4ng4q950";
      inherit pname version;
    };

    propagatedBuildInputs = [ requests wheel ];

    prePatch = ''
      substituteInPlace setup.py --replace 'SETUPTOOLS_REQUIREMENT,' '"setuptools",'
      substituteInPlace setup.py --replace 'WHEEL_REQUIREMENT,' '"wheel",'
    '';
    patches = [ ./set-source-date-epoch.patch ];

    # A few more dependencies I don't want to handle right now...
    doCheck = false;

    meta = with stdenv.lib; {
      description = "A library and tool for generating .pex (Python EXecutable) files";
      homepage = "https://github.com/pantsbuild/pex";
      license = licenses.asl20;
      maintainers = with maintainers; [ copumpkin ];
    };
  }
