{ python3Packages }: [
  (python3Packages.fetchPypi {
    pname = "certifi";
    version = "2018.10.15";
    format = "wheel";
    python = "py2.py3";
    sha256 = "339dc09518b07e2fa7eda5450740925974815557727d6bd35d319c1524a04a4c";
  })
  (python3Packages.fetchPypi {
    pname = "pip";
    version = "18.1";
    format = "wheel";
    python = "py2.py3";
    sha256 = "7909d0a0932e88ea53a7014dfd14522ffef91a464daaaf5c573343852ef98550";
  })
  (python3Packages.fetchPypi {
    pname = "pipenv";
    version = "2018.11.14";
    format = "wheel";
    python = "py3";
    sha256 = "117b0015837578cef25ee09c9d38094245966cbff92f0984e0f0fbebdbb5bc75";
  })
  (python3Packages.fetchPypi {
    pname = "setuptools";
    version = "40.6.2";
    format = "wheel";
    python = "py2.py3";
    sha256 = "88ee6bcd5decec9bd902252e02e641851d785c6e5e75677d2744a9d13fed0b0a";
  })
  (python3Packages.fetchPypi {
    pname = "virtualenv";
    version = "16.1.0";
    format = "wheel";
    python = "py2.py3";
    sha256 = "686176c23a538ecc56d27ed9d5217abd34644823d6391cbeb232f42bf722baad";
  })
  (python3Packages.fetchPypi {
    pname = "virtualenv_clone";
    version = "0.4.0";
    format = "wheel";
    python = "py2.py3";
    sha256 = "afce268508aa5596c90dda234abe345deebc401a57d287bcbd76baa140a1aa58";
  })
]
