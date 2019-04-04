Testing under Windows:

1) Install Python (pip should be part of that)
2) Install shelltestrunner: stack install shelltestrunner
3) Run run-windows-tests.py

To make the test automatically read email/username/password
for `da setup`, create a file in the tests-windows directory.
The file should contain email/username/password in separate
lines.
