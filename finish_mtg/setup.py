import sys
from cx_Freeze import setup, Executable

# # Dependencies are automatically detected, but it might need fine tuning.
# build_exe_options = {"packages": ["os"], "excludes": ["tkinter"]}

# GUI applications require a different base on Windows (the default is for a
# console application).
base = None
if sys.platform == "win32":
    base = "Win32GUI"
#
# setup(  name = "test",
#         version = "0.1",
#         description = "My GUI application!",
#         options = {"build_exe": build_exe_options},
#         executables = [Executable("guifoo.py", base=base)])

exe = Executable(script="main.py", base=base)
buildOptions = dict(excludes=["tkinter"], includes=["idna.idnadata", "certifi", ], optimize=1)
setup(name="MTG", version="1.0", description="test", executables=[exe], options=dict(build_exe=buildOptions))
