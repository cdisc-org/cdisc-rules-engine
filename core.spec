# -*- mode: python ; coding: utf-8 -*-
from PyInstaller.utils.hooks import collect_submodules, collect_all

# Collect all required imports
hiddenimports = []
hiddenimports += collect_submodules('pyreadstat')
hiddenimports += collect_submodules('worker')
hiddenimports += collect_submodules('numpy')

# Collect numpy binaries and data
numpy_bins = []
numpy_datas = []
collected = collect_all('numpy')
numpy_bins.extend(collected[0])
numpy_datas.extend(collected[1])
hiddenimports.extend(collected[2])

block_cipher = None

a = Analysis(
    ['core.py'],
    pathex=[],
    binaries=numpy_bins,
    datas=numpy_datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='core',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='core',
)