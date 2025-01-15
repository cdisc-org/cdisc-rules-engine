# -*- mode: python ; coding: utf-8 -*-
from PyInstaller.utils.hooks import collect_submodules, collect_all
import os

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

if os.name == 'nt':
    xmlschema_path = os.path.join(os.environ.get('pythonLocation', ''), 'Lib\\site-packages\\xmlschema\\schemas')
else:
    xmlschema_path = os.path.join(os.environ.get('pythonLocation', ''), 'lib/python3.9/site-packages/xmlschema/schemas')
numpy_datas.extend([
    (xmlschema_path, 'xmlschema/schemas'),
    ('resources/cache', 'resources/cache'),
    ('resources/templates', 'resources/templates'),
    ('resources/schema', 'resources/schema')
])
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
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
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