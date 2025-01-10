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

# Get xmlschema path based on platform
if os.name == 'nt':  # Windows check
    xmlschema_path = os.path.join(os.environ.get('pythonLocation', ''), 'Lib\\site-packages\\xmlschema\\schemas')
else:
    xmlschema_path = os.path.join(os.environ.get('pythonLocation', ''), 'lib/python3.9/site-packages/xmlschema/schemas')

# Add our data files to numpy_datas
numpy_datas.extend([
    (xmlschema_path, 'xmlschema/schemas'),
    ('resources/cache', 'resources/cache'),
    ('resources/templates', 'resources/templates'),
    ('resources/schema', 'resources/schema')
])

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
    a.binaries,
    a.zipfiles,
    a.datas,
    debug=False,
    strip=False,
    upx=True,
    name='core',
    console=True,
)