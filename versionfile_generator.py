import re


def _read_app_version() -> str:
    with open("global_vars.py", "r", encoding="utf-8") as f:
        data = f.read()
    match = re.search(r'APP_VERSION\s*=\s*"([^"]+)"', data)
    if not match:
        raise RuntimeError("APP_VERSION não encontrado em global_vars.py")
    return match.group(1)

def _version_tuple(version: str) -> str:
    parts = [int(p) for p in version.split(".") if p.strip().isdigit()]
    while len(parts) < 4:
        parts.append(0)
    return ", ".join(str(p) for p in parts[:4])


def versionfile_generator():
    app_version = _read_app_version()
    ver_tuple = _version_tuple(app_version)
    template = f"""
VSVersionInfo(
  ffi=FixedFileInfo(
    filevers=({ver_tuple}),
    prodvers=({ver_tuple}),
    mask=0x3f,
    flags=0x0,
    OS=0x40004,
    fileType=0x1,
    subtype=0x0,
    date=(0, 0)
    ),
  kids=[
    StringFileInfo(
      [
      StringTable(
        '040904B0',
        [StringStruct('CompanyName', 'MVA'),
        StringStruct('FileDescription', 'Relatório de Clientes por Vendedor'),
        StringStruct('FileVersion', '{app_version}'),
        StringStruct('InternalName', 'RelatorioClientes'),
        StringStruct('LegalCopyright', '© 2025 MVA'),
        StringStruct('OriginalFilename', 'RelatorioClientes.exe'),
        StringStruct('ProductName', 'Sistema de Relatórios'),
        StringStruct('ProductVersion', '{app_version}')])
      ]),
    VarFileInfo([VarStruct('Translation', [1033, 1200])])
  ]
)
"""
    with open("version.txt", "w", encoding="utf-8") as f:
        f.write(template)

if __name__ == "__main__":
    versionfile_generator()
