import re


def _read_app_version() -> str:
    with open("global_vars.py", "r", encoding="utf-8") as file:
        data = file.read()
    match = re.search(r'APP_VERSION\s*=\s*"([^"]+)"', data)
    if not match:
        raise RuntimeError("APP_VERSION not found in global_vars.py")
    return match.group(1)


def _version_tuple(version: str) -> str:
    parts = [int(part) for part in version.split(".") if part.strip().isdigit()]
    while len(parts) < 4:
        parts.append(0)
    return ", ".join(str(part) for part in parts[:4])


def versionfile_generator() -> None:
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
        StringStruct('FileDescription', 'Customer Sales Report'),
        StringStruct('FileVersion', '{app_version}'),
        StringStruct('InternalName', 'RelatorioClientes'),
        StringStruct('LegalCopyright', '(c) 2026 MVA'),
        StringStruct('OriginalFilename', 'RelatorioClientes.exe'),
        StringStruct('ProductName', 'Sales Report System'),
        StringStruct('ProductVersion', '{app_version}')])
      ]),
    VarFileInfo([VarStruct('Translation', [1033, 1200])])
  ]
)
"""
    with open("version.txt", "w", encoding="utf-8") as file:
        file.write(template)


if __name__ == "__main__":
    versionfile_generator()
