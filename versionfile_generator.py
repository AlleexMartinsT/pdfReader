from global_vars import APP_VERSION

def versionfile_generator():
    template = f"""
VSVersionInfo(
  ffi=FixedFileInfo(
    filevers=({APP_VERSION.replace('.', ',')}, 0),
    prodvers=({APP_VERSION.replace('.', ',')}, 0),
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
        StringStruct('FileVersion', '{APP_VERSION}'),
        StringStruct('InternalName', 'RelatorioClientes'),
        StringStruct('LegalCopyright', '© 2025 MVA'),
        StringStruct('OriginalFilename', 'RelatorioClientes.exe'),
        StringStruct('ProductName', 'Sistema de Relatórios'),
        StringStruct('ProductVersion', '{APP_VERSION}')])
      ]),
    VarFileInfo([VarStruct('Translation', [1033, 1200])])
  ]
)
"""
    with open("version.txt", "w", encoding="utf-8") as f:
        f.write(template)

if __name__ == "__main__":
    versionfile_generator()