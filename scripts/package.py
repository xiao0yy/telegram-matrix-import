#!/usr/bin/env python3
"""Package only the executable, public docs and dependency license notices."""
import hashlib
import json
import os
import pathlib
import shutil
import struct
import subprocess
import tarfile
import tempfile
import urllib.parse

ROOT = pathlib.Path(__file__).resolve().parent.parent
GO = os.environ.get('GO', 'go')


def output(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True)


def modules():
    text = output(GO, 'list', '-deps', '-json', '-tags',
                  'goolm,netgo,osusergo,sqlite_omit_load_extension', './cmd/tg-import')
    decoder = json.JSONDecoder()
    seen = set()
    while text.strip():
        package, end = decoder.raw_decode(text.lstrip())
        text = text.lstrip()[end:]
        mod = package.get('Module')
        if mod and not mod.get('Main') and mod['Path'] not in seen:
            seen.add(mod['Path'])
            yield mod


def main():
    binary = ROOT / 'bin/tg-import'
    with binary.open('rb') as src:
        header = src.read(20)
    if header[:5] != b'\x7fELF\x02' or header[5] != 1 or struct.unpack('<H', header[18:20])[0] != 62:
        raise RuntimeError('expected a Linux amd64 ELF executable')
    if 'INTERP' in output('readelf', '-l', str(binary)):
        raise RuntimeError('release executable depends on a dynamic loader')
    identity = json.loads(output(str(binary), 'version'))
    dist = ROOT / 'dist'
    dist.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='tg-import-package-') as tmp:
        stage = pathlib.Path(tmp)
        for name in ['README.md', 'LICENSE', 'config.example.json']:
            shutil.copyfile(ROOT / name, stage / name)
        shutil.copyfile(binary, stage / 'tg-import')
        (stage / 'tg-import').chmod(0o755)
        (stage / 'BUILD.json').write_text(json.dumps(identity, indent=2) + '\n')
        licenses = stage / 'licenses'
        licenses.mkdir()
        goroot = pathlib.Path(output(GO, 'env', 'GOROOT').strip())
        shutil.copyfile(goroot / 'LICENSE', licenses / 'Go-LICENSE')
        shutil.copyfile('/usr/share/doc/musl/copyright', licenses / 'musl-copyright')
        for notice in pathlib.Path('/usr/share/doc').glob('gcc-*-base/copyright'):
            shutil.copyfile(notice, licenses / (notice.parent.name + '-copyright'))
        notices = ['# Third-party source and notices', '',
                   'Dependencies retain their own licenses. The exact module source archives',
                   'linked below include the corresponding source for this build.', '',
                   '| Module | Version | Source |', '| --- | --- | --- |']
        for mod in modules():
            directory = pathlib.Path(mod['Dir'])
            module = mod['Path']
            target = licenses / module.replace('/', '_')
            target.mkdir()
            for item in directory.iterdir():
                if item.is_file() and item.name.upper().startswith(('LICENSE', 'NOTICE', 'COPYING')):
                    shutil.copyfile(item, target / item.name)
            escaped = ''.join('!' + c.lower() if c.isupper() else c for c in module)
            source = 'https://proxy.golang.org/' + escaped + '/@v/' + urllib.parse.quote(mod['Version'], safe='') + '.zip'
            notices.append(f'| {module} | {mod["Version"]} | [Source]({source}) |')
        (stage / 'THIRD_PARTY.md').write_text('\n'.join(notices) + '\n')
        artifact = dist / 'tg-import-linux-amd64.tar.gz'
        with tarfile.open(artifact, 'w:gz') as archive:
            for item in sorted(stage.iterdir()):
                archive.add(item, arcname=item.name)
    checksum = hashlib.sha256()
    with artifact.open('rb') as src:
        while chunk := src.read(1024 * 1024):
            checksum.update(chunk)
    digest = checksum.hexdigest()
    (dist / 'SHA256SUMS').write_text(f'{digest}  {artifact.name}\n')
    print(json.dumps(identity | {'artifact': artifact.name, 'sha256': digest}))


if __name__ == '__main__':
    main()
