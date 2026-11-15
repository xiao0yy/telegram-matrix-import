#!/usr/bin/env python3
"""Isolated Tuwunel + versioned in-memory S3 fixture. No production credentials.

Run as a foreground process; interrupt to remove only its named test container.
The S3 fixture deliberately implements only the operations exercised by tests.
"""
import hashlib
import http.server
import json
import pathlib
import signal
import shutil
import os
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
import urllib.request
import uuid
import xml.etree.ElementTree as ET

IMAGE = 'ghcr.io/matrix-construct/tuwunel@sha256:3dde1e9b3503c68218f95415ec3ebff522bd945065ba76019d408543540d1730'
OBJECTS, UPLOADS = {}, {}
LOCK = threading.Lock()

class S3(http.server.BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    def log_message(self, *_): pass
    def respond(self, code=200, body=b'', headers=None):
        self.send_response(code)
        for k, v in (headers or {}).items(): self.send_header(k, str(v))
        if not headers or 'Content-Length' not in headers: self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        if self.command != 'HEAD': self.wfile.write(body)
    def handle_s3(self):
        u = urllib.parse.urlsplit(self.path)
        key = urllib.parse.unquote(u.path).removeprefix('/fixture/')
        q = urllib.parse.parse_qs(u.query, keep_blank_values=True)
        length = int(self.headers.get('Content-Length', '0'))
        if length > 16 * 1024 * 1024: return self.respond(413)
        body = self.rfile.read(length)
        with LOCK:
            def save(data, metadata):
                v = str(uuid.uuid4()); etag = '"' + hashlib.md5(data).hexdigest() + '"'
                OBJECTS.setdefault(key, []).append((v, data, etag, metadata))
                return v, etag
            if self.command == 'POST' and 'uploads' in q:
                uid = str(uuid.uuid4()); UPLOADS[uid] = (key, {}, self.headers.get('X-Amz-Meta-Import-Id', ''))
                return self.respond(body=f'<InitiateMultipartUploadResult><UploadId>{uid}</UploadId></InitiateMultipartUploadResult>'.encode())
            if 'uploadId' in q:
                uid = q['uploadId'][0]
                if uid not in UPLOADS: return self.respond(404, b'<Error><Code>NoSuchUpload</Code></Error>')
                _, parts, meta = UPLOADS[uid]
                if self.command == 'PUT':
                    parts[int(q['partNumber'][0])] = body
                    return self.respond(headers={'ETag': '"'+hashlib.md5(body).hexdigest()+'"'})
                if self.command == 'GET':
                    rows=''.join(f'<Part><PartNumber>{n}</PartNumber><ETag>&quot;{hashlib.md5(data).hexdigest()}&quot;</ETag><Size>{len(data)}</Size></Part>' for n,data in sorted(parts.items()))
                    return self.respond(body=f'<ListPartsResult><IsTruncated>false</IsTruncated>{rows}</ListPartsResult>'.encode())
                if self.command == 'DELETE': del UPLOADS[uid]; return self.respond(204)
                if self.command == 'POST':
                    request = ET.fromstring(body); data = b''.join(parts[int(p.findtext('PartNumber'))] for p in request.findall('Part'))
                    v,etag=save(data, meta); del UPLOADS[uid]
                    return self.respond(body=f'<CompleteMultipartUploadResult><ETag>{etag}</ETag></CompleteMultipartUploadResult>'.encode(),headers={'X-Amz-Version-Id':v})
            if self.command == 'GET' and 'uploads' in q:
                prefix=q.get('prefix',[''])[0]; rows=''.join(f'<Upload><Key>{k}</Key><UploadId>{u}</UploadId></Upload>' for u,(k,_,_) in UPLOADS.items() if k.startswith(prefix))
                return self.respond(body=f'<ListMultipartUploadsResult><IsTruncated>false</IsTruncated>{rows}</ListMultipartUploadsResult>'.encode())
            if self.command == 'GET' and 'versions' in q:
                prefix=q.get('prefix',[''])[0];rows=''.join(f'<Version><Key>{k}</Key><VersionId>{v}</VersionId></Version>' for k,versions in OBJECTS.items() if k.startswith(prefix) for v,_,_,_ in versions)
                return self.respond(body=f'<ListVersionsResult><IsTruncated>false</IsTruncated>{rows}</ListVersionsResult>'.encode())
            if self.command == 'GET' and ('list-type' in q or key in ('fixture', 'fixture/')):
                return self.respond(body=b'<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>')
            if self.command == 'PUT':
                v,etag=save(body,self.headers.get('X-Amz-Meta-Import-Id',''));return self.respond(headers={'ETag':etag,'X-Amz-Version-Id':v})
            if self.command == 'DELETE':
                if 'versionId' in q: OBJECTS[key]=[v for v in OBJECTS.get(key,[]) if v[0]!=q['versionId'][0]]
                else: OBJECTS.pop(key,None)
                return self.respond(204)
            versions=OBJECTS.get(key,[])
            if not versions:return self.respond(404,b'<Error><Code>NoSuchKey</Code></Error>')
            v,data,etag,meta=versions[-1]
            return self.respond(body=data,headers={'Content-Length':len(data),'Content-Type':'application/octet-stream','ETag':etag,'X-Amz-Version-Id':v,'X-Amz-Meta-Import-Id':meta,'Last-Modified':'Sun, 15 Nov 2026 00:00:00 GMT'})
    do_HEAD = do_GET = do_PUT = do_POST = do_DELETE = handle_s3

def main():
    work=pathlib.Path(tempfile.mkdtemp(prefix='tg-import-tuwunel-',dir='/tmp'))
    server=http.server.ThreadingHTTPServer(('127.0.0.1',19791),S3)
    threading.Thread(target=server.serve_forever,daemon=True).start()
    (work/'data').mkdir()
    (work/'tuwunel.toml').write_text('''[global]
server_name = "import.test"
address = "127.0.0.1"
port = 19792
database_path = "/fixture/data"
allow_federation = false
db_cache_capacity_mb = 8
db_write_buffer_capacity_mb = 2
db_pool_max_workers = 2
[global.media]
storage_providers = ["media"]
[global.storage_provider.media.s3]
bucket = "fixture"
key = "fixture-access"
secret = "fixture-secret"
region = "us-east-1"
endpoint = "http://127.0.0.1:19791"
base_path = "media"
use_https = false
startup_check = false
[global.appservice.importer]
as_token = "fixture-as-token"
hs_token = "fixture-hs-token"
sender_localpart = "telegrambot"
[[global.appservice.importer.users]]
regex = "@.*:import\\.test"
exclusive = false
'''.replace('"@.*:import\\.test"', "'@.*:import\\.test'"))
    name='tg-import-isolated-'+uuid.uuid4().hex[:10]
    try:
        # Match the runner's UID so it can remove the fixture database after
        # Docker exits, including on GitHub's non-root runners.
        subprocess.run(['docker','run','-d','--name',name,'--network','host','--user',f'{os.getuid()}:{os.getgid()}','--memory','160m','--memory-swap','256m','--pids-limit','96','--mount',f'type=bind,src={work},dst=/fixture','-e','TUWUNEL_CONFIG=/fixture/tuwunel.toml',IMAGE,'--execute','users create owner fixture-password','--execute','users make-user-admin owner'],check=True,stdout=subprocess.DEVNULL)
        for _ in range(240):
            try:
                urllib.request.urlopen('http://127.0.0.1:19792/_matrix/client/versions',timeout=1).read();break
            except Exception:time.sleep(0.5)
        else:
            subprocess.run(['docker','logs','--tail','35',name]);raise RuntimeError('isolated homeserver did not start')
        print(json.dumps({'container':name,'path':str(work),'matrix':'http://127.0.0.1:19792','s3':'http://127.0.0.1:19791'}),flush=True)
        if len(sys.argv)>1:
            env=dict(os.environ,TG_IMPORT_ISOLATED_TEST='1')
            raise SystemExit(subprocess.call(sys.argv[1:],env=env))
        while True:time.sleep(1)
    finally:
        subprocess.run(['docker','rm','-f',name],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        server.shutdown()
        shutil.rmtree(work)

if __name__=='__main__':
    def stop(*_): raise KeyboardInterrupt
    signal.signal(signal.SIGTERM,stop)
    try:main()
    except KeyboardInterrupt:pass
