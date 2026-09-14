"""Private subprocess entrypoint for operator-installed adapters."""
import importlib
import json
import sys

def main():
    if sys.platform!='win32':
        import resource
        resource.setrlimit(resource.RLIMIT_FSIZE,(131072,131072))
    module,name=sys.argv[1].split(':',1)
    handler=getattr(importlib.import_module(module),name)
    raw=sys.stdin.buffer.read(65537)
    if len(raw)>65536:raise ValueError('Adapter request exceeds 64 KiB')
    payload=json.loads(raw)
    result=handler(payload['config'],payload['request'])
    data=json.dumps(result,allow_nan=False,ensure_ascii=False)
    if len(data.encode())>65536:raise ValueError('Adapter response exceeds 64 KiB')
    sys.stdout.write(data)

if __name__=='__main__':main()
