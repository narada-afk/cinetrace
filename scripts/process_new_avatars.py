import os, ssl, time, shutil
import urllib.request
import urllib.parse

API_KEY    = 'fzotK6cmX6mEkRyr5ma8aiFy'
SOURCE_DIR = '/Users/macmini/Documents/Primary_Images'
OUT_DIR    = '/Users/macmini/south-cinema-analytics/frontend/public/avatars'
WORKTREE   = '/Users/macmini/south-cinema-analytics/.claude/worktrees/relaxed-euler/frontend/public/avatars'

# source filename slug → target avatar slug
NEW_ACTORS = {
    'arjunsarja':           'arjunsarja',
    'chiranjeevi':          'chiranjeevi',
    'nagarjunaakkineni':    'nagarjunaakkineni',
    'nandamuribalakrishna': 'nandamuribalakrishna',
    'ranadaggubati':        'ranadaggubati',
    'raviteja':             'raviteja',
    'sharwanand':           'sharwanand',
    'siddhujonnalagadda':   'siddhujonnalagadda',
    'varunsandesh':         'varunsandesh',
    'varuntej':             'varuntej',
    'venkateshdaggubati':   'venkateshdaggubati',
    'vishwaksen':           'vishwaksen',
}

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

def find_source(slug):
    for f in os.listdir(SOURCE_DIR):
        if f.lower().replace('.png','') == slug.lower():
            return os.path.join(SOURCE_DIR, f)
    return None

def process(src_slug, tgt_slug):
    src = find_source(src_slug)
    if not src:
        print(f'✗ {tgt_slug}: source file not found')
        return False

    with open(src, 'rb') as f:
        img_data = f.read()

    boundary = 'SCABoundary12345'
    body = (
        f'--{boundary}\r\n'
        f'Content-Disposition: form-data; name="size"\r\n\r\nauto\r\n'
        f'--{boundary}\r\n'
        f'Content-Disposition: form-data; name="image_file"; filename="image.png"\r\n'
        f'Content-Type: image/png\r\n\r\n'
    ).encode() + img_data + f'\r\n--{boundary}--\r\n'.encode()

    req = urllib.request.Request(
        'https://api.remove.bg/v1.0/removebg',
        data=body,
        headers={
            'X-Api-Key': API_KEY,
            'Content-Type': f'multipart/form-data; boundary={boundary}',
        },
        method='POST'
    )

    print(f'⏳ {tgt_slug}: processing...')
    try:
        with urllib.request.urlopen(req, timeout=30, context=ctx) as resp:
            result = resp.read()
            out = os.path.join(OUT_DIR, f'{tgt_slug}.png')
            wt  = os.path.join(WORKTREE, f'{tgt_slug}.png')
            with open(out, 'wb') as f: f.write(result)
            shutil.copy(out, wt)
            print(f'✓ {tgt_slug}: saved ({len(result)//1024}KB)')
            return True
    except urllib.error.HTTPError as e:
        print(f'✗ {tgt_slug}: HTTP {e.code} — {e.read().decode()[:120]}')
        return False
    except Exception as e:
        print(f'✗ {tgt_slug}: {e}')
        return False

ok = fail = 0
print(f'Processing {len(NEW_ACTORS)} new actors...\n')
for src, tgt in NEW_ACTORS.items():
    if process(src, tgt):
        ok += 1
    else:
        fail += 1
    time.sleep(0.6)

print(f'\nDone — {ok} succeeded, {fail} failed')
