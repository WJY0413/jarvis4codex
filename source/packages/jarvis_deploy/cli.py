"""Explicit, dry-run-first installation and stopped-instance update."""
from __future__ import annotations
import argparse, hashlib, json, os, pathlib, shutil, subprocess, sys, uuid, venv
from .artifact import inspect_archive, extract_verified, download_verified
from .lifecycle import LifecycleLease

SCHEMA='jarvis-managed-install/v1'

def read(path):
    if path.is_symlink() or path.stat().st_nlink != 1: raise ValueError('Unsafe metadata path')
    return json.loads(path.read_text(encoding='utf-8'))

def atomic(path, value):
    if path.is_symlink(): raise ValueError('Unsafe metadata destination')
    tmp=path.with_name(path.name+'.'+uuid.uuid4().hex+'.tmp')
    with tmp.open('x',encoding='utf-8') as out:
        json.dump(value,out,indent=2); out.write('\n'); out.flush(); os.fsync(out.fileno())
    os.replace(tmp,path)

def safe_root(value):
    p=pathlib.Path(value).expanduser().absolute()
    if '..' in p.parts or p.resolve()!=p or p.is_symlink(): raise ValueError('Root must not traverse links')
    if {'.codex','.ssh','.aws','.gnupg'}.intersection(x.lower() for x in p.parts): raise ValueError('Credential directory refused')
    if os.environ.get('CODEX_HOME') and p.is_relative_to(pathlib.Path(os.environ['CODEX_HOME']).resolve()): raise ValueError('CODEX_HOME refused')
    for component in (p,*p.parents):
        if component.exists() and getattr(component.lstat(),'st_file_attributes',0)&0x400: raise ValueError('Windows reparse point refused')
    if p.parent==p or str(p) in ('/etc','/usr','/var','/tmp','/workspace'): raise ValueError('Dedicated root required')
    for rel in ('releases','backups','runtime','runtime/state','runtime/config','managed-install.json','current.json','lifecycle.json','.lifecycle.lock'):
        item=p/rel
        if item.is_symlink() or item.resolve()!=item: raise ValueError('Managed path traverses symlink: '+rel)
    return p

def py(release):
    return release/'venv'/('Scripts/python.exe' if os.name=='nt' else 'bin/python')

def install_dependencies(release):
    venv.EnvBuilder(with_pip=False).create(release/'venv')
    # Build/package tools get no account HOME, keyring or auth settings.
    build_home=release/'build-home'; build_home.mkdir(exist_ok=True)
    temp=release/'build-temp'; temp.mkdir(exist_ok=True)
    safe=('PATH','SystemRoot','SYSTEMROOT','COMSPEC','PATHEXT','HTTPS_PROXY','HTTP_PROXY','ALL_PROXY','NO_PROXY','SSL_CERT_FILE','SSL_CERT_DIR','REQUESTS_CA_BUNDLE')
    env={k:v for k,v in os.environ.items() if k in safe}
    env.update(HOME=str(build_home),USERPROFILE=str(build_home),APPDATA=str(build_home/'appdata'),LOCALAPPDATA=str(build_home/'local'),XDG_CONFIG_HOME=str(build_home/'config'),XDG_CACHE_HOME=str(build_home/'cache'),TMPDIR=str(temp),TEMP=str(temp),TMP=str(temp),PIP_CONFIG_FILE=os.devnull,PYTHONDONTWRITEBYTECODE='1',PIP_KEYRING_PROVIDER='disabled')
    subprocess.run([str(py(release)),'-I','-m','ensurepip','--default-pip'],check=True,env=env,timeout=120)
    subprocess.run([str(py(release)),'-I','-m','pip','--isolated','--keyring-provider','disabled','--no-input','install','--no-user','--no-cache-dir','--disable-pip-version-check','-r',str(release/'deploy/requirements.lock.txt')],check=True,env=env,timeout=600)
    subprocess.run([str(py(release)),'-I','-m','pip','--isolated','--keyring-provider','disabled','--no-input','install','--no-deps','--no-build-isolation',str(release/'source')],check=True,env=env,timeout=120)
    subprocess.run([str(py(release)),'-I','-B','-m','jarvis_deploy.smoke','--release',str(release)],check=True,env=env,timeout=90)

def file_sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()

def inventory(release):
    result={}
    paths=[p for folder in ('venv','source/packages') for p in (release/folder).rglob('*')]
    for p in sorted(paths):
        if getattr(p.lstat(),'st_file_attributes',0)&0x400: raise ValueError('Installed runtime reparse point refused')
        if p.is_symlink():
            interpreter=p.parent.name=='bin' and p.name.startswith('python') and p.resolve()==pathlib.Path(sys.executable).resolve()
            liblink=p==release/'venv/lib64' and p.resolve()==release/'venv/lib'
            if not (interpreter or liblink): raise ValueError('Unsafe installed runtime link')
            result[p.relative_to(release).as_posix()+'@link']=str(p.resolve())
        if p.is_file():
            if not p.is_symlink() and p.stat().st_nlink!=1: raise ValueError('Hardlinked runtime file refused')
            result[p.relative_to(release).as_posix()]=file_sha(p)
    return result

def verify_staged(release,pointer):
    if release.is_symlink() or release.resolve()!=release: raise ValueError('Unsafe release path')
    receipt=read(release/'deployment-receipt.json')
    for key in ('version','sha256'):
        if receipt.get(key)!=pointer.get(key): raise ValueError('Receipt mismatch')
    if receipt.get('status')!='passed': raise ValueError('Release acceptance missing')
    if file_sha(release/'deployment-manifest.json')!=pointer.get('manifest_sha256'): raise ValueError('Release manifest changed')
    if file_sha(release/'runtime-inventory.json')!=pointer.get('runtime_inventory_sha256'): raise ValueError('Runtime inventory changed')
    if inventory(release)!=read(release/'runtime-inventory.json'): raise ValueError('Installed executable runtime changed')
    manifest=read(release/'deployment-manifest.json')
    for name,digest in manifest['files'].items():
        parts=pathlib.PurePosixPath(name).parts
        if not parts or '..' in parts or pathlib.PurePosixPath(name).is_absolute() or '\\' in name or ':' in name: raise ValueError('Unsafe stored manifest path')
        path=release.joinpath(*pathlib.PurePosixPath(name).parts)
        if path.is_symlink() or path.resolve()!=path or not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest()!=digest: raise ValueError('Staged source changed: '+name)

def release_from_pointer(root,pointer):
    name=pointer.get('directory','')
    if not name or pathlib.PurePath(name).name!=name or '/' in name or '\\' in name: raise ValueError('Invalid release pointer')
    release=root/'releases'/name
    verify_staged(release,pointer)
    return release

def deploy(args):
    root=safe_root(args.root)
    manifest=inspect_archive(pathlib.Path(args.archive),args.sha256,args.version)
    expected_platform='windows' if os.name=='nt' else 'linux'
    if manifest.get('platform') not in (None,expected_platform): raise ValueError('Archive platform mismatch')
    marker=root/'managed-install.json'
    exists=root.exists()
    if exists and (not marker.is_file() or read(marker).get('schema')!=SCHEMA): raise ValueError('Existing unmanaged root: adoption is unsupported')
    if args.action=='install' and exists and (root/'current.json').exists(): raise ValueError('Use update for an existing managed installation')
    if args.action=='update' and not exists: raise ValueError('Update requires managed installation')
    plan={'action':args.action,'version':args.version,'root':str(root),'dry_run':not args.apply,'changes':'new release and next-start pointer only; runtime/HOME/auth/live unchanged','channel':manifest.get('channel')}
    if not args.apply: return plan
    if not exists:
        root.mkdir(parents=True,exist_ok=False)
        atomic(marker,{'schema':SCHEMA})
        (root/'runtime/state').mkdir(parents=True)
    with LifecycleLease(root) as lease:
        if not exists: lease.initialize_managed(root/'runtime/state')
        lease.assert_update_safe()
        name=args.version+'-'+args.sha256[:12]
        pointer={'version':args.version,'sha256':args.sha256,'directory':name}
        release=root/'releases'/name
        current=read(root/'current.json') if (root/'current.json').exists() else None
        if current and current.get('version')==pointer['version'] and current.get('sha256')==pointer['sha256']:
            release_from_pointer(root,current)
            return {**plan,'status':'unchanged','dry_run':False}
        if release.exists():
            raise ValueError('Non-current release already exists; use verified backup rollback or a fresh managed root')
        else:
            release.parent.mkdir(exist_ok=True)
            stage=release
            try:
                extract_verified(pathlib.Path(args.archive),stage,args.sha256,args.version)
                install_dependencies(stage)
                atomic(stage/'runtime-inventory.json',inventory(stage))
                pointer.update(manifest_sha256=file_sha(stage/'deployment-manifest.json'),runtime_inventory_sha256=file_sha(stage/'runtime-inventory.json'))
                atomic(stage/'deployment-receipt.json',{**pointer,'status':'passed','scope':'install and zero-model SDK smoke'})
                verify_staged(stage,pointer)
                # venv paths must remain fixed; only the next-start pointer is activated after acceptance.
            except BaseException:
                # Quarantine only this newly created failed release; keep diagnostics and allow a fresh retry.
                failed=root/'releases'/('.failed-'+uuid.uuid4().hex)
                if stage.exists(): os.replace(stage,failed)
                raise
        verify_staged(release,pointer)
        lease.assert_update_safe()
        backup=None
        if current:
            release_from_pointer(root,current)
            (root/'backups').mkdir(exist_ok=True)
            backup=uuid.uuid4().hex+'.json'
            atomic(root/'backups'/backup,current)
        if current is None and not (root/'runtime/config').exists():
            config=root/'runtime/config'; config.mkdir()
            (root/'runtime/home').mkdir(); (root/'runtime/project').mkdir()
            template=read(release/'deploy/smoke-config.json')
            template['launcher.json'].update(linux_cloud=os.name!='nt',expected_codex_home=str(root/'runtime/home'),codex_cli=shutil.which('codex'),allowed_projects={'Acceptance':str(root/'runtime/project')},runtime_environments=[],runtime_constraints={'model_provider':'openai','sandbox':'read-only','approval_policy':'never'})
            template['transport.json']['standard_bridge_package_root']=str(release/'source/packages')
            for filename,body in template.items(): atomic(config/filename,body)
        atomic(root/'current.json',pointer)
        return {**plan,'status':'installed' if not exists else 'updated_next_start','dry_run':False,'backup':backup,'restarted':False}

def rollback(args):
    root=safe_root(args.root)
    if not args.backup or pathlib.Path(args.backup).name!=args.backup: raise ValueError('Exact local backup filename required')
    desired=read(root/'backups'/args.backup)
    release_from_pointer(root,desired)
    if not args.apply: return {'action':'rollback','dry_run':True,'version':desired['version']}
    with LifecycleLease(root) as lease:
        lease.assert_update_safe()
        current=read(root/'current.json'); release_from_pointer(root,current)
        backup=uuid.uuid4().hex+'.json'; atomic(root/'backups'/backup,current)
        atomic(root/'current.json',desired)
        return {'status':'rolled_back_next_start','version':desired['version'],'backup':backup,'restarted':False}

def run(args):
    root=safe_root(args.root)
    if not args.apply: return {'action':'run','dry_run':True,'root':str(root)}
    with LifecycleLease(root) as lease:
        lease.assert_update_safe(); pointer=read(root/'current.json'); release=release_from_pointer(root,pointer)
        config=root/'runtime/config'; state=root/'runtime/state'
        for name in ('transport.json','launcher.json','local-heartbeat.json'):
            if not (config/name).is_file(): raise ValueError('Configure managed runtime explicitly before run: '+name)
        # Resolve the code route for this launch only, preserve user-owned runtime config.
        transport=read(config/'transport.json'); transport['standard_bridge_package_root']=str(release/'source/packages')
        transport['native_task_launcher_config']=str(config/'launcher.json')
        for key in ('db_path','health_path','lock_path','heartbeat_contracts_dir'):
            if key in transport and not pathlib.Path(transport[key]).is_absolute(): transport[key]=str((config/transport[key]).resolve())
        launch_transport=root/'runtime/launch-transport.json'; atomic(launch_transport,transport)
        lease.begin_launch()
        try:
            child=subprocess.Popen([str(py(release)),'-I','-B','-m','jarvis_mcp.entrypoint','--config',str(launch_transport),'--launcher-config',str(config/'launcher.json'),'--state-dir',str(state),'--local-heartbeat-config',str(config/'local-heartbeat.json')])
            lease.mark_running(child.pid)
            code=child.wait()
            result=lease.finish_launch(code)
        except BaseException:
            # Never claim stopped or restart/kill another service on uncertain exit.
            lease.finish_launch(1)
            raise
        return {'exit_code':code,'lifecycle':result}

def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['download','install','update','rollback','run'])
    parser.add_argument('--root'); parser.add_argument('--url')
    parser.add_argument('--archive'); parser.add_argument('--sha256'); parser.add_argument('--version'); parser.add_argument('--backup')
    parser.add_argument('--apply',action='store_true',help='Perform exactly the displayed scoped action; default is dry-run')
    args=parser.parse_args(argv)
    try:
        if args.action=='download':
            if not all([args.url,args.archive,args.sha256,args.version]): parser.error('url, archive destination, sha256 and exact version required')
            if args.apply:
                manifest=download_verified(args.url,pathlib.Path(args.archive),args.sha256,args.version)
                result={'status':'downloaded_verified','version':manifest['version'],'executed':False}
            else: result={'action':'download','dry_run':True,'url':args.url,'destination':args.archive,'version':args.version}
        elif not args.root: parser.error('root required')
        elif args.action in ('install','update'):
            if not all([args.archive,args.sha256,args.version]): parser.error('archive, sha256 and exact version required')
            result=deploy(args)
        elif args.action=='rollback': result=rollback(args)
        else: result=run(args)
        print(json.dumps(result,ensure_ascii=False))
        return 0
    except Exception as exc:
        print(json.dumps({'status':'blocked_or_failed','error_type':type(exc).__name__,'error':str(exc),'restarted':False}),file=sys.stderr)
        return 1

if __name__=='__main__': raise SystemExit(main())
