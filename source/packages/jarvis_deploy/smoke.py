"""Zero-model real SDK acceptance in a new synthetic HOME/state."""
import argparse, asyncio, json, os, pathlib, shutil, sqlite3, sys, tempfile
from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client

async def check(release):
    manifest=json.loads((release/'deployment-manifest.json').read_text())
    base=pathlib.Path(tempfile.mkdtemp(prefix='smoke-',dir=release))
    for d in ('config','state','home','project'): (base/d).mkdir()
    cfg=json.loads((release/'deploy/smoke-config.json').read_text())
    launcher=cfg['launcher.json']
    launcher.update(linux_cloud=sys.platform.startswith('linux'),expected_codex_home=str(base/'home'),codex_cli=shutil.which('codex'),allowed_projects={'ReadonlyProtocolTest':str(base/'project')},runtime_environments=[],runtime_constraints={'model_provider':'openai','sandbox':'read-only','approval_policy':'never'})
    if not launcher['codex_cli']: raise RuntimeError('Codex CLI required for real smoke')
    cfg['transport.json']['standard_bridge_package_root']=str(release/'source/packages')
    for name,data in cfg.items(): (base/'config'/name).write_text(json.dumps(data))
    from jarvis_runtime.jarvis_native_task_launcher import NativeTaskLauncherConfig
    from jarvis_runtime.linux_cloud import child_environment,SAFE_ENV
    config=NativeTaskLauncherConfig(base/'config/launcher.json')
    if sys.platform.startswith('linux'): child_environment(config)
    env={k:v for k,v in os.environ.items() if k in SAFE_ENV or k in ('SYSTEMROOT','SystemRoot','TEMP','TMP','COMSPEC')}
    env.update(HOME=str(base/'home'),USERPROFILE=str(base/'home'),APPDATA=str(base/'home'),LOCALAPPDATA=str(base/'home'),XDG_CONFIG_HOME=str(base/'home'),XDG_CACHE_HOME=str(base/'home'),CODEX_HOME=str(base/'home'),PYTHONDONTWRITEBYTECODE='1')
    args=['-I','-B','-m','jarvis_mcp.entrypoint','--config',str(base/'config/transport.json'),'--launcher-config',str(base/'config/launcher.json'),'--state-dir',str(base/'state'),'--local-heartbeat-config',str(base/'config/local-heartbeat.json')]
    async with asyncio.timeout(45):
        async with stdio_client(StdioServerParameters(command=sys.executable,args=args,env=env)) as (r,w):
            async with ClientSession(r,w,read_timeout_seconds=12) as session:
                init=await session.initialize()
                assert init.server_info.version==manifest['version']
                tools=await session.list_tools(); names=sorted(t.name for t in tools.tools)
                assert names==sorted(manifest['expected_tools'])
                for tool,params in [('jarvis_read',{'subject':'capabilities'}),('jarvis_loop',{'action':'preflight','request_id':'deployment-zero-model'})]:
                    result=await session.call_tool(tool,params)
                    assert not result.is_error
                    payload=json.loads(next(c.text for c in result.content if c.type=='text'))
                    assert payload['status']=='completed'
    counts={}
    for db in (base/'state').glob('*.sqlite'):
        with sqlite3.connect(db.as_uri()+'?mode=ro',uri=True) as conn:
            for (table,) in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"):
                n=conn.execute('SELECT COUNT(*) FROM "'+table.replace('"','""')+'"').fetchone()[0]
                counts[db.name+':'+table]=n
                assert n==0
    result={'status':'passed','version':manifest['version'],'tools':names,'model_turns':0,'state_rows':counts,'live_creation_enabled':False,'process_closed':True}
    (release/'smoke-result.json').write_text(json.dumps(result,indent=2))
    print(json.dumps(result))

def main():
    p=argparse.ArgumentParser();p.add_argument('--release',type=pathlib.Path,required=True);a=p.parse_args();asyncio.run(check(a.release.resolve()))
if __name__=='__main__': main()
