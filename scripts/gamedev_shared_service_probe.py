"""Owned synthetic shared-client + no-hook exclusive lane gate; run with --borg PATH."""
import argparse, json, os, pathlib, socket, subprocess, sys, tempfile, time, uuid

ap = argparse.ArgumentParser()
ap.add_argument('--borg', type=pathlib.Path, required=True)
ap.add_argument('--restore-failure-recovery', action='store_true')
args = ap.parse_args()
binary = args.borg.resolve(strict=True)
root = pathlib.Path(tempfile.mkdtemp(prefix='gd-shared-client-'))
state_dir = root / 'lanes'
identity = uuid.uuid4().hex
service = 'shared-' + identity[:8]
resource = 'shared-resource-' + identity
owners = ['client-a-' + identity, 'client-b-' + identity, 'client-c-' + identity]
owner_ids = [str(uuid.uuid5(uuid.NAMESPACE_OID, name)) for name in owners]

def cli(*parts, check=True, timeout=40):
    command = [str(binary), 'lane', '--state-dir', str(state_dir), *parts, '--json']
    proc = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
    if check and proc.returncode:
        raise RuntimeError(f'{command}: {proc.returncode}: {proc.stdout} {proc.stderr}')
    lines = [line for line in proc.stdout.splitlines() if line.startswith('{')]
    return (json.loads(lines[-1]) if lines else None), proc

def service_cli(*parts, **kw):
    return cli('service', *parts, **kw)

def free_port():
    with socket.socket() as connection:
        connection.bind(('127.0.0.1', 0))
        return connection.getsockname()[1]

def front_status():
    with socket.create_connection(('127.0.0.1', ports[0]), timeout=3) as connection:
        connection.sendall(b'GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n')
        return int(connection.recv(64).split()[1])

ports = []
while len(ports) < 3:
    value = free_port()
    if value not in ports: ports.append(value)
server = root / 'server.py'
server.write_text('''import http.server,sys
class H(http.server.BaseHTTPRequestHandler):
 def log_message(self,*args):pass
 def do_GET(self):
  self.send_response(200);self.end_headers();self.wfile.write(b"ok")
http.server.ThreadingHTTPServer(("127.0.0.1",int(sys.argv[1])),H).serve_forever()
''')
worker = root / 'worker.py'
worker.write_text('''import json,os,pathlib,socket,subprocess,sys
root,binary,service,front,pid,owner_a,owner_b=sys.argv[1:]
cmd=[binary,'lane','--state-dir',str(pathlib.Path(root)/'lanes'),'service','status',service,'--json']
status=json.loads(subprocess.check_output(cmd,text=True))
assert status['clients']==[] and status['backend_pid'] is None and status['state'] in ('Yielded', 'RestartPending'),status
lines=(pathlib.Path(root)/'restored').read_text().splitlines()
assert lines.count(owner_a)==2 and lines.count(owner_b)==1,lines
try: os.kill(int(pid),0)
except ProcessLookupError: pass
else: raise AssertionError('service backend alive after exclusive grant')
with socket.create_connection(('127.0.0.1',int(front)),timeout=3) as connection:
 connection.sendall(b'GET /health HTTP/1.1\\r\\nHost: localhost\\r\\nConnection: close\\r\\n\\r\\n')
 body=connection.recv(512)
assert b'503 Service Unavailable' in body,body
(pathlib.Path(root)/'exclusive-verified').write_text('both clients restored; backend stopped; proxy 503')
''')
spec = dict(id=service, argv=[sys.executable,str(server),'{port}'], cwd=str(root), env=[],
    resources=[dict(key=dict(scope='Host',name=resource),access='Exclusive')],
    memory_max_bytes=128*1024*1024,
    admission=dict(min_available_ram_bytes=0,reserve_ram_bytes=0,min_free_disk_bytes=0,
                   reserve_disk_bytes=0,disk_path=str(root)),
    health=dict(kind='http',argv=['/health'],interval_ms=200,timeout_ms=500),
    restart=dict(max_restarts=2,backoff_ms=200,debounce_ms=100),
    endpoint=dict(listen=f'127.0.0.1:{ports[0]}',backend_ports=ports[1:]),
    restore=dict(argv=[sys.executable,'-c',
        'import pathlib,sys;owner=sys.argv[2];marker=pathlib.Path(sys.argv[3]);'
        'sys.exit(4) if marker.exists() and owner==sys.argv[4] else None;'
        'open(sys.argv[1],"a").write(owner+chr(10))',
        str(root/'restored'),'{owner}',str(root/'restore-blocked'),owner_ids[1]],
        timeout_ms=1500),
    client_mode={'Shared':{'max_clients':2}},read_only_paths=['/health'],
    adapter_enforces_leases=False,readiness_timeout_ms=6000)
path = root / 'definition.json'
path.write_text(json.dumps(spec))
launched=False
try:
    launched=True
    started,_=service_cli('start',service,'--definition',str(path),'--wait-ready','8')
    assert 'Healthy' in started['state'],started
    pid=started['backend_pid']
    leases=[]
    for index in (0,1):
        status,_=service_cli('lease',service,'--owner',owners[index],
                             '--purpose',f'db-{index}','--ttl-seconds','60')
        matches=[c for c in status['clients'] if c['owner']['participant_id']==owner_ids[index]]
        assert len(matches)==1,status
        leases.append(matches[0]['id'])
    assert leases[0]!=leases[1]
    denied,reason=service_cli('lease',service,'--owner',owners[2],
                              '--purpose','overflow','--ttl-seconds','60',check=False)
    assert reason.returncode and 'client limit reached' in reason.stderr,(denied,reason.stderr)
    assert len(service_cli('status',service)[0]['clients'])==2
    after_release,_=service_cli('release',service,'--owner',owners[0],'--lease-id',leases[0])
    assert len(after_release['clients'])==1 and after_release['clients'][0]['id']==leases[1]
    assert (root/'restored').read_text().splitlines()==[owner_ids[0]]
    reacquired,_=service_cli('lease',service,'--owner',owners[0],
                             '--purpose','db-a-again','--ttl-seconds','60')
    assert len(reacquired['clients'])==2
    job=dict(fingerprint='shared-client-exclusive-'+identity,
      lease=dict(resources=[dict(key=dict(scope='Host',name=resource),access='Exclusive')],
        holder=dict(participant_id=str(uuid.uuid4()),session_id=str(uuid.uuid4()),
                    host_pid=None,purpose='shared-client handoff smoke'),queue_timeout_ms=20000),
      argv=[sys.executable,str(worker),str(root),str(binary),service,str(ports[0]),str(pid),
            owner_ids[0],owner_ids[1]],cwd=str(root),env=[],memory_max_bytes=128*1024*1024,
      admission=dict(min_available_ram_bytes=0,reserve_ram_bytes=0,min_free_disk_bytes=0,
                     reserve_disk_bytes=0,disk_path=str(root)),
      pre_hook=None,post_hook=None,timeout_ms=12000,stall_timeout_ms=None,coalesce=False)
    job_path=root/'exclusive.json'
    job_path.write_text(json.dumps(job))
    # v0.1: both clients are foreign to this holder, so the exclusive must
    # hold in Preparing (clients and backend untouched) until the grace ends.
    submitted,_=cli('job','submit','--spec',str(job_path),'--foreign-lease-grace-seconds','5')
    job_id=submitted['job_id']
    deadline=time.monotonic()+4
    while True:
        row,_=cli('job','status',job_id)
        if row.get('state')=='Preparing' and (row.get('wait_reason') or '').startswith('foreign client lease:'):
            break
        if row.get('started_ms') is not None or (root/'exclusive-verified').exists():
            raise RuntimeError(f'foreign shared clients did not hold the exclusive: {row!r}')
        if time.monotonic()>=deadline:
            raise RuntimeError(f'exclusive never waited on foreign shared clients: {row!r}')
        subprocess.run(['inotifywait','-q','-t','1','-e','modify,close_write,moved_to',
                        str(state_dir)],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=2)
    foreign_wait_reason=row['wait_reason']
    waiting=service_cli('status',service)[0]
    assert len(waiting['clients'])==2 and waiting['backend_pid']==pid and 'Healthy' in waiting['state'],waiting
    assert foreign_wait_reason in (waiting.get('reason') or ''),waiting
    assert front_status()==200,'front endpoint not serving while foreign clients hold the exclusive'
    done,_=cli('job','wait',job_id,timeout=45)
    assert done['state']['Finished']['exit_code']==0,done
    assert (root/'exclusive-verified').exists()
    deadline=time.monotonic()+8
    while time.monotonic()<deadline:
        resumed=service_cli('status',service)[0]
        if 'Healthy' in resumed['state']:
            break
        subprocess.run(['inotifywait','-q','-t','1','-e','modify,close_write,moved_to',
                        str(state_dir/'services'/service)],stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL,timeout=2)
    else:
        raise RuntimeError('service did not resume Healthy '+str(resumed))
    assert resumed['backend_pid']!=pid and resumed['clients']==[],resumed
    recovery = None
    if args.restore_failure_recovery:
        for index in (0, 1):
            service_cli('lease',service,'--owner',owners[index],
                        '--purpose',f'recovery-{index}','--ttl-seconds','60')
        marker=root/'restore-blocked'
        marker.write_text('reject second owner')
        _, failed_stop = service_cli('stop',service,check=False)
        assert failed_stop.returncode and 'restore client' in failed_stop.stderr,failed_stop.stderr
        blocked=service_cli('status',service)[0]
        assert len(blocked['clients'])==1 and blocked['clients'][0]['owner']['participant_id']==owner_ids[1],blocked
        assert blocked['backend_pid']==resumed['backend_pid'] and 'Healthy' in blocked['state'],blocked
        stuck_id=blocked['clients'][0]['id']
        for value in (owner_ids[1],stuck_id):
            assert value in blocked['reason'] and value in failed_stop.stderr,(blocked,failed_stop.stderr)
        _, failed_yield=service_cli('yield',service,'--by','blocked-exclusive',
                                    '--for-seconds','10',check=False)
        assert failed_yield.returncode,failed_yield.stderr
        still_blocked=service_cli('status',service)[0]
        assert still_blocked['clients']==blocked['clients'] and not still_blocked['yields'],still_blocked
        assert still_blocked['backend_pid']==resumed['backend_pid'],still_blocked
        assert all(value in still_blocked['reason'] and value in failed_yield.stderr
                   for value in (owner_ids[1],stuck_id)),still_blocked
        blocked_job=json.loads(json.dumps(job))
        blocked_job['fingerprint']='failed-restore-'+identity
        blocked_job['lease']['queue_timeout_ms']=5000
        forbidden=root/'must-not-start'
        blocked_job['argv']=[sys.executable,'-c',
            'import pathlib,sys;pathlib.Path(sys.argv[1]).write_text("unsafe grant")',str(forbidden)]
        blocked_spec=root/'failed-job.json'
        blocked_spec.write_text(json.dumps(blocked_job))
        # Short grace so the yield (and its failed restore) decides the job,
        # not the queue timeout on a still-leased foreign client.
        denied_job,_=cli('job','submit','--spec',str(blocked_spec),'--foreign-lease-grace-seconds','1')
        denied_id=denied_job['job_id']
        failed_job,_=cli('job','wait',denied_id,check=False,timeout=40)
        assert failed_job['state']['Finished']['exit_code']==125,failed_job
        assert not forbidden.exists(),'exclusive command ran after a failed client restore'
        record_proc=subprocess.run([str(binary),'lane','--state-dir',str(state_dir),
            'resource','status','--json'],capture_output=True,text=True,timeout=15)
        assert record_proc.returncode==0,record_proc.stderr
        records=json.loads(record_proc.stdout)
        job_row=next(row for row in records if row['ticket']['id']==denied_id)
        assert all(value in job_row['evidence'] for value in (owner_ids[1],stuck_id)),job_row
        still_blocked=service_cli('status',service)[0]
        assert len(still_blocked['clients'])==1 and still_blocked['backend_pid']==resumed['backend_pid'],still_blocked
        marker.unlink()
        released,_=service_cli('release',service,'--owner',owners[1],
                               '--lease-id',blocked['clients'][0]['id'])
        assert released['clients']==[] and 'Healthy' in released['state'],released
        recovery=dict(failed_stop_fenced=True,failed_yield_fenced=True,
                      failed_job_id=denied_id,failed_job_evidence=job_row['evidence'],
                      stuck_owner=owner_ids[1],stuck_lease_id=stuck_id,
                      human_release_cleared_client=True)
    print(json.dumps(dict(mode='real-cli-shared-client-exclusive-handoff',
        service=service,job_id=job_id,client_mode=spec['client_mode'],
        lease_ids=leases,third_denial=reason.stderr.strip(),
        owner_restore_lines=(root/'restored').read_text().splitlines(),
        clients_before_exclusive=2,clients_while_preparing=2,foreign_wait_reason=foreign_wait_reason,
        clients_at_grant=0,verified_before_grant=True,
        backend_before=pid,backend_after=resumed['backend_pid'],
        resumed_healthy=True,manual_recovery=recovery,fixture=str(root)),indent=2))
finally:
    marker=root/'restore-blocked'
    if marker.exists(): marker.unlink()
    if launched:
        status,proc=service_cli('stop',service,check=False)
        print('cleanup',proc.returncode,(status or {}).get('state'),proc.stderr[:150])
