#!/usr/bin/env python3
"""Share identical immutable Git objects on APFS; never remove work or history.

Dry-run by default. Pass --apply for atomic copy-on-write replacement. Independent
inodes, permissions, timestamps and extended attributes are retained. Roots are
explicit: no blanket home-directory cleanup or deletion based on file age.
"""
import argparse
import ctypes
import errno
import filecmp
import fcntl
import json
import os
from pathlib import Path
import re
import shutil
import stat
import sys
import time
import uuid

MIN_SIZE = 64 * 1024
SKIP = {"node_modules", "target", ".venv", "Binaries", "Intermediate", "DerivedDataCache"}


def fingerprint(info):
    return [info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns]


def git_directories(roots):
    seen = set()
    for root in roots:
        if root.is_symlink() or not root.is_dir():
            continue
        if root.name == ".git":
            candidates = [root]
        else:
            candidates = []
            for directory, children, _ in os.walk(root, followlinks=False):
                if ".git" in children:
                    candidates.append(Path(directory) / ".git")
                children[:] = [name for name in children if name != ".git" and name not in SKIP]
        for directory in candidates:
            if directory.is_symlink() or directory in seen:
                continue
            seen.add(directory)
            yield directory


def objects(directory):
    root = directory / "objects"
    if root.is_symlink() or not root.is_dir():
        return
    for bucket in sorted(root.iterdir()):
        if bucket.is_symlink() or not bucket.is_dir():
            continue
        if bucket.name != "pack" and not re.fullmatch("[0-9a-f]{2}", bucket.name):
            continue
        for path in bucket.iterdir():
            valid = (re.fullmatch(r"pack-[0-9a-f]{40,64}\.pack", path.name)
                     if bucket.name == "pack" else re.fullmatch("[0-9a-f]{38}|[0-9a-f]{62}", path.name))
            if valid and not path.is_symlink() and path.is_file():
                info = path.stat()
                if info.st_size >= MIN_SIZE and info.st_uid == os.getuid():
                    yield (str(path.relative_to(root)), info.st_size), path, info


class Apfs:
    def __init__(self):
        self.lib = ctypes.CDLL("/usr/lib/libSystem.B.dylib", use_errno=True)
        self.lib.clonefile.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int]
        self.lib.clonefile.restype = ctypes.c_int
        self.lib.listxattr.argtypes = [ctypes.c_char_p, ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int]
        self.lib.listxattr.restype = ctypes.c_ssize_t
        self.lib.getxattr.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_void_p,
                                     ctypes.c_size_t, ctypes.c_uint32, ctypes.c_int]
        self.lib.getxattr.restype = ctypes.c_ssize_t
        self.lib.acl_get_file.argtypes = [ctypes.c_char_p, ctypes.c_int]
        self.lib.acl_get_file.restype = ctypes.c_void_p
        self.lib.acl_to_text.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_ssize_t)]
        self.lib.acl_to_text.restype = ctypes.c_void_p
        self.lib.acl_free.argtypes = [ctypes.c_void_p]

    def acl(self, path):
        acl = self.lib.acl_get_file(os.fsencode(path), 0x100)  # ACL_TYPE_EXTENDED
        if not acl:
            if ctypes.get_errno() == errno.ENOENT and path.exists():
                return b""
            raise OSError(ctypes.get_errno(), "acl_get_file", str(path))
        text = None
        try:
            length = ctypes.c_ssize_t()
            text = self.lib.acl_to_text(acl, ctypes.byref(length))
            if not text:
                raise OSError(ctypes.get_errno(), "acl_to_text", str(path))
            return ctypes.string_at(text, length.value)
        finally:
            if text:
                self.lib.acl_free(text)
            self.lib.acl_free(acl)

    def attributes(self, path):
        encoded = os.fsencode(path)
        size = self.lib.listxattr(encoded, None, 0, 0)
        if size < 0:
            raise OSError(ctypes.get_errno(), "listxattr", str(path))
        names = ctypes.create_string_buffer(size)
        if self.lib.listxattr(encoded, names, size, 0) < 0:
            raise OSError(ctypes.get_errno(), "listxattr", str(path))
        values = {}
        for name in names.raw.split(b"\0"):
            if not name:
                continue
            size = self.lib.getxattr(encoded, name, None, 0, 0, 0)
            if size < 0:
                raise OSError(ctypes.get_errno(), "getxattr", str(path))
            value = ctypes.create_string_buffer(size)
            if self.lib.getxattr(encoded, name, value, size, 0, 0) != size:
                raise OSError("extended attribute changed during inspection")
            values[name] = value.raw
        return values

    def share(self, source, target, source_info, target_info):
        if source_info.st_dev != target_info.st_dev:
            return False
        if source_info.st_flags or target_info.st_flags:
            return False
        target_acl = self.acl(target)
        if self.acl(source) != target_acl or self.attributes(source) != self.attributes(target):
            return False
        temporary = target.with_name(".disk-share-" + uuid.uuid4().hex)
        # CLONE_NOOWNERCOPY | CLONE_ACL: inherit directory group, retain matching ACL.
        if self.lib.clonefile(os.fsencode(source), os.fsencode(temporary), 6):
            raise OSError(ctypes.get_errno(), "clonefile", str(target))
        try:
            created = temporary.stat()
            if (created.st_uid, created.st_gid) != (target_info.st_uid, target_info.st_gid):
                return False
            os.chmod(temporary, stat.S_IMODE(target_info.st_mode))
            os.utime(temporary, ns=(target_info.st_atime_ns, target_info.st_mtime_ns))
            if (fingerprint(source.stat()) != fingerprint(source_info)
                    or fingerprint(target.stat()) != fingerprint(target_info)
                    or target.is_symlink() or source.is_symlink()):
                return False
            if self.acl(temporary) != target_acl or not filecmp.cmp(temporary, target, shallow=False):
                return False
            os.replace(temporary, target)
            return True
        finally:
            temporary.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", action="append", type=Path, required=True)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--warn-below-gib", type=float, default=40)
    parser.add_argument("--notify", action="store_true")
    args = parser.parse_args()
    if sys.platform != "darwin":
        parser.error("copy-on-write maintenance requires macOS/APFS")
    args.state.parent.mkdir(parents=True, exist_ok=True)
    with args.state.with_suffix(".lock").open("w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        previous = json.loads(args.state.read_text()) if args.state.exists() else {}
        completed = previous.get("completed", {})
        retained, canonical = {}, {}
        report = {"time": time.time(), "free_before": shutil.disk_usage(Path.home()).free,
                  "files_shared": 0, "identical_bytes": 0, "errors": []}
        apfs = Apfs()
        for directory in git_directories(args.root):
            try:
                for key, path, info in objects(directory):
                    name = str(path)
                    signature = fingerprint(info)
                    if key not in canonical:
                        canonical[key] = path
                        continue
                    source = canonical[key]
                    if completed.get(name) == signature:
                        retained[name] = signature
                        continue
                    source_info = source.stat()
                    if source_info.st_ino == info.st_ino or not filecmp.cmp(source, path, shallow=False):
                        continue
                    if args.apply and apfs.share(source, path, source_info, info):
                        retained[name] = fingerprint(path.stat())
                        report["files_shared"] += 1
                        report["identical_bytes"] += info.st_size
                    elif not args.apply:
                        report["identical_bytes"] += info.st_size
            except OSError as error:
                if len(report["errors"]) < 20:
                    report["errors"].append(str(error))
        report["free_after"] = shutil.disk_usage(Path.home()).free
        report["below_warning_threshold"] = report["free_after"] < args.warn_below_gib * 2**30
        notified = previous.get("notified", 0)
        if args.notify and report["below_warning_threshold"] and time.time() - notified > 6 * 3600:
            import subprocess
            free = report["free_after"] / 2**30
            result = subprocess.run(["/usr/bin/osascript", "-e",
                f"display notification \"{free:.1f} GiB free. Active work and history were preserved.\" with title \"Disk headroom warning\""],
                capture_output=True, timeout=10)
            if result.returncode == 0:
                notified = time.time()
        if args.apply:
            temporary = args.state.with_suffix(".new")
            temporary.write_text(json.dumps({"completed": retained, "report": report, "notified": notified}))
            temporary.replace(args.state)
        print(json.dumps(report), flush=True)


if __name__ == "__main__":
    main()
