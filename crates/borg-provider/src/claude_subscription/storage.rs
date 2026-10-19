use super::MAX_BYTES;
use anyhow::{Context, Result, ensure};
use serde_json::Value;
use std::{
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeychainIdentity {
    pub service: String,
    pub account: String,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CredentialStore {
    File,
    MacOsKeychain(KeychainIdentity),
}

/// Controller-owned persistent selection. An explicit selection always wins
/// over process environment; there is no credential-copy or API-key authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthAuthority {
    config_dir: PathBuf,
    store: CredentialStore,
}
impl AuthAuthority {
    /// Explicit controller selection wins ambient secure/config directories.
    pub fn resolve(selected: Option<&Path>) -> Result<Self> {
        let Some(directory) = selected else {
            return Self::discover();
        };
        #[cfg(not(target_os = "macos"))]
        let store = CredentialStore::File;
        #[cfg(target_os = "macos")]
        let store = CredentialStore::MacOsKeychain(KeychainIdentity::for_config_dir(
            directory,
            &keychain_user(),
        )?);
        Self::selected(directory.to_owned(), store)
    }
    pub fn selected(config_dir: PathBuf, store: CredentialStore) -> Result<Self> {
        ensure!(
            config_dir.is_absolute(),
            "Claude authority must use an absolute persistent directory"
        );
        // Resolve existing symlinks so locks follow the actual authority, not an alias.
        let config_dir = if config_dir.exists() {
            config_dir
                .canonicalize()
                .context("resolve Claude authority directory")?
        } else {
            config_dir
        };
        Ok(Self { config_dir, store })
    }
    pub fn config_dir(&self) -> &Path {
        &self.config_dir
    }
    pub fn store(&self) -> &CredentialStore {
        &self.store
    }
    pub fn credential_file(&self) -> PathBuf {
        self.config_dir.join(".credentials.json")
    }

    /// Default discovery only. Controllers should persist and reuse the returned selection.
    /// A Borg-owned login selected with `borg login claude` comes first, so
    /// Borg does not share (and race) Claude Code's refreshing credentials.
    pub fn discover() -> Result<Self> {
        if let Some(directory) = crate::credentials::claude_subscription_config_dir()? {
            return Self::resolve(Some(&directory));
        }
        let home = std::env::var_os("HOME")
            .filter(|v| !v.is_empty())
            .map(PathBuf::from)
            .context("HOME missing for Claude authority")?;
        let secure = std::env::var_os("CLAUDE_SECURESTORAGE_CONFIG_DIR");
        let config = std::env::var_os("CLAUDE_CONFIG_DIR")
            .filter(|v| !v.is_empty())
            .map(PathBuf::from);
        let directory = match &secure {
            Some(value) if value.is_empty() => home.join(".claude"),
            Some(value) => PathBuf::from(value),
            None => config.clone().unwrap_or_else(|| home.join(".claude")),
        };
        let directory = if directory.is_absolute() {
            directory
        } else {
            std::env::current_dir()?.join(directory)
        };
        #[cfg(not(target_os = "macos"))]
        let store = CredentialStore::File;
        #[cfg(target_os = "macos")]
        let store = {
            let custom = match &secure {
                Some(value) => !value.is_empty(),
                None => config.is_some(),
            };
            // Native hashes the configured string, before absolute/canonical path resolution.
            let raw = secure
                .as_ref()
                .filter(|v| !v.is_empty())
                .map(PathBuf::from)
                .or(config);
            let user = keychain_user();
            CredentialStore::MacOsKeychain(if custom {
                KeychainIdentity::for_config_dir(raw.as_ref().unwrap(), &user)?
            } else {
                KeychainIdentity::default_for_user(&user)
            })
        };
        Self::selected(directory, store)
    }

    pub(super) async fn load(&self) -> Result<Value> {
        let authority = self.clone();
        tokio::task::spawn_blocking(move || authority.read_document())
            .await
            .map_err(|_| anyhow::anyhow!("Claude credential read task failed"))?
    }
    fn read_document(&self) -> Result<Value> {
        let bytes = match &self.store {
            CredentialStore::File => {
                let mut options = fs::OpenOptions::new();
                options.read(true);
                #[cfg(unix)]
                {
                    use std::os::unix::fs::OpenOptionsExt;
                    options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
                }
                let file = options
                    .open(self.credential_file())
                    .map_err(|error| {
                        if error.kind() == std::io::ErrorKind::NotFound {
                            anyhow::anyhow!(MissingAuthority)
                        } else {
                            anyhow::anyhow!("Claude credential read failed")
                        }
                    })
                    .context("Claude subscription missing; reconnect (no API-key fallback)")?;
                ensure!(
                    file.metadata()?.is_file(),
                    "Claude credential authority must be a regular file"
                );
                let mut bytes = Vec::new();
                file.take((MAX_BYTES + 1) as u64)
                    .read_to_end(&mut bytes)
                    .context("read Claude credentials")?;
                bytes
            }
            CredentialStore::MacOsKeychain(identity) => keychain_read(identity)?,
        };
        ensure!(bytes.len() <= MAX_BYTES, "Claude credentials too large");
        let value: Value = serde_json::from_slice(&bytes)
            .map_err(|_| anyhow::anyhow!("malformed Claude credential JSON"))?;
        ensure!(
            value.is_object(),
            "Claude credentials must be a JSON object"
        );
        Ok(value)
    }
    pub(super) async fn save(&self, document: &Value) -> Result<()> {
        let authority = self.clone();
        let bytes = serde_json::to_vec(document)
            .map_err(|_| anyhow::anyhow!("cannot encode Claude credential JSON"))?;
        ensure!(bytes.len() <= MAX_BYTES, "Claude credentials too large");
        tokio::task::spawn_blocking(move || match &authority.store {
            CredentialStore::File => atomic_write(&authority.credential_file(), &bytes),
            CredentialStore::MacOsKeychain(identity) => keychain_write(identity, &bytes),
        })
        .await
        .map_err(|_| anyhow::anyhow!("Claude credential save task failed"))?
    }
    pub(super) async fn write_lock(&self) -> Result<RefreshLock> {
        let paths = [self.config_dir.join(".storage-write.lock")];
        loop {
            if let Some(lock) = RefreshLock::try_acquire(&paths)
                .map_err(|_| anyhow::anyhow!("cannot acquire Claude storage write lock"))?
            {
                return Ok(lock);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    pub(super) async fn lock(&self) -> Result<RefreshLock> {
        loop {
            if let Some(lock) = self.try_lock()? {
                return Ok(lock);
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
    /// `None` while a live process holds the refresh lock; the caller decides
    /// whether to wait for its rotation or keep using a still-valid token.
    pub(super) fn try_lock(&self) -> Result<Option<RefreshLock>> {
        // Existing authorities must not be replaced or have directory permissions changed.
        if !self.config_dir.exists() {
            create_private_dir(&self.config_dir)?;
        }
        let directory = self
            .config_dir
            .canonicalize()
            .context("resolve Claude refresh directory")?;
        let paths = [
            directory.join(".oauth_refresh.lock"),
            PathBuf::from(format!("{}.lock", directory.display())),
        ];
        RefreshLock::try_acquire(&paths)
            .map_err(|_| anyhow::anyhow!("cannot acquire Claude refresh lock"))
    }
}

fn create_private_dir(path: &Path) -> Result<()> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder
        .create(path)
        .context("create private Claude authority directory")
}
fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let directory = path
        .parent()
        .context("Claude authority has no parent directory")?;
    let mut temporary = tempfile::NamedTempFile::new_in(directory)
        .context("create private Claude credential file")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        temporary
            .as_file()
            .set_permissions(fs::Permissions::from_mode(0o600))
            .context("restrict Claude credential file")?;
    }
    temporary
        .write_all(bytes)
        .context("write Claude credentials")?;
    temporary
        .as_file()
        .sync_all()
        .context("sync Claude credentials")?;
    temporary
        .persist(path)
        .map_err(|_| anyhow::anyhow!("replace Claude credential file"))?;
    fs::File::open(directory)?
        .sync_all()
        .context("sync Claude credential directory")?;
    Ok(())
}

// Native proper-lockfile uses mkdir, NOT flock, with `stale: 60000, update: 5000`.
// Holders refresh the directory mtime every 5s; like native, a lock left
// unrefreshed for 60s belongs to a dead process and is reclaimed.
const STALE: Duration = Duration::from_secs(60);
pub(super) struct RefreshLock {
    entries: Vec<(PathBuf, fs::File)>,
    heartbeat: tokio::task::JoinHandle<()>,
}
impl RefreshLock {
    fn try_acquire(paths: &[PathBuf]) -> std::io::Result<Option<Self>> {
        match Self::acquire(paths) {
            Ok(lock) => return Ok(Some(lock)),
            Err(error) if error.kind() != std::io::ErrorKind::AlreadyExists => return Err(error),
            Err(_) => {}
        }
        let mut reclaimed = false;
        for path in paths {
            let Ok(meta) = fs::symlink_metadata(path) else {
                continue;
            };
            if meta.is_dir()
                && meta
                    .modified()
                    .is_ok_and(|at| at.elapsed().is_ok_and(|age| age > STALE))
            {
                reclaimed |= fs::remove_dir_all(path).is_ok();
            }
        }
        if !reclaimed {
            return Ok(None);
        }
        match Self::acquire(paths) {
            Ok(lock) => Ok(Some(lock)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(None),
            Err(error) => Err(error),
        }
    }
    fn acquire(paths: &[PathBuf]) -> std::io::Result<Self> {
        let mut entries = Vec::new();
        for path in paths {
            if let Err(error) = fs::create_dir(path) {
                for (path, _) in &entries {
                    let _ = fs::remove_dir(path);
                }
                return Err(error);
            }
            match fs::File::open(path) {
                Ok(file) => entries.push((path.clone(), file)),
                Err(error) => {
                    let _ = fs::remove_dir(path);
                    for (path, _) in &entries {
                        let _ = fs::remove_dir(path);
                    }
                    return Err(error);
                }
            }
        }
        let heartbeat_entries: Vec<_> = entries
            .iter()
            .map(|(path, file)| Ok((path.clone(), file.try_clone()?)))
            .collect::<std::io::Result<_>>()?;
        let heartbeat = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                interval.tick().await;
                for (path, file) in &heartbeat_entries {
                    if !same_directory(path, file) {
                        return;
                    }
                    if file
                        .set_times(fs::FileTimes::new().set_modified(SystemTime::now()))
                        .is_err()
                    {
                        return;
                    }
                }
            }
        });
        Ok(Self { entries, heartbeat })
    }
    pub(super) fn check(&self) -> Result<()> {
        ensure!(
            !self.heartbeat.is_finished()
                && self
                    .entries
                    .iter()
                    .all(|(path, file)| same_directory(path, file)),
            "Claude refresh lock compromised; credentials kept"
        );
        Ok(())
    }
}
impl Drop for RefreshLock {
    fn drop(&mut self) {
        self.heartbeat.abort();
        for (path, file) in &self.entries {
            if same_directory(path, file) {
                let _ = fs::remove_dir(path);
            }
        }
    }
}
fn same_directory(path: &Path, file: &fs::File) -> bool {
    let Ok(current) = fs::symlink_metadata(path) else {
        return false;
    };
    let Ok(held) = file.metadata() else {
        return false;
    };
    if !current.is_dir() || current.file_type().is_symlink() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        current.dev() == held.dev() && current.ino() == held.ino()
    }
    #[cfg(not(unix))]
    {
        current.created().ok() == held.created().ok()
    }
}

#[cfg(not(target_os = "macos"))]
fn keychain_read(_: &KeychainIdentity) -> Result<Vec<u8>> {
    anyhow::bail!("macOS Keychain authority is not available on this host")
}
#[cfg(not(target_os = "macos"))]
fn keychain_write(_: &KeychainIdentity, _: &[u8]) -> Result<()> {
    anyhow::bail!("macOS Keychain authority is not available on this host")
}

impl KeychainIdentity {
    pub fn default_for_user(user: &str) -> Self {
        Self {
            service: "Claude Code-credentials".into(),
            account: validated_user(user),
        }
    }
    /// Native Lue/Tz: 2.1.296 binary offsets 209490753/209491040.
    /// Hash the NFC configured string, NOT its canonical path. macOS uses native
    /// CoreFoundation normalization; other hosts cannot access a macOS keychain.
    pub fn for_config_dir(directory: &Path, user: &str) -> Result<Self> {
        use sha2::{Digest, Sha256};
        let directory = directory
            .to_str()
            .context("Claude Keychain directory must be UTF-8")?;
        let directory = normalize_keychain_directory(directory)?;
        let suffix = hex::encode(Sha256::digest(directory.as_bytes()));
        Ok(Self {
            service: format!("Claude Code-credentials-{}", &suffix[..8]),
            account: validated_user(user),
        })
    }
}
fn validated_user(user: &str) -> String {
    if !user.is_empty()
        && user
            .bytes()
            .all(|v| v.is_ascii_alphanumeric() || matches!(v, b'.' | b'_' | b'-'))
    {
        user.into()
    } else {
        "claude-code-user".into()
    }
}
#[cfg(target_os = "macos")]
fn keychain_user() -> String {
    if let Ok(user) = std::env::var("USER") {
        if !user.is_empty() {
            return user;
        }
    }
    // Reentrant OS username lookup, equivalent to native os.userInfo().username.
    unsafe {
        let mut record: libc::passwd = std::mem::zeroed();
        let mut result = std::ptr::null_mut();
        let mut buffer = vec![0u8; 16 * 1024];
        if libc::getpwuid_r(
            libc::geteuid(),
            &mut record,
            buffer.as_mut_ptr().cast(),
            buffer.len(),
            &mut result,
        ) == 0
            && !result.is_null()
            && !record.pw_name.is_null()
        {
            return std::ffi::CStr::from_ptr(record.pw_name)
                .to_string_lossy()
                .into_owned();
        }
    }
    "claude-code-user".into()
}

// Generic-password data is UTF-8 JSON, not a JSON-encoded hex string. Native
// dRs at 209491759 parses read data as JSON; `security -X` writes decoded bytes.
// Use OS APIs directly: no executable/runtime dependency and no secrets in argv.
#[cfg(target_os = "macos")]
mod macos {
    use super::*;
    use std::ffi::c_void;
    type Item = *mut c_void;
    #[link(name = "Security", kind = "framework")]
    unsafe extern "C" {
        fn SecKeychainFindGenericPassword(
            keychain: Item,
            service_len: u32,
            service: *const u8,
            account_len: u32,
            account: *const u8,
            length: *mut u32,
            data: *mut *mut c_void,
            item: *mut Item,
        ) -> i32;
        fn SecKeychainItemFreeContent(attributes: Item, data: Item) -> i32;
        fn SecKeychainItemModifyAttributesAndData(
            item: Item,
            attributes: Item,
            length: u32,
            data: *const u8,
        ) -> i32;
        fn SecKeychainAddGenericPassword(
            keychain: Item,
            service_len: u32,
            service: *const u8,
            account_len: u32,
            account: *const u8,
            length: u32,
            data: *const u8,
            item: *mut Item,
        ) -> i32;
    }
    #[link(name = "CoreFoundation", kind = "framework")]
    unsafe extern "C" {
        fn CFRelease(value: Item);
    }
    // Native normalization uses NFC, equivalent to CoreFoundation form C = 2.
    pub(super) fn normalize(input: &str) -> Result<String> {
        const UTF8: u32 = 0x08000100;
        #[link(name = "CoreFoundation", kind = "framework")]
        unsafe extern "C" {
            fn CFStringCreateMutable(allocator: Item, capacity: isize) -> Item;
            fn CFStringAppendCString(string: Item, bytes: *const libc::c_char, encoding: u32);
            fn CFStringNormalize(string: Item, form: isize);
            fn CFStringGetLength(string: Item) -> isize;
            fn CFStringGetMaximumSizeForEncoding(length: isize, encoding: u32) -> isize;
            fn CFStringGetCString(
                string: Item,
                buffer: *mut libc::c_char,
                size: isize,
                encoding: u32,
            ) -> u8;
        }
        let input = std::ffi::CString::new(input).context("invalid Claude keychain directory")?;
        unsafe {
            let string = CFStringCreateMutable(std::ptr::null_mut(), 0);
            ensure!(
                !string.is_null(),
                "cannot normalize Claude keychain directory"
            );
            CFStringAppendCString(string, input.as_ptr(), UTF8);
            CFStringNormalize(string, 2);
            let size = CFStringGetMaximumSizeForEncoding(CFStringGetLength(string), UTF8);
            if !(0..=1024 * 1024).contains(&size) {
                CFRelease(string);
                anyhow::bail!("Claude keychain directory too large");
            }
            let mut buffer = vec![0u8; size as usize + 1];
            let ok = CFStringGetCString(
                string,
                buffer.as_mut_ptr().cast(),
                buffer.len() as isize,
                UTF8,
            );
            CFRelease(string);
            ensure!(
                ok != 0,
                "cannot encode normalized Claude keychain directory"
            );
            let length = buffer
                .iter()
                .position(|byte| *byte == 0)
                .unwrap_or(buffer.len());
            buffer.truncate(length);
            String::from_utf8(buffer).context("invalid normalized Claude keychain directory")
        }
    }
    pub(super) fn read(identity: &KeychainIdentity) -> Result<Vec<u8>> {
        let mut length = 0;
        let mut data = std::ptr::null_mut();
        let status = unsafe {
            SecKeychainFindGenericPassword(
                std::ptr::null_mut(),
                identity.service.len() as u32,
                identity.service.as_ptr(),
                identity.account.len() as u32,
                identity.account.as_ptr(),
                &mut length,
                &mut data,
                std::ptr::null_mut(),
            )
        };
        ensure!(
            status == 0,
            "Claude Keychain read failed (OS status {status}); unlock/reconnect selected authority, no fallback"
        );
        let bytes = if length as usize <= MAX_BYTES && (!data.is_null() || length == 0) {
            if length == 0 {
                Vec::new()
            } else {
                unsafe { std::slice::from_raw_parts(data.cast::<u8>(), length as usize).to_vec() }
            }
        } else {
            unsafe {
                SecKeychainItemFreeContent(std::ptr::null_mut(), data);
            }
            anyhow::bail!("Claude Keychain credentials too large or malformed");
        };
        unsafe {
            SecKeychainItemFreeContent(std::ptr::null_mut(), data);
        }
        Ok(bytes)
    }
    pub(super) fn write(identity: &KeychainIdentity, bytes: &[u8]) -> Result<()> {
        let mut item = std::ptr::null_mut();
        let status = unsafe {
            SecKeychainFindGenericPassword(
                std::ptr::null_mut(),
                identity.service.len() as u32,
                identity.service.as_ptr(),
                identity.account.len() as u32,
                identity.account.as_ptr(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &mut item,
            )
        };
        let status = if status == 0 {
            let changed = unsafe {
                SecKeychainItemModifyAttributesAndData(
                    item,
                    std::ptr::null_mut(),
                    bytes.len() as u32,
                    bytes.as_ptr(),
                )
            };
            unsafe {
                CFRelease(item);
            }
            changed
        } else if status == -25300 {
            unsafe {
                SecKeychainAddGenericPassword(
                    std::ptr::null_mut(),
                    identity.service.len() as u32,
                    identity.service.as_ptr(),
                    identity.account.len() as u32,
                    identity.account.as_ptr(),
                    bytes.len() as u32,
                    bytes.as_ptr(),
                    std::ptr::null_mut(),
                )
            }
        } else {
            status
        };
        ensure!(
            status == 0,
            "Claude Keychain save failed (OS status {status}); no plaintext fallback"
        );
        Ok(())
    }
}
#[cfg(target_os = "macos")]
fn keychain_read(identity: &KeychainIdentity) -> Result<Vec<u8>> {
    macos::read(identity)
}
#[cfg(target_os = "macos")]
fn keychain_write(identity: &KeychainIdentity, bytes: &[u8]) -> Result<()> {
    macos::write(identity, bytes)
}

#[derive(Debug)]
pub(super) struct MissingAuthority;
impl std::fmt::Display for MissingAuthority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Claude subscription authority has no credentials")
    }
}
impl std::error::Error for MissingAuthority {}

#[cfg(not(target_os = "macos"))]
fn normalize_keychain_directory(directory: &str) -> Result<String> {
    ensure!(
        directory.is_ascii(),
        "Unicode keychain derivation on a non-macOS host requires an NFC normalization dependency"
    );
    Ok(directory.to_owned())
}
#[cfg(target_os = "macos")]
fn normalize_keychain_directory(directory: &str) -> Result<String> {
    macos::normalize(directory)
}
