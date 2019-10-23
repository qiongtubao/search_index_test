use std::path::{PathBuf, Path};
use tantivy::directory::{WatchHandle, WatchCallback, ReadOnlySource, MmapDirectory, DirectoryLock, Lock};
use tantivy::{TantivyError, Directory};
use tantivy::directory::error::{OpenReadError, DeleteError, OpenWriteError, IOError, LockError};
use std::io::{BufWriter, Write, Read};
use std::fs::{File, OpenOptions};
use std::io;
use tantivy::directory::error::OpenDirectoryError::IoError;
use fs2::FileExt;
use std::sync::Arc;
use std::sync::RwLock;
#[derive(Clone)]
pub struct OnlyReadDirectory {
    root_path: PathBuf,
    #[cfg(test)]
    test: MmapDirectory,
    watch_router: Arc<RwLock<tantivy::directory::WatchCallbackList>>,
}
impl std::fmt::Debug for OnlyReadDirectory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OnlyReadDirectory")
    }
}

impl OnlyReadDirectory {
    pub fn new(root_path: PathBuf) -> Self {
        OnlyReadDirectory {
            root_path: root_path.clone(),
            #[cfg(test)]
            test: MmapDirectory::open(root_path).expect("open dir"),
            watch_router: Arc::new(RwLock::new(Default::default()))
        }
    }
    pub fn resolve_path(&self, relative_path: &Path) -> PathBuf {
        self.root_path.join(relative_path)
    }
    fn sync_directory(&self) -> Result<(), io::Error> {
        let mut open_opts = OpenOptions::new();
        open_opts.read(true);
        #[cfg(windows)]
            {
                panic!("并没有支持window")
            }
        let fd = open_opts.open(&self.root_path)?;
        fd.sync_all()?;
        Ok(())
    }
}
struct SafeFileWriter(File);
impl SafeFileWriter {
    fn new(file:File) -> SafeFileWriter {
        SafeFileWriter(file)
    }
}
impl Write for SafeFileWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.0.flush()?;
        self.0.sync_all()
    }
}
impl Directory for OnlyReadDirectory {
    fn open_read(&self, path: &Path) -> Result<ReadOnlySource, OpenReadError> {
        let full_path = self.resolve_path(path);
        let mut file = File::open(full_path.clone()).map_err(|e| {
            if e.kind() == io::ErrorKind::NotFound {
                OpenReadError::FileDoesNotExist(full_path.to_owned())
            } else {
                OpenReadError::IOError(IOError::with_path(full_path.to_owned(), e))
            }
        })?;
        let meta_data= file.metadata().map_err(|e| {
            IOError::with_path(full_path.to_owned(), e)
        })?;
        if meta_data.len() == 0 {
            return Ok(ReadOnlySource::empty());
        }
        let len = meta_data.len();
        let mut data= (0..len)
            .map(|_| 0)
            .collect::<Vec<u8>>();
        let result = file.read(&mut data).expect("result");
        #[cfg(test)]
        assert_eq!(self.test.open_read(path).expect("a").data.len(), data.len()); 
        Ok(ReadOnlySource::new(data))

    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        let full_path = self.resolve_path(path);
        match std::fs::remove_file(&full_path) {
            Ok(_) => self.sync_directory().map_err(|e| IOError::with_path(path.to_owned(), e).into()),
            Err(e) => {
                if e.kind() == io::ErrorKind::NotFound {
                    Err(DeleteError::FileDoesNotExist(path.to_owned()))
                } else {
                    Err(IOError::with_path(path.to_owned(), e).into())
                }
            }

        }
    }

    fn exists(&self, path: &Path) -> bool {
       let full_path = self.resolve_path(path);
        full_path.exists()
    }

    fn open_write(&mut self, path: &Path) -> Result<BufWriter<Box<Write>>, OpenWriteError> {
        let full_path = self.resolve_path(path);
        let open_res = OpenOptions::new().write(true).create_new(true).open(full_path);
        let mut file = open_res.map_err(|err| {
            if err.kind() == io::ErrorKind::AlreadyExists {
                OpenWriteError::FileAlreadyExists(path.to_owned())
            } else {
                IOError::with_path(path.to_owned(), err).into()
            }
        })?;
        file.flush().map_err(|e| IOError::with_path(path.to_owned(), e))?;
        self.sync_directory().map_err(|e| IOError::with_path(path.to_owned(), e))?;
        let writer = SafeFileWriter::new(file);
        Ok(BufWriter::new(Box::new(writer)))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let full_path = self.resolve_path(path);
        let mut buffer = Vec::new();
        match File::open(&full_path) {
            Ok(mut file) => {
                file.read_to_end(&mut buffer).map_err(|e| IOError::with_path(path.to_owned(), e))?;
                Ok(buffer)
            }
            Err(e) => {
                if e.kind() == io::ErrorKind::NotFound {
                    Err(OpenReadError::FileDoesNotExist(path.to_owned()))
                } else {
                    Err(IOError::with_path(path.to_owned(), e).into())
                }
            }
        }
//        unimplemented!()
    }

    fn atomic_write(&mut self, path: &Path, data: &[u8]) -> std::io::Result<()> {
        let full_path = self.resolve_path(path);
        let meta_file = atomicwrites::AtomicFile::new(full_path, atomicwrites::AllowOverwrite);
        meta_file.write(|f| f.write_all(data))?;
        Ok(())
    }
    fn acquire_lock(&self, lock: &Lock) -> Result<DirectoryLock, LockError> {
        let full_path = self.resolve_path(&lock.filepath);
        let file: File = OpenOptions::new().write(true).create(true).open(&full_path).map_err(LockError::IOError)?;
        if lock.is_blocking {
            file.lock_exclusive().map_err(LockError::IOError)?
        } else {
            file.try_lock_exclusive().map_err(|_| LockError::LockBusy)?
        }
        Ok(DirectoryLock::from(Box::new(ReleaseLockFile {
            path: lock.filepath.clone(),
            _file: file
        })))
    }

    fn watch(&self, watch_callback: WatchCallback) -> Result<WatchHandle, TantivyError> {
        Ok(self.watch_router.write().unwrap().subscribe(watch_callback))
    }
}

struct ReleaseLockFile {
    _file: File,
    path: PathBuf
}
impl Drop for ReleaseLockFile {
    fn drop(&mut self) {
        
    }
}