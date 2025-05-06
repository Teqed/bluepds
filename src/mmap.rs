#![allow(clippy::arbitrary_source_item_ordering)]
use std::io::{ErrorKind, Read as _, Seek as _, Write as _};

#[cfg(unix)]
use std::os::fd::AsRawFd as _;
#[cfg(windows)]
use std::os::windows::io::AsRawHandle;

use memmap2::{MmapMut, MmapOptions};

pub(crate) struct MappedFile {
    /// The underlying file handle.
    file: std::fs::File,
    /// The length of the file.
    len: u64,
    /// The mapped memory region.
    map: MmapMut,
    /// Our current offset into the file.
    off: u64,
}

impl MappedFile {
    pub(crate) fn new(mut f: std::fs::File) -> std::io::Result<Self> {
        let len = f.seek(std::io::SeekFrom::End(0))?;

        #[cfg(windows)]
        let raw = f.as_raw_handle();
        #[cfg(unix)]
        let raw = f.as_raw_fd();

        #[expect(unsafe_code)]
        Ok(Self {
            // SAFETY:
            // All file-backed memory map constructors are marked \
            // unsafe because of the potential for Undefined Behavior (UB) \
            // using the map if the underlying file is subsequently modified, in or out of process.
            map: unsafe { MmapOptions::new().map_mut(raw)? },
            file: f,
            len,
            off: 0,
        })
    }

    /// Resize the memory-mapped file. This will reallocate the memory mapping.
    #[expect(unsafe_code)]
    fn resize(&mut self, len: u64) -> std::io::Result<()> {
        // Resize the file.
        self.file.set_len(len)?;

        #[cfg(windows)]
        let raw = self.file.as_raw_handle();
        #[cfg(unix)]
        let raw = self.file.as_raw_fd();

        // SAFETY:
        // All file-backed memory map constructors are marked \
        // unsafe because of the potential for Undefined Behavior (UB) \
        // using the map if the underlying file is subsequently modified, in or out of process.
        self.map = unsafe { MmapOptions::new().map_mut(raw)? };
        self.len = len;

        Ok(())
    }
}

impl std::io::Read for MappedFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.off == self.len {
            // If we're at EOF, return an EOF error code. `Ok(0)` tends to trip up some implementations.
            return Err(std::io::Error::new(ErrorKind::UnexpectedEof, "eof"));
        }

        // Calculate the number of bytes we're going to read.
        let remaining_bytes = self.len.saturating_sub(self.off);
        let buf_len = u64::try_from(buf.len()).unwrap_or(u64::MAX);
        let len = usize::try_from(std::cmp::min(remaining_bytes, buf_len)).unwrap_or(usize::MAX);

        let off = usize::try_from(self.off).map_err(|e| {
            std::io::Error::new(
                ErrorKind::InvalidInput,
                format!("offset too large for this platform: {e}"),
            )
        })?;

        if let (Some(dest), Some(src)) = (
            buf.get_mut(..len),
            self.map.get(off..off.saturating_add(len)),
        ) {
            dest.copy_from_slice(src);
            self.off = self.off.saturating_add(u64::try_from(len).unwrap_or(0));
            Ok(len)
        } else {
            Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                "invalid buffer range",
            ))
        }
    }
}

impl std::io::Write for MappedFile {
    fn flush(&mut self) -> std::io::Result<()> {
        // This is done by the system.
        Ok(())
    }
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // Determine if we need to resize the file.
        let buf_len = u64::try_from(buf.len()).map_err(|e| {
            std::io::Error::new(
                ErrorKind::InvalidInput,
                format!("buffer length too large for this platform: {e}"),
            )
        })?;

        if self.off.saturating_add(buf_len) >= self.len {
            self.resize(self.off.saturating_add(buf_len))?;
        }

        let off = usize::try_from(self.off).map_err(|e| {
            std::io::Error::new(
                ErrorKind::InvalidInput,
                format!("offset too large for this platform: {e}"),
            )
        })?;
        let len = buf.len();

        if let Some(dest) = self.map.get_mut(off..off.saturating_add(len)) {
            dest.copy_from_slice(buf);
            self.off = self.off.saturating_add(buf_len);
            Ok(len)
        } else {
            Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                "invalid buffer range",
            ))
        }
    }
}

impl std::io::Seek for MappedFile {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        let off = match pos {
            std::io::SeekFrom::Start(i) => i,
            std::io::SeekFrom::End(i) => {
                if i <= 0 {
                    // If i is negative or zero, we're seeking backwards from the end
                    // or exactly at the end
                    self.len.saturating_sub(i.unsigned_abs())
                } else {
                    // If i is positive, we're seeking beyond the end, which is allowed
                    // but requires extending the file
                    self.len.saturating_add(i.unsigned_abs())
                }
            }
            std::io::SeekFrom::Current(i) => {
                if i >= 0 {
                    self.off.saturating_add(i.unsigned_abs())
                } else {
                    self.off.saturating_sub(i.unsigned_abs())
                }
            }
        };

        // If the offset is beyond EOF, extend the file to the new size.
        if off > self.len {
            self.resize(off)?;
        }

        self.off = off;
        Ok(off)
    }
}

impl tokio::io::AsyncRead for MappedFile {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let wbuf = buf.initialize_unfilled();
        let len = wbuf.len();

        std::task::Poll::Ready(match self.read(wbuf) {
            Ok(_) => {
                buf.advance(len);
                Ok(())
            }
            Err(e) => Err(e),
        })
    }
}

impl tokio::io::AsyncWrite for MappedFile {
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        std::task::Poll::Ready(self.write(buf))
    }
}

impl tokio::io::AsyncSeek for MappedFile {
    fn poll_complete(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<u64>> {
        std::task::Poll::Ready(Ok(self.off))
    }

    fn start_seek(
        mut self: std::pin::Pin<&mut Self>,
        position: std::io::SeekFrom,
    ) -> std::io::Result<()> {
        self.seek(position).map(|_p| ())
    }
}

#[cfg(test)]
mod test {
    use rand::Rng as _;
    use std::io::Write as _;

    use super::*;

    #[test]
    fn basic_rw() {
        let tmp = std::env::temp_dir().join(
            rand::thread_rng()
                .sample_iter(rand::distributions::Alphanumeric)
                .take(10)
                .map(char::from)
                .collect::<String>(),
        );

        let mut m = MappedFile::new(
            std::fs::File::options()
                .create(true)
                .truncate(true)
                .read(true)
                .write(true)
                .open(&tmp)
                .expect("Failed to open temporary file"),
        )
        .expect("Failed to create MappedFile");

        m.write_all(b"abcd123").expect("Failed to write data");
        let _: u64 = m
            .seek(std::io::SeekFrom::Start(0))
            .expect("Failed to seek to start");

        let mut buf = [0_u8; 7];
        m.read_exact(&mut buf).expect("Failed to read data");

        assert_eq!(&buf, b"abcd123");

        drop(m);
        std::fs::remove_file(tmp).expect("Failed to remove temporary file");
    }
}
