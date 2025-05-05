use std::io::{ErrorKind, Read, Seek, Write};

#[cfg(unix)]
use std::os::fd::AsRawFd;
#[cfg(windows)]
use std::os::windows::io::AsRawHandle;

use memmap2::{MmapMut, MmapOptions};

pub struct MappedFile {
    /// The underlying file handle.
    file: std::fs::File,
    /// The mapped memory region.
    map: MmapMut,
    /// The length of the file.
    len: u64,
    /// Our current offset into the file.
    off: u64,
}

impl MappedFile {
    pub fn new(mut f: std::fs::File) -> std::io::Result<Self> {
        let len = f.seek(std::io::SeekFrom::End(0))?;

        #[cfg(windows)]
        let raw = f.as_raw_handle();
        #[cfg(unix)]
        let raw = f.as_raw_fd();

        #[expect(unsafe_code)]
        Ok(Self {
            map: unsafe { MmapOptions::new().map_mut(raw)? },
            file: f,
            len: len,
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
        let len = std::cmp::min(self.len - self.off, buf.len() as u64) as usize;
        let off = self.off as usize;

        buf[..len].copy_from_slice(&self.map[off..off + len]);
        self.off += len as u64;
        Ok(len)
    }
}

impl std::io::Write for MappedFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // Determine if we need to resize the file.
        if self.off + buf.len() as u64 >= self.len {
            self.resize(self.off + buf.len() as u64)?;
        }

        let off = self.off as usize;
        let len = buf.len();

        self.map[off..off + len].copy_from_slice(buf);
        self.off += len as u64;

        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // This is done by the system.
        Ok(())
    }
}

impl std::io::Seek for MappedFile {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        let off = match pos {
            std::io::SeekFrom::Start(i) => i,
            std::io::SeekFrom::End(i) => (self.len as i64 - i) as u64,
            std::io::SeekFrom::Current(i) => (self.off as i64 + i) as u64,
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
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        std::task::Poll::Ready(self.write(buf))
    }

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
}

impl tokio::io::AsyncSeek for MappedFile {
    fn start_seek(
        mut self: std::pin::Pin<&mut Self>,
        position: std::io::SeekFrom,
    ) -> std::io::Result<()> {
        self.seek(position).map(|_p| ())
    }

    fn poll_complete(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<u64>> {
        std::task::Poll::Ready(Ok(self.off as u64))
    }
}

#[cfg(test)]
mod test {
    use rand::Rng;
    use std::io::Write;

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
                .read(true)
                .write(true)
                .open(&tmp)
                .unwrap(),
        )
        .unwrap();

        m.write_all(b"abcd123").unwrap();
        m.seek(std::io::SeekFrom::Start(0)).unwrap();

        let mut buf = [0u8; 7];
        m.read_exact(&mut buf).unwrap();

        assert_eq!(&buf, b"abcd123");

        drop(m);
        std::fs::remove_file(tmp).unwrap();
    }
}
