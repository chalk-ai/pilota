use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf, Take};

use super::{ProtocolExceptionKind, ThriftException, new_protocol_exception};

pub(super) const DEFAULT_MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;
pub(super) const DEFAULT_MAX_COLLECTION_ELEMENTS: usize = 1024 * 1024;

pub(super) fn signed_size(size: i32) -> Result<usize, ThriftException> {
    usize::try_from(size).map_err(|_| {
        new_protocol_exception(
            ProtocolExceptionKind::NegativeSize,
            "negative Thrift length",
        )
    })
}

pub(super) struct BoundedReader<R> {
    inner: Take<R>,
    max_message_bytes: usize,
    max_collection_elements: usize,
    remaining_collection_elements: usize,
}

impl<R: AsyncRead + Unpin> BoundedReader<R> {
    pub(super) fn new(reader: R, max_message_bytes: usize, max_collection_elements: usize) -> Self {
        Self {
            inner: reader.take(max_message_bytes as u64),
            max_message_bytes,
            max_collection_elements,
            remaining_collection_elements: max_collection_elements,
        }
    }

    pub(super) fn reset(&mut self) {
        self.inner.set_limit(self.max_message_bytes as u64);
        self.remaining_collection_elements = self.max_collection_elements;
    }

    pub(super) fn collection_size(&mut self, size: usize) -> Result<usize, ThriftException> {
        self.remaining_collection_elements = self
            .remaining_collection_elements
            .checked_sub(size)
            .ok_or_else(|| {
            new_protocol_exception(
                ProtocolExceptionKind::SizeLimit,
                "Thrift message exceeds its collection element limit",
            )
        })?;
        Ok(size)
    }

    pub(super) async fn read_bytes(&mut self, len: usize) -> Result<Vec<u8>, ThriftException> {
        if len as u64 > self.inner.limit() {
            return Err(new_protocol_exception(
                ProtocolExceptionKind::SizeLimit,
                "Thrift field exceeds the remaining message byte limit",
            ));
        }
        // Grow only as bytes arrive. A truncated field must not allocate its declared length.
        let mut bytes = Vec::new();
        (&mut self.inner)
            .take(len as u64)
            .read_to_end(&mut bytes)
            .await?;
        if bytes.len() != len {
            return Err(
                io::Error::new(io::ErrorKind::UnexpectedEof, "truncated Thrift field").into(),
            );
        }
        Ok(bytes)
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for BoundedReader<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

#[cfg(test)]
mod tests;
