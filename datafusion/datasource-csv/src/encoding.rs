#![cfg(feature = "encoding_rs")]

use std::io::{BufRead, Read};
use std::task::Poll;

use bytes::{Buf, Bytes};
use datafusion_common::{DataFusionError, Result};
use encoding_rs::{Decoder, Encoding};
use futures::{Stream, stream::BoxStream};
use futures::{StreamExt, TryStreamExt};
use pin_project_lite::pin_project;
use tokio::io::AsyncRead;
use tokio_util::io::ReaderStream;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct CharEncoding(&'static Encoding);

impl CharEncoding {
    pub fn for_label(label: &str) -> Result<CharEncoding> {
        match Encoding::for_label(label.as_bytes()) {
            Some(encoding) => Ok(CharEncoding(encoding)),
            None => Err(DataFusionError::Configuration(format!(
                "unsupported character encoding: \"{label}\""
            ))),
        }
    }

    pub fn convert_read(&self, r: impl BufRead + Send + 'static) -> Box<dyn Read + Send> {
        Box::new(CharDecoder::new(r, self.0.new_decoder()))
    }

    pub fn convert_stream<'a>(
        &self,
        s: BoxStream<'a, Result<Bytes>>,
    ) -> BoxStream<'a, Result<Bytes>> {
        let reader = AsyncCharDecoder::new(s, self.0.new_decoder());
        ReaderStream::new(reader).map_err(Into::into).boxed()
    }
}

struct CharDecoder<R> {
    inner: R,
    decoder: Decoder,
}

impl<R: BufRead + Send> CharDecoder<R> {
    fn new(inner: R, decoder: Decoder) -> Self {
        Self { inner, decoder }
    }
}

impl<R: BufRead + Send> Read for CharDecoder<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let src = self.inner.fill_buf()?;
        let (_, read, written, _) = self.decoder.decode_to_utf8(src, buf, src.is_empty());
        self.inner.consume(read);
        Ok(written)
    }
}
pin_project! {
    struct AsyncCharDecoder<S> {
        #[pin]
        inner: S,
        decoder: Decoder,
        chunk: Option<Bytes>,
        done: bool,
    }
}

impl<S: Stream<Item = Result<Bytes>>> AsyncCharDecoder<S> {
    fn new(inner: S, decoder: Decoder) -> Self {
        Self {
            inner,
            decoder,
            chunk: None,
            done: false,
        }
    }
}

impl<S: Stream<Item = Result<Bytes>>> AsyncRead for AsyncCharDecoder<S> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        loop {
            let mut this = self.as_mut().project();

            if *this.done {
                return Poll::Ready(Ok(()));
            }

            let mut decode = |src, last| {
                let dst = buf.initialize_unfilled();
                let (_, read, written, _) = this.decoder.decode_to_utf8(src, dst, last);
                buf.advance(written);
                (read, written)
            };

            let chunk = match this.chunk {
                Some(chunk) if !chunk.is_empty() => chunk,
                _ => match this.inner.as_mut().poll_next(cx) {
                    Poll::Ready(Some(Ok(bytes))) => {
                        *this.chunk = Some(bytes);
                        // Go around the loop again in case the chunk is empty
                        continue;
                    }
                    Poll::Ready(Some(Err(err))) => return Poll::Ready(Err(err.into())),
                    Poll::Ready(None) => {
                        *this.done = true;
                        decode(&[], true);
                        return Poll::Ready(Ok(()));
                    }
                    Poll::Pending => return Poll::Pending,
                },
            };

            let (read, written) = decode(chunk, false);
            chunk.advance(read);

            if written > 0 {
                return Poll::Ready(Ok(()));
            }
        }
    }
}
