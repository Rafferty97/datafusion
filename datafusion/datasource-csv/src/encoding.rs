#![cfg(feature = "encoding_rs")]

use std::io::{BufRead, BufReader, Read};
use std::task::Poll;

use bytes::{Buf, Bytes, BytesMut};
use datafusion_common::{DataFusionError, Result};
use encoding_rs::{Decoder, Encoding};
use futures::{Stream, stream::BoxStream};
use pin_project_lite::pin_project;

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

    pub fn convert_read(&self, r: impl Read + Send + 'static) -> Box<dyn Read + Send> {
        Box::new(CharDecoder {
            inner: BufReader::new(r),
            decoder: self.0.new_decoder(),
        })
    }

    pub fn convert_stream<'a>(
        &self,
        s: BoxStream<'a, Result<Bytes>>,
    ) -> BoxStream<'a, Result<Bytes>> {
        Box::pin(AsyncCharDecoder {
            inner: s,
            chunk: None,
            decoder: BufDecoder::new(self.0.new_decoder(), 4096),
            done: false,
        })
    }
}

struct CharDecoder<R> {
    inner: R,
    decoder: Decoder,
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
        chunk: Option<Bytes>,
        decoder: BufDecoder,
        done: bool,
    }
}

impl<S: Stream<Item = Result<Bytes>>> Stream for AsyncCharDecoder<S> {
    type Item = Result<Bytes>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            let mut this = self.as_mut().project();

            if *this.done {
                return Poll::Ready(None);
            }

            let src = match this.chunk {
                Some(chunk) if !chunk.is_empty() => &*chunk.clone(),
                _ => match this.inner.as_mut().poll_next(cx) {
                    Poll::Ready(Some(Ok(bytes))) => {
                        *this.chunk = Some(bytes);
                        // Go around the loop again in case the chunk is empty
                        continue;
                    }
                    Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err))),
                    Poll::Ready(None) => {
                        *this.done = true;
                        let (_, output) = this.decoder.decode(&[], true);
                        if output.is_empty() {
                            return Poll::Ready(None);
                        }
                        return Poll::Ready(Some(Ok(output)));
                    }
                    Poll::Pending => return Poll::Pending,
                },
            };

            let (bytes_read, output) = this.decoder.decode(src, false);
            this.chunk.as_mut().expect("no chunk").advance(bytes_read);

            if !output.is_empty() {
                return Poll::Ready(Some(Ok(output)));
            }
        }
    }
}

struct BufDecoder {
    decoder: Decoder,
    buffer: BytesMut,
    capacity: usize,
}

impl BufDecoder {
    fn new(decoder: Decoder, capacity: usize) -> Self {
        let buffer = BytesMut::new();
        Self {
            decoder,
            buffer,
            capacity,
        }
    }

    fn decode(&mut self, src: &[u8], last: bool) -> (usize, Bytes) {
        if self.buffer.is_empty() {
            self.buffer.resize(self.capacity, 0);
        }
        let dst = &mut self.buffer;
        let (_, read, written, _) = self.decoder.decode_to_utf8(src, dst, last);
        let output = self.buffer.split_to(written).freeze();
        (read, output)
    }
}
