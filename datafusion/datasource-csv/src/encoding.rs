#![cfg(feature = "encoding_rs")]

use std::{
    io::{BufRead, BufReader, Read},
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Buf, Bytes, BytesMut};
use datafusion_common::{DataFusionError, Result};
use encoding_rs::{Decoder, Encoding};
use futures::{Stream, stream::BoxStream};

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

struct AsyncCharDecoder<S> {
    inner: S,
    chunk: Option<Bytes>,
    decoder: BufDecoder,
}

impl<S: Stream<Item = Result<Bytes>>> Stream for AsyncCharDecoder<S> {
    type Item = Result<Bytes>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            let (src, last) = match self.as_mut().chunk() {
                Some(chunk) if !chunk.is_empty() => (&*chunk.clone(), false),
                _ => match self.as_mut().inner().as_mut().poll_next(cx) {
                    Poll::Ready(Some(Ok(bytes))) => {
                        *self.as_mut().chunk() = Some(bytes);
                        // Go around the loop again in case the chunk is empty
                        continue;
                    }
                    Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err))),
                    Poll::Ready(None) => (&[] as &[_], true),
                    Poll::Pending => return Poll::Pending,
                },
            };

            let (bytes_read, output) = self.as_mut().decoder().decode(src, last);
            self.as_mut()
                .chunk()
                .as_mut()
                .expect("no chunk")
                .advance(bytes_read);

            return Poll::Ready(Some(Ok(output)));
        }
    }
}

impl<S: Stream<Item = Result<Bytes>>> AsyncCharDecoder<S> {
    fn inner(self: Pin<&mut Self>) -> Pin<&mut S> {
        // SAFETY: `self.inner` is always pin projected
        unsafe { self.map_unchecked_mut(|s| &mut s.inner) }
    }

    fn chunk(self: Pin<&mut Self>) -> &mut Option<Bytes> {
        // SAFETY: `self.chunk` is never pin projected
        unsafe { &mut self.get_unchecked_mut().chunk }
    }

    fn decoder(self: Pin<&mut Self>) -> &mut BufDecoder {
        // SAFETY: `self.decoder` is never pin projected
        unsafe { &mut self.get_unchecked_mut().decoder }
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
