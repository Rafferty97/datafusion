#![cfg(feature = "encoding_rs")]

use std::io::{BufRead, BufReader, Read};

use bytes::Bytes;
use datafusion_common::{DataFusionError, Result};
use encoding_rs::{Decoder, Encoding};
use futures::stream::BoxStream;

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
        unimplemented!()
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
