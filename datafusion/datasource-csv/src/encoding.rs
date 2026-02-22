use std::io::Read;

use bytes::Bytes;
use datafusion_common::{DataFusionError, Result};
#[cfg(feature = "encoding_rs")]
use encoding_rs::{Decoder, Encoding};
use futures::stream::BoxStream;

#[cfg(feature = "encoding_rs")]
use internal::{AsyncCharDecoder, CharDecoder};

#[cfg(feature = "encoding_rs")]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct CharEncoding(&'static Encoding);

#[cfg(feature = "encoding_rs")]
impl Default for CharEncoding {
    fn default() -> Self {
        Self(encoding_rs::UTF_8)
    }
}

#[cfg(feature = "encoding_rs")]
impl CharEncoding {
    pub fn for_label(label: &str) -> Result<CharEncoding> {
        match Encoding::for_label(label.as_bytes()) {
            Some(encoding) => Ok(CharEncoding(encoding)),
            None => Err(DataFusionError::Configuration(format!(
                "unsupported character encoding: \"{label}\""
            ))),
        }
    }

    pub fn is_utf8(&self) -> bool {
        self.0 == encoding_rs::UTF_8
    }

    pub fn convert_read(&self, reader: Box<dyn Read + Send>) -> Box<dyn Read + Send> {
        use std::io::BufReader;

        if self.is_utf8() {
            return reader;
        }

        let reader = BufReader::new(reader);
        Box::new(CharDecoder::new(reader, self.new_decoder()))
    }

    pub fn convert_stream<'a>(
        &self,
        stream: BoxStream<'a, Result<Bytes>>,
    ) -> BoxStream<'a, Result<Bytes>> {
        use futures::{StreamExt, TryStreamExt};
        use tokio_util::io::ReaderStream;

        if self.is_utf8() {
            return stream;
        }

        let decoder = AsyncCharDecoder::new(stream, self.new_decoder());
        ReaderStream::new(decoder).map_err(Into::into).boxed()
    }

    fn new_decoder(&self) -> Decoder {
        self.0.new_decoder()
    }
}

#[cfg(not(feature = "encoding_rs"))]
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct CharEncoding;

#[cfg(not(feature = "encoding_rs"))]
impl CharEncoding {
    pub fn for_label(label: &str) -> Result<CharEncoding> {
        let msg = "feature 'encoding_rs' must be enabled to decode non-UTF-8 files";
        match label {
            "utf8" => Ok(Self),
            _ => Err(DataFusionError::Configuration(msg.into())),
        }
    }

    pub fn convert_read(&self, reader: Box<dyn Read + Send>) -> Box<dyn Read + Send> {
        reader
    }

    pub fn convert_stream<'a>(
        &self,
        stream: BoxStream<'a, Result<Bytes>>,
    ) -> BoxStream<'a, Result<Bytes>> {
        stream
    }
}

#[cfg(feature = "encoding_rs")]
mod internal {
    use std::io::{BufRead, Read};
    use std::task::Poll;

    use bytes::{Buf, Bytes};
    use datafusion_common::Result;
    use encoding_rs::Decoder;
    use futures::Stream;
    use pin_project_lite::pin_project;
    use tokio::io::AsyncRead;

    pub struct CharDecoder<R> {
        inner: R,
        decoder: Decoder,
    }

    impl<R: BufRead + Send> CharDecoder<R> {
        pub fn new(inner: R, decoder: Decoder) -> Self {
            Self { inner, decoder }
        }
    }

    impl<R: BufRead + Send> Read for CharDecoder<R> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let src = self.inner.fill_buf()?;
            let (_, read, written, _) =
                self.decoder.decode_to_utf8(src, buf, src.is_empty());
            self.inner.consume(read);
            Ok(written)
        }
    }

    pin_project! {
        pub struct AsyncCharDecoder<S> {
            #[pin]
            inner: S,
            decoder: Decoder,
            chunk: Option<Bytes>,
            done: bool,
        }
    }

    impl<S: Stream<Item = Result<Bytes>>> AsyncCharDecoder<S> {
        pub fn new(inner: S, decoder: Decoder) -> Self {
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
                    let (_, read, written, _) =
                        this.decoder.decode_to_utf8(src, dst, last);
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
                        Poll::Ready(Some(Err(err))) => {
                            return Poll::Ready(Err(err.into()));
                        }
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
}
