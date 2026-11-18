use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use crate::thrift::{
    ProtocolExceptionKind, TAsyncBinaryProtocol, TAsyncCompactProtocol, TAsyncInputProtocol, TType,
    ThriftException,
};
use tokio::io::{AsyncRead, ReadBuf};

fn assert_protocol_error<T: std::fmt::Debug>(
    result: Result<T, ThriftException>,
    expected: ProtocolExceptionKind,
) {
    let error = result.expect_err("malformed input must fail");
    assert!(
        matches!(&error, ThriftException::Protocol(error) if error.kind() == expected),
        "expected {expected:?}, got {error:?}"
    );
}

macro_rules! both_protocols {
    ($binary:expr, $compact:expr, $bytes:expr, $elements:expr, |$protocol:ident| $body:block) => {{
        {
            let input = $binary;
            let mut $protocol =
                TAsyncBinaryProtocol::with_limits(input.as_slice(), $bytes, $elements);
            $body
        }
        {
            let input = $compact;
            let mut $protocol =
                TAsyncCompactProtocol::with_limits(input.as_slice(), $bytes, $elements);
            $body
        }
    }};
}

fn binary_bytes(value: &[u8]) -> Vec<u8> {
    let mut result = (value.len() as i32).to_be_bytes().to_vec();
    result.extend_from_slice(value);
    result
}

fn compact_bytes(value: &[u8]) -> Vec<u8> {
    assert!(value.len() < 128);
    let mut result = vec![value.len() as u8];
    result.extend_from_slice(value);
    result
}

#[tokio::test]
async fn rejects_negative_binary_lengths() {
    for len in [-1_i32, i32::MIN] {
        let input = len.to_be_bytes();
        let mut protocol = TAsyncBinaryProtocol::new(input.as_slice());
        assert_protocol_error(
            protocol.read_string().await,
            ProtocolExceptionKind::NegativeSize,
        );
        let mut protocol = TAsyncBinaryProtocol::new(input.as_slice());
        assert_protocol_error(
            protocol.read_bytes_vec().await,
            ProtocolExceptionKind::NegativeSize,
        );
        for kind in [0, 1, 2] {
            let mut input = vec![8];
            if kind == 2 {
                input.push(8);
            }
            input.extend_from_slice(&len.to_be_bytes());
            let mut protocol = TAsyncBinaryProtocol::new(input.as_slice());
            let result = match kind {
                0 => protocol.read_list_begin().await.map(|value| value.size),
                1 => protocol.read_set_begin().await.map(|value| value.size),
                _ => protocol.read_map_begin().await.map(|value| value.size),
            };
            assert_protocol_error(result, ProtocolExceptionKind::NegativeSize);
        }
    }
}

#[tokio::test]
async fn rejects_oversized_fields_without_reading_payload() {
    for string in [false, true] {
        both_protocols!(
            i32::MAX.to_be_bytes().to_vec(),
            vec![0xff, 0xff, 0xff, 0xff, 0x0f],
            64,
            8,
            |protocol| {
                let result = if string {
                    protocol.read_string().await.map(String::into_bytes)
                } else {
                    protocol.read_bytes_vec().await
                };
                assert_protocol_error(result, ProtocolExceptionKind::SizeLimit);
            }
        );
    }
}

#[tokio::test]
async fn rejects_invalid_utf8_but_preserves_binary_values() {
    let invalid = [0xff, 0xfe];
    for faststr in [false, true] {
        both_protocols!(
            binary_bytes(&invalid),
            compact_bytes(&invalid),
            32,
            8,
            |protocol| {
                let result = if faststr {
                    protocol.read_faststr().await.map(|value| value.to_string())
                } else {
                    protocol.read_string().await
                };
                assert_protocol_error(result, ProtocolExceptionKind::InvalidData);
            }
        );
    }
    both_protocols!(
        binary_bytes(&invalid),
        compact_bytes(&invalid),
        32,
        8,
        |protocol| {
            assert_eq!(protocol.read_bytes_vec().await.unwrap(), invalid);
        }
    );
    let unicode = "héllo 🦀";
    both_protocols!(
        binary_bytes(unicode.as_bytes()),
        compact_bytes(unicode.as_bytes()),
        32,
        8,
        |protocol| {
            assert_eq!(protocol.read_string().await.unwrap(), unicode);
        }
    );
}

#[tokio::test]
async fn rejects_truncated_fields() {
    both_protocols!(vec![0, 0, 0, 4, b'a'], vec![4, b'a'], 32, 8, |protocol| {
        assert!(matches!(
            protocol.read_string().await,
            Err(ThriftException::Transport(_))
        ));
    });
    both_protocols!(vec![0, 0], Vec::<u8>::new(), 32, 8, |protocol| {
        assert!(matches!(
            protocol.read_bytes_vec().await,
            Err(ThriftException::Transport(_))
        ));
    });
}

#[tokio::test]
async fn enforces_aggregate_byte_limit_across_fields() {
    let mut binary = binary_bytes(b"abc");
    binary.extend(binary_bytes(b"def"));
    let mut compact = compact_bytes(b"abc");
    compact.extend(compact_bytes(b"def"));
    both_protocols!(binary, compact, 7, 8, |protocol| {
        assert_eq!(protocol.read_string().await.unwrap(), "abc");
        assert!(protocol.read_string().await.is_err());
    });
    both_protocols!(vec![0; 9], vec![0; 9], 8, 8, |protocol| {
        protocol.read_double().await.unwrap();
        assert!(protocol.read_byte().await.is_err());
    });
}

#[tokio::test]
async fn bounds_collection_counts_and_aggregate_elements() {
    for kind in [0, 1, 2] {
        let mut binary = vec![8];
        if kind == 2 {
            binary.push(8);
        }
        binary.extend_from_slice(&i32::MAX.to_be_bytes());
        let compact = if kind == 2 {
            vec![0xff, 0xff, 0xff, 0xff, 0x0f, 0x55]
        } else {
            vec![0xf5, 0xff, 0xff, 0xff, 0xff, 0x0f]
        };
        both_protocols!(binary, compact, 64, 8, |protocol| {
            let result = match kind {
                0 => protocol.read_list_begin().await.map(|value| value.size),
                1 => protocol.read_set_begin().await.map(|value| value.size),
                _ => protocol.read_map_begin().await.map(|value| value.size),
            };
            assert_protocol_error(result, ProtocolExceptionKind::SizeLimit);
        });
    }
    // Nested collection headers must share one quota.
    both_protocols!(
        vec![14, 0, 0, 0, 2, 8, 0, 0, 0, 2],
        vec![0x2a, 0x25],
        32,
        3,
        |protocol| {
            assert_eq!(protocol.read_list_begin().await.unwrap().size, 2);
            assert_protocol_error(
                protocol.read_set_begin().await,
                ProtocolExceptionKind::SizeLimit,
            );
        }
    );
    // Each map entry contributes both its key and its value.
    both_protocols!(vec![8, 8, 0, 0, 0, 2], vec![2, 0x55], 32, 3, |protocol| {
        assert_protocol_error(
            protocol.read_map_begin().await,
            ProtocolExceptionKind::SizeLimit,
        );
    });
}

#[tokio::test]
async fn permits_zero_lengths_and_exact_limits() {
    both_protocols!(binary_bytes(b""), compact_bytes(b""), 4, 0, |protocol| {
        assert_eq!(protocol.read_bytes_vec().await.unwrap(), Vec::<u8>::new());
    });
    both_protocols!(
        binary_bytes(b"abc"),
        compact_bytes(b"abcdef"),
        7,
        0,
        |protocol| {
            assert!(!protocol.read_string().await.unwrap().is_empty());
        }
    );
    both_protocols!(vec![8, 8, 0, 0, 0, 2], vec![2, 0x55], 32, 4, |protocol| {
        assert_eq!(protocol.read_map_begin().await.unwrap().size, 2);
    });
}

async fn check_message_reuse<P: TAsyncInputProtocol>(mut protocol: P) {
    for _ in 0..2 {
        let message = protocol.read_message_begin().await.unwrap();
        assert_eq!(message.name.as_str(), "m");
        assert_eq!(protocol.read_string().await.unwrap(), "abc");
        assert_eq!(protocol.read_list_begin().await.unwrap().size, 1);
        assert_eq!(protocol.read_byte().await.unwrap(), 7);
        protocol.read_list_end().await.unwrap();
        protocol.read_message_end().await.unwrap();
    }
}

#[tokio::test]
async fn resets_limits_at_each_message_boundary() {
    let mut binary = vec![0x80, 1, 0, 2];
    binary.extend(binary_bytes(b"m"));
    binary.extend_from_slice(&0_i32.to_be_bytes());
    binary.extend(binary_bytes(b"abc"));
    binary.extend_from_slice(&[3, 0, 0, 0, 1, 7]);
    let binary_len = binary.len();
    binary.extend(binary.clone());
    check_message_reuse(TAsyncBinaryProtocol::with_limits(
        binary.as_slice(),
        binary_len,
        1,
    ))
    .await;
    let mut compact = vec![0x82, 0x41, 0, 1, b'm'];
    compact.extend(compact_bytes(b"abc"));
    compact.extend_from_slice(&[0x13, 7]);
    let compact_len = compact.len();
    compact.extend(compact.clone());
    check_message_reuse(TAsyncCompactProtocol::with_limits(
        compact.as_slice(),
        compact_len,
        1,
    ))
    .await;
}

struct Fragmented<'a> {
    remaining: &'a [u8],
    pending: bool,
}

impl AsyncRead for Fragmented<'_> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.pending {
            this.pending = false;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        let len = this.remaining.len().min(1).min(output.remaining());
        output.put_slice(&this.remaining[..len]);
        this.remaining = &this.remaining[len..];
        this.pending = true;
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn reads_fragmented_payloads_without_consuming_next_field() {
    let mut binary = binary_bytes(b"abc");
    binary.push(42);
    let mut binary = TAsyncBinaryProtocol::new(Fragmented {
        remaining: &binary,
        pending: true,
    });
    assert_eq!(binary.read_string().await.unwrap(), "abc");
    assert_eq!(binary.read_byte().await.unwrap(), 42);
    let mut compact = compact_bytes(b"abc");
    compact.push(42);
    let mut compact = TAsyncCompactProtocol::new(Fragmented {
        remaining: &compact,
        pending: true,
    });
    assert_eq!(compact.read_string().await.unwrap(), "abc");
    assert_eq!(compact.read_byte().await.unwrap(), 42);
}

#[tokio::test]
async fn skips_unknown_binary_fields_without_treating_them_as_text() {
    let invalid_utf8 = [0xff, 0xfe];
    let mut binary = binary_bytes(&invalid_utf8);
    binary.push(42);
    let mut compact = compact_bytes(&invalid_utf8);
    compact.push(42);
    both_protocols!(binary, compact, 32, 8, |protocol| {
        protocol.skip(TType::Binary).await.unwrap();
        assert_eq!(protocol.read_byte().await.unwrap(), 42);
    });
}

#[tokio::test]
async fn default_constructors_enforce_their_limits() {
    let length = (64_i32 * 1024 * 1024 + 1).to_be_bytes();
    let mut binary = TAsyncBinaryProtocol::new(length.as_slice());
    assert_protocol_error(
        binary.read_bytes_vec().await,
        ProtocolExceptionKind::SizeLimit,
    );
    let mut compact = TAsyncCompactProtocol::new([0x81, 0x80, 0x80, 0x20].as_slice());
    assert_protocol_error(
        compact.read_bytes_vec().await,
        ProtocolExceptionKind::SizeLimit,
    );

    let mut binary_input = vec![3];
    binary_input.extend_from_slice(&(1024_i32 * 1024 + 1).to_be_bytes());
    let mut binary = TAsyncBinaryProtocol::new(binary_input.as_slice());
    assert_protocol_error(
        binary.read_list_begin().await,
        ProtocolExceptionKind::SizeLimit,
    );
    let mut compact = TAsyncCompactProtocol::new([0xf3, 0x81, 0x80, 0x40].as_slice());
    assert_protocol_error(
        compact.read_list_begin().await,
        ProtocolExceptionKind::SizeLimit,
    );
}
