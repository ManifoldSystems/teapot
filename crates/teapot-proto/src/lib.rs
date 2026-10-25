//! Wire format shared by every Teapot implementation (Rust agent, Flutter app).
//!
//! The proxy protocol runs over one bi-directional QUIC stream per proxied TCP
//! connection. See `docs/protocol.md` for the full description.

use tokio::io::{AsyncRead, AsyncReadExt};

/// ALPN for device-to-device proxy traffic.
pub const ALPN_PROXY: &[u8] = b"teapot/proxy/0";

/// Upper bound for a header line, so a peer cannot make us buffer forever.
pub const MAX_LINE: usize = 1024;

/// First line sent by the client (A) on a new stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectRequest {
    /// `host:port`, resolved by the exit node.
    pub target: String,
}

impl ConnectRequest {
    pub fn encode(&self) -> Vec<u8> {
        format!("CONNECT {}\n", self.target).into_bytes()
    }

    pub fn parse(line: &str) -> Result<Self, ProtoError> {
        let target = line
            .strip_prefix("CONNECT ")
            .ok_or(ProtoError::Malformed)?
            .trim();
        if target.is_empty() || !target.contains(':') {
            return Err(ProtoError::Malformed);
        }
        Ok(Self {
            target: target.to_string(),
        })
    }
}

/// Reply line sent by the exit node (B) before raw bytes start flowing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectResponse {
    Ok,
    Err(String),
}

impl ConnectResponse {
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Self::Ok => b"OK\n".to_vec(),
            Self::Err(reason) => format!("ERR {}\n", reason.replace('\n', " ")).into_bytes(),
        }
    }

    pub fn parse(line: &str) -> Result<Self, ProtoError> {
        let line = line.trim();
        if line == "OK" {
            Ok(Self::Ok)
        } else if let Some(reason) = line.strip_prefix("ERR ") {
            Ok(Self::Err(reason.to_string()))
        } else {
            Err(ProtoError::Malformed)
        }
    }
}

#[derive(Debug)]
pub enum ProtoError {
    Malformed,
    TooLong,
    Closed,
    Io(std::io::Error),
}

impl std::fmt::Display for ProtoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Malformed => write!(f, "malformed header line"),
            Self::TooLong => write!(f, "header line longer than {MAX_LINE} bytes"),
            Self::Closed => write!(f, "stream closed before header line"),
            Self::Io(e) => write!(f, "io: {e}"),
        }
    }
}

impl std::error::Error for ProtoError {}

/// Reads one `\n`-terminated line byte by byte.
///
/// Byte-at-a-time on purpose: whatever follows the newline is payload and must
/// stay in the stream for the caller to copy.
pub async fn read_line<R: AsyncRead + Unpin>(r: &mut R) -> Result<String, ProtoError> {
    let mut buf = Vec::with_capacity(64);
    loop {
        let b = match r.read_u8().await {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Err(ProtoError::Closed);
            }
            Err(e) => return Err(ProtoError::Io(e)),
        };
        if b == b'\n' {
            return String::from_utf8(buf).map_err(|_| ProtoError::Malformed);
        }
        if buf.len() >= MAX_LINE {
            return Err(ProtoError::TooLong);
        }
        buf.push(b);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_roundtrip() {
        let req = ConnectRequest {
            target: "example.com:443".into(),
        };
        let line = String::from_utf8(req.encode()).unwrap();
        assert_eq!(ConnectRequest::parse(line.trim_end()).unwrap(), req);
    }

    #[test]
    fn request_rejects_missing_port() {
        assert!(ConnectRequest::parse("CONNECT example.com").is_err());
    }

    #[test]
    fn response_roundtrip() {
        for resp in [
            ConnectResponse::Ok,
            ConnectResponse::Err("lan blocked".into()),
        ] {
            let line = String::from_utf8(resp.encode()).unwrap();
            assert_eq!(ConnectResponse::parse(&line).unwrap(), resp);
        }
    }

    #[tokio::test]
    async fn read_line_leaves_payload() {
        let mut data: &[u8] = b"OK\nrest";
        assert_eq!(read_line(&mut data).await.unwrap(), "OK");
        assert_eq!(data, b"rest");
    }
}
