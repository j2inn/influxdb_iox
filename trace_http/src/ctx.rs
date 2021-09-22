use std::num::{NonZeroU128, NonZeroU64, ParseIntError};
use std::str::FromStr;
use std::sync::Arc;

use http::HeaderMap;
use observability_deps::tracing::info;
use snafu::Snafu;

use trace::ctx::{SpanContext, SpanId, TraceId};
use trace::TraceCollector;

const B3_FLAGS: &str = "X-B3-Flags";
const B3_SAMPLED_HEADER: &str = "X-B3-Sampled";
const B3_TRACE_ID_HEADER: &str = "X-B3-TraceId";
const B3_PARENT_SPAN_ID_HEADER: &str = "X-B3-ParentSpanId";
const B3_SPAN_ID_HEADER: &str = "X-B3-SpanId";

const DEFAULT_JAEGER_TRACE_HEADER: &str = "uber-trace-id";

/// Error decoding SpanContext from transport representation
#[derive(Debug, Snafu)]
pub enum ContextError {
    #[snafu(display("header '{}' not found", header))]
    Missing { header: String },

    #[snafu(display("header '{}' has non-UTF8 content: {}", header, source))]
    InvalidUtf8 {
        header: String,
        source: http::header::ToStrError,
    },

    #[snafu(display("error decoding header '{}': {}", header, source))]
    HeaderDecodeError { header: String, source: DecodeError },
}

/// Error decoding a specific header value
#[derive(Debug, Snafu)]
pub enum DecodeError {
    #[snafu(display("value decode error: {}", source))]
    ValueDecodeError { source: ParseIntError },

    #[snafu(display("Expected \"trace-id:span-id:parent-span-id:flags\""))]
    InvalidJaegerTrace,

    #[snafu(display("value cannot be 0"))]
    ZeroError,
}

impl From<ParseIntError> for DecodeError {
    // Snafu doesn't allow both no context and a custom message
    fn from(source: ParseIntError) -> Self {
        Self::ValueDecodeError { source }
    }
}

fn parse_trace(s: &str) -> Result<TraceId, DecodeError> {
    Ok(TraceId(
        NonZeroU128::new(u128::from_str_radix(s, 16)?).ok_or(DecodeError::ZeroError)?,
    ))
}

fn parse_span(s: &str) -> Result<SpanId, DecodeError> {
    Ok(SpanId(
        NonZeroU64::new(u64::from_str_radix(s, 16)?).ok_or(DecodeError::ZeroError)?,
    ))
}

/// Extracts tracing information such as the `SpanContext`s , if any,
/// from http request headers.
#[derive(Debug, Clone)]
pub struct TraceHeaderParser {
    jaeger_header_name: Arc<str>,
}

impl Default for TraceHeaderParser {
    fn default() -> Self {
        Self {
            jaeger_header_name: DEFAULT_JAEGER_TRACE_HEADER.into(),
        }
    }
}

impl TraceHeaderParser {
    /// Create a new span context parser with default Jaeger trace
    /// header name
    pub fn new() -> Self {
        Default::default()
    }

    /// specify a custom jaeger_trace_context_header_name
    pub fn with_jaeger_header_name(mut self, name: impl AsRef<str>) -> Self {
        self.jaeger_header_name = name.as_ref().into();
        self
    }

    /// Create a SpanContext for the trace described in the request's
    /// headers, if any
    ///
    /// Currently support the following formats:
    /// * <https://github.com/openzipkin/b3-propagation#multiple-headers>
    /// * <https://www.jaegertracing.io/docs/1.21/client-libraries/#propagation-format>
    pub fn parse(
        &self,
        collector: &Arc<dyn TraceCollector>,
        headers: &HeaderMap,
    ) -> Result<Option<SpanContext>, ContextError> {
        let jaeger_header = self.jaeger_header_name.as_ref();
        if headers.contains_key(jaeger_header) {
            decode_jaeger(collector, headers, jaeger_header)
        } else if headers.contains_key(B3_TRACE_ID_HEADER) {
            decode_b3(collector, headers)
        } else {
            Ok(None)
        }
    }
}

/// Decodes headers in the B3 format
fn decode_b3(
    collector: &Arc<dyn TraceCollector>,
    headers: &HeaderMap,
) -> Result<Option<SpanContext>, ContextError> {
    let debug = decoded_header(headers, B3_FLAGS)?
        .map(|header| header == "1")
        .unwrap_or(false);

    let sampled = match debug {
        // Debug implies an accept decision
        true => true,
        false => decoded_header(headers, B3_SAMPLED_HEADER)?
            .map(|value| value == "1" || value == "true")
            .unwrap_or(false),
    };

    if !sampled {
        return Ok(None);
    }

    Ok(Some(SpanContext {
        trace_id: required_header(headers, B3_TRACE_ID_HEADER, parse_trace)?,
        parent_span_id: parsed_header(headers, B3_PARENT_SPAN_ID_HEADER, parse_span)?,
        span_id: required_header(headers, B3_SPAN_ID_HEADER, parse_span)?,
        collector: Some(Arc::clone(collector)),
    }))
}

struct JaegerCtx {
    trace_id: TraceId,
    span_id: SpanId,
    parent_span_id: Option<SpanId>,
    flags: u8,
}

impl FromStr for JaegerCtx {
    type Err = DecodeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use itertools::Itertools;

        // TEMPORARY (#2297)
        info!("traced request {}", s);
        let (trace_id, span_id, parent_span_id, flags) = s
            .split(':')
            .collect_tuple()
            .ok_or(DecodeError::InvalidJaegerTrace)?;

        let trace_id = parse_trace(trace_id)?;
        let span_id = parse_span(span_id)?;
        let parent_span_id = match parse_span(parent_span_id) {
            Ok(span_id) => Some(span_id),
            Err(DecodeError::ZeroError) => None,
            Err(e) => return Err(e),
        };
        let flags = u8::from_str_radix(flags, 16)?;

        Ok(Self {
            trace_id,
            span_id,
            parent_span_id,
            flags,
        })
    }
}

/// Decodes headers in the Jaeger format
fn decode_jaeger(
    collector: &Arc<dyn TraceCollector>,
    headers: &HeaderMap,
    jaeger_header: &str,
) -> Result<Option<SpanContext>, ContextError> {
    let decoded: JaegerCtx = required_header(headers, jaeger_header, FromStr::from_str)?;
    if decoded.flags & 0x01 == 0 {
        return Ok(None);
    }

    Ok(Some(SpanContext {
        trace_id: decoded.trace_id,
        parent_span_id: decoded.parent_span_id,
        span_id: decoded.span_id,
        collector: Some(Arc::clone(collector)),
    }))
}

/// Decodes a given header from the provided HeaderMap to a string
///
/// - Returns Ok(None) if the header doesn't exist
/// - Returns Err if the header fails to decode to a string
/// - Returns Ok(Some(_)) otherwise
fn decoded_header<'a>(
    headers: &'a HeaderMap,
    header: &str,
) -> Result<Option<&'a str>, ContextError> {
    headers
        .get(header)
        .map(|value| {
            value.to_str().map_err(|source| ContextError::InvalidUtf8 {
                header: header.to_string(),
                source,
            })
        })
        .transpose()
}

/// Decodes and parses a given header from the provided HeaderMap
///
/// - Returns Ok(None) if the header doesn't exist
/// - Returns Err if the header fails to decode to a string or fails to parse
/// - Returns Ok(Some(_)) otherwise
fn parsed_header<T, F: FnOnce(&str) -> Result<T, DecodeError>>(
    headers: &HeaderMap,
    header: &str,
    parse: F,
) -> Result<Option<T>, ContextError> {
    decoded_header(headers, header)?
        .map(parse)
        .transpose()
        .map_err(|source| ContextError::HeaderDecodeError {
            source,
            header: header.to_string(),
        })
}

/// Decodes and parses a given required header from the provided HeaderMap
///
/// - Returns Err if the header fails to decode to a string, fails to parse, or doesn't exist
/// - Returns Ok(str) otherwise
fn required_header<T, F: FnOnce(&str) -> Result<T, DecodeError>>(
    headers: &HeaderMap,
    header: &str,
    parse: F,
) -> Result<T, ContextError> {
    parsed_header(headers, header, parse)?.ok_or(ContextError::Missing {
        header: header.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use http::HeaderValue;

    use super::*;

    #[test]
    fn test_decode_b3() {
        let parser = TraceHeaderParser::new();
        let collector: Arc<dyn TraceCollector> = Arc::new(trace::LogTraceCollector::new());

        let mut headers = HeaderMap::new();

        // No headers should be None
        assert!(parser.parse(&collector, &headers).unwrap().is_none());

        headers.insert(B3_TRACE_ID_HEADER, HeaderValue::from_static("ee25f"));
        headers.insert(B3_SAMPLED_HEADER, HeaderValue::from_static("0"));

        // Not sampled
        assert!(parser.parse(&collector, &headers).unwrap().is_none());

        headers.insert(B3_SAMPLED_HEADER, HeaderValue::from_static("1"));

        // Missing required headers
        assert_eq!(
            parser.parse(&collector, &headers).unwrap_err().to_string(),
            "header 'X-B3-SpanId' not found"
        );

        headers.insert(B3_SPAN_ID_HEADER, HeaderValue::from_static("34e"));

        let span = parser.parse(&collector, &headers).unwrap().unwrap();

        assert_eq!(span.span_id.0.get(), 0x34e);
        assert_eq!(span.trace_id.0.get(), 0xee25f);
        assert!(span.parent_span_id.is_none());

        headers.insert(
            B3_PARENT_SPAN_ID_HEADER,
            HeaderValue::from_static("4595945"),
        );

        let span = parser.parse(&collector, &headers).unwrap().unwrap();

        assert_eq!(span.span_id.0.get(), 0x34e);
        assert_eq!(span.trace_id.0.get(), 0xee25f);
        assert_eq!(span.parent_span_id.unwrap().0.get(), 0x4595945);

        headers.insert(B3_SPAN_ID_HEADER, HeaderValue::from_static("not a number"));

        assert_eq!(
            parser.parse(&collector, &headers)
                .unwrap_err()
                .to_string(),
            "error decoding header 'X-B3-SpanId': value decode error: invalid digit found in string"
        );

        headers.insert(B3_SPAN_ID_HEADER, HeaderValue::from_static("0"));

        assert_eq!(
            parser.parse(&collector, &headers).unwrap_err().to_string(),
            "error decoding header 'X-B3-SpanId': value cannot be 0"
        );
    }

    #[test]
    fn test_decode_jaeger() {
        let parser = TraceHeaderParser::new();
        let collector: Arc<dyn TraceCollector> = Arc::new(trace::LogTraceCollector::new());
        let mut headers = HeaderMap::new();

        // Invalid format
        headers.insert(
            DEFAULT_JAEGER_TRACE_HEADER,
            HeaderValue::from_static("invalid"),
        );
        assert_eq!(
            parser.parse(&collector, &headers)
                .unwrap_err()
                .to_string(),
            "error decoding header 'uber-trace-id': Expected \"trace-id:span-id:parent-span-id:flags\""
        );

        // Not sampled
        headers.insert(
            DEFAULT_JAEGER_TRACE_HEADER,
            HeaderValue::from_static("343:4325345:0:0"),
        );
        assert!(parser.parse(&collector, &headers).unwrap().is_none());

        // Sampled
        headers.insert(
            DEFAULT_JAEGER_TRACE_HEADER,
            HeaderValue::from_static("3a43:432e345:0:1"),
        );
        let span = parser.parse(&collector, &headers).unwrap().unwrap();

        assert_eq!(span.trace_id.0.get(), 0x3a43);
        assert_eq!(span.span_id.0.get(), 0x432e345);
        assert!(span.parent_span_id.is_none());

        // Parent span
        headers.insert(
            DEFAULT_JAEGER_TRACE_HEADER,
            HeaderValue::from_static("343:4325345:3434:F"),
        );
        let span = parser.parse(&collector, &headers).unwrap().unwrap();

        assert_eq!(span.trace_id.0.get(), 0x343);
        assert_eq!(span.span_id.0.get(), 0x4325345);
        assert_eq!(span.parent_span_id.unwrap().0.get(), 0x3434);

        // Invalid trace id
        headers.insert(
            DEFAULT_JAEGER_TRACE_HEADER,
            HeaderValue::from_static("0:4325345:3434:1"),
        );
        assert_eq!(
            parser.parse(&collector, &headers).unwrap_err().to_string(),
            "error decoding header 'uber-trace-id': value cannot be 0"
        );

        headers.insert(
            DEFAULT_JAEGER_TRACE_HEADER,
            HeaderValue::from_static("008e813572f53b3a:008e813572f53b3a:0000000000000000:1"),
        );

        let span = parser.parse(&collector, &headers).unwrap().unwrap();

        assert_eq!(span.trace_id.0.get(), 0x008e813572f53b3a);
        assert_eq!(span.span_id.0.get(), 0x008e813572f53b3a);
        assert!(span.parent_span_id.is_none());
    }

    #[test]
    fn test_decode_jaeger_custom_header() {
        let parser = TraceHeaderParser::new().with_jaeger_header_name("my-awesome-header");

        let collector: Arc<dyn TraceCollector> = Arc::new(trace::LogTraceCollector::new());
        let mut headers = HeaderMap::new();

        let value = HeaderValue::from_static("1:2:3:1");

        // Default header is ignored
        headers.insert(DEFAULT_JAEGER_TRACE_HEADER, value.clone());
        assert!(parser.parse(&collector, &headers).unwrap().is_none());

        // custom header is parsed
        let mut headers = HeaderMap::new();
        headers.insert("my-awesome-header", value);
        let span = parser.parse(&collector, &headers).unwrap().unwrap();

        assert_eq!(span.trace_id.0.get(), 1);
        assert_eq!(span.span_id.0.get(), 2);
        assert_eq!(span.parent_span_id.unwrap().get(), 3);
    }
}
