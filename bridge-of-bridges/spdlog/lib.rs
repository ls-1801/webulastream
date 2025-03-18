use cxx::SharedPtr;
use tracing::span::Attributes;
use tracing::span::Id;
use tracing::span::Record;
use tracing::field::Field;
use tracing_subscriber::layer::Context;
use std::error::Error;
use std::fmt::Write;
use tracing::{Event, Subscriber};
use tracing_subscriber::prelude::*;
use tracing_subscriber::Layer;

#[cxx::bridge]
mod ffi {
    enum Level {
        Debug,
        Info,
        Warn,
        Error,
        Fatal,
    }

    extern "Rust" {
        fn initialize_logging(logger: SharedPtr<SpdLogger>);
    }

    // C++ functions we'll call from Rust
    unsafe extern "C++" {
        include!("bridge.hpp");
        type SpdLogger;
        fn log(log: &SharedPtr<SpdLogger>, level: i32, file: &str, line_number: u32, func: &str, message: &str);
    }
}
unsafe impl Send for ffi::SpdLogger {}
unsafe impl Sync for ffi::SpdLogger {}
pub struct SpdlogLayer {
    logger: SharedPtr<ffi::SpdLogger>,
}

impl SpdlogLayer {
    pub fn new(logger: SharedPtr<ffi::SpdLogger>) -> Self {
        SpdlogLayer { logger }
    }

    fn convert_level(level: &tracing::Level) -> i32 {
        match *level {
            tracing::Level::TRACE => 0,
            tracing::Level::DEBUG => 1,
            tracing::Level::INFO => 2,
            tracing::Level::WARN => 3,
            tracing::Level::ERROR => 4,
        }
    }
}

// Helper to extract the message field
struct MessageExtractor<'a>(&'a mut String);

impl<'a> tracing::field::Visit for MessageExtractor<'a> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            write!(self.0, "{:?}", value).unwrap();
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            *self.0 = value.to_owned();
        }
    }

    // Implement other record_* methods as needed
}

struct ME<'a>(&'a mut String);

impl<'a> tracing::field::Visit for ME<'a> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        *self.0 += if self.0.is_empty() { "" } else { "; " };
        *self.0 += field.name();
        *self.0 += ": ";
        write!(self.0, "{:?}", value).unwrap();
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        *self.0 += if self.0.is_empty() { "" } else { "; " };
        *self.0 += field.name();
        *self.0 += " ";
        *self.0 += value;
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        *self.0 += if self.0.is_empty() { "" } else { "; " };
        *self.0 += field.name();
        *self.0 += " ";
        *self.0 += value.to_string().as_str();
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        *self.0 += if self.0.is_empty() { "" } else { "; " };
        *self.0 += field.name();
        *self.0 += " ";
        *self.0 += value.to_string().as_str();
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        *self.0 += if self.0.is_empty() { "" } else { "; " };
        *self.0 += field.name();
        *self.0 += " ";
        *self.0 += value.to_string().as_str();
    }
    fn record_i128(&mut self, field: &Field, value: i128) {
        *self.0 += if self.0.is_empty() { "" } else { "; " };
        *self.0 += field.name();
        *self.0 += " ";
        *self.0 += value.to_string().as_str();
    }
    fn record_u128(&mut self, field: &Field, value: u128) {
        *self.0 += if self.0.is_empty() { "" } else { "; " };
        *self.0 += field.name();
        *self.0 += " ";
        *self.0 += value.to_string().as_str();
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        *self.0 += if self.0.is_empty() { "" } else { "; " };
        *self.0 += field.name();
        *self.0 += " ";
        *self.0 += value.to_string().as_str();
    }
    fn record_bytes(&mut self, field: &Field, value: &[u8]) {
        *self.0 += if self.0.is_empty() { "" } else { "; " };
        *self.0 += field.name();
        *self.0 += " ";
        *self.0 += value.escape_ascii().to_string().as_str();
    }
    fn record_error(&mut self, field: &Field, value: &(dyn Error + 'static)) {
        *self.0 += if self.0.is_empty() { "" } else { "; " };
        *self.0 += field.name();
        *self.0 += " ";
        *self.0 += value.to_string().as_str();
    }
}

impl<S> Layer<S> for SpdlogLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        // Extract fields via visitor pattern
        let mut message = String::new();
        let mut visitor = MessageExtractor(&mut message);
        event.record(&mut visitor);

        // Extract metadata
        let metadata = event.metadata();
        let file = metadata.file().unwrap_or("");
        let line = metadata.line().unwrap_or(0);
        let level = Self::convert_level(metadata.level());

        ffi::log(&self.logger, level, file, line, "RUST", message.as_str());
    }

    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let metadata = attrs.metadata();
        let file = metadata.file().unwrap_or("");
        let line = metadata.line().unwrap_or(0);
        let func = metadata.name();

        let level = Self::convert_level(metadata.level());

        let mut msg = String::new();
        let mut vis = ME(&mut msg);
        attrs.record(&mut vis);

        let mut s = String::new();
        s.push('(');
        s.push_str(msg.as_str());
        s.push(')');

        ffi::log(&self.logger, level, file, line, func, s.as_str())
    }

    fn on_record(&self, _span: &Id, _values: &Record<'_>, _ctx: Context<'_, S>) {
        // ffi::log(&self.logger, 4, "foo", 3, format!("on_record {:?}", _span).as_str());
    }

    fn on_enter(&self, _id: &Id, _ctx: Context<'_, S>) {
        // ffi::log(&self.logger, 4, "foo", 3, format!("on_enter {:?}", _id).as_str());
    }

}

fn initialize_logging(logger: SharedPtr<ffi::SpdLogger>) {
    // Create spdlog layer
    let spdlog_layer = SpdlogLayer::new(logger);

    // Set the subscriber globally without calling init()
    let subscriber = tracing_subscriber::registry().with(spdlog_layer);
    tracing::subscriber::set_global_default(subscriber).expect("Failed to set global subscriber");
}
