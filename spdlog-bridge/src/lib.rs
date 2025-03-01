use cxx::SharedPtr;
use std::fmt::Write;
use tracing::instrument::WithSubscriber;
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
        fn log(log: &SharedPtr<SpdLogger>, level: i32, file: &str, line_number: u32, message: &str);
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

// Helper to format event fields
struct JsonVisitor<'a>(&'a mut String);

impl<'a> tracing::field::Visit for JsonVisitor<'a> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if !self.0.is_empty() {
            write!(self.0, ", ").unwrap();
        }
        write!(self.0, "{}={:?}", field.name(), value).unwrap();
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if !self.0.is_empty() {
            write!(self.0, ", ").unwrap();
        }
        write!(self.0, "{}=\"{}\"", field.name(), value).unwrap();
    }

    // Implement other record_* methods as needed
}

impl<S> Layer<S> for SpdlogLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _ctx: tracing_subscriber::layer::Context<'_, S>) {
        // Extract fields via visitor pattern
        let mut message = String::new();
        let mut visitor = JsonVisitor(&mut message);
        event.record(&mut visitor);

        // Extract metadata
        let metadata = event.metadata();
        let file = metadata.file().unwrap_or("");
        let line = metadata.line().unwrap_or(0);
        let level = Self::convert_level(metadata.level());

        ffi::log(&self.logger, level, file, line, message.as_str());
    }
}

fn initialize_logging(logger: SharedPtr<ffi::SpdLogger>) {
    // Create spdlog layer
    let spdlog_layer = SpdlogLayer::new(logger);

    // Set the subscriber globally without calling init()
    let subscriber = tracing_subscriber::registry().with(spdlog_layer);
    tracing::subscriber::set_global_default(subscriber).expect("Failed to set global subscriber");
}
