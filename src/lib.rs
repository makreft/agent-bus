//! agent-bus: a local controller through which AI coding agents in different tools
//! send each other messages and wake each other. See README.md.

pub mod admin;
pub mod bus;
pub mod client;
pub mod config;
pub mod display;
pub mod hook;
pub mod identity;
pub mod mcp;
pub mod model;
pub mod server;
pub mod store;
pub mod util;
