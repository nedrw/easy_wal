#[allow(unused_imports)]
pub use tracing::{debug, info, warn};

pub use crate::error::Error;
pub type Result<T> = core::result::Result<T, Error>;
