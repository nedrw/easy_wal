#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Generic {0}")]
    Generic(String),

    #[error("End of file")]
    Eof,

    #[error(transparent)]
    IO(#[from] std::io::Error),
}
