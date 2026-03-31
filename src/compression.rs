//! 压缩模块
//!
//! 提供记录级压缩功能，支持 Snappy 和 Zstd 压缩算法

/// 压缩算法标记
const COMPRESSION_NONE: u8 = 0x00;
const COMPRESSION_SNAPPY: u8 = 0x01;
const COMPRESSION_ZSTD: u8 = 0x02;

/// 压缩算法
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionAlgo {
    /// 无压缩
    None,
    /// Snappy 压缩（速度快，压缩率中等）
    Snappy,
    /// Zstd 压缩（速度中等，压缩率高）
    Zstd,
}

impl CompressionAlgo {
    /// 从字节标记转换为压缩算法
    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            COMPRESSION_NONE => Some(CompressionAlgo::None),
            COMPRESSION_SNAPPY => Some(CompressionAlgo::Snappy),
            COMPRESSION_ZSTD => Some(CompressionAlgo::Zstd),
            _ => None,
        }
    }

    /// 转换为字节标记
    pub fn to_byte(self) -> u8 {
        match self {
            CompressionAlgo::None => COMPRESSION_NONE,
            CompressionAlgo::Snappy => COMPRESSION_SNAPPY,
            CompressionAlgo::Zstd => COMPRESSION_ZSTD,
        }
    }
}

#[cfg(feature = "compression")]
impl CompressionAlgo {
    /// 压缩数据
    pub fn compress(&self, data: &[u8]) -> crate::Result<Vec<u8>> {
        match self {
            CompressionAlgo::None => Ok(data.to_vec()),
            CompressionAlgo::Snappy => {
                use snap::raw::Encoder;
                let mut encoder = Encoder::new();
                encoder
                    .compress_vec(data)
                    .map_err(|e| crate::Error::Compression(e.to_string()))
            }
            CompressionAlgo::Zstd => {
                zstd::encode_all(data, 0).map_err(|e| crate::Error::Compression(e.to_string()))
            }
        }
    }

    /// 解压数据
    pub fn decompress(&self, data: &[u8]) -> crate::Result<Vec<u8>> {
        match self {
            CompressionAlgo::None => Ok(data.to_vec()),
            CompressionAlgo::Snappy => {
                use snap::raw::Decoder;
                let mut decoder = Decoder::new();
                decoder
                    .decompress_vec(data)
                    .map_err(|e| crate::Error::Compression(e.to_string()))
            }
            CompressionAlgo::Zstd => {
                zstd::decode_all(data).map_err(|e| crate::Error::Compression(e.to_string()))
            }
        }
    }
}

// 当未启用 compression feature 时，提供空实现
#[cfg(not(feature = "compression"))]
impl CompressionAlgo {
    /// 压缩数据
    /// - CompressionAlgo::None: 不压缩，直接返回原数据
    /// - 其他算法: 返回错误（需要启用 compression feature）
    pub fn compress(&self, data: &[u8]) -> crate::Result<Vec<u8>> {
        match self {
            CompressionAlgo::None => Ok(data.to_vec()),
            _ => Err(crate::Error::Compression(
                "Compression feature not enabled".to_string(),
            )),
        }
    }

    /// 解压数据
    /// - CompressionAlgo::None: 不解压，直接返回原数据
    /// - 其他算法: 返回错误（需要启用 compression feature）
    pub fn decompress(&self, data: &[u8]) -> crate::Result<Vec<u8>> {
        match self {
            CompressionAlgo::None => Ok(data.to_vec()),
            _ => Err(crate::Error::Compression(
                "Compression feature not enabled".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_algo_conversion() {
        assert_eq!(CompressionAlgo::None.to_byte(), COMPRESSION_NONE);
        assert_eq!(CompressionAlgo::Snappy.to_byte(), COMPRESSION_SNAPPY);
        assert_eq!(CompressionAlgo::Zstd.to_byte(), COMPRESSION_ZSTD);

        assert_eq!(
            CompressionAlgo::from_byte(COMPRESSION_NONE),
            Some(CompressionAlgo::None)
        );
        assert_eq!(
            CompressionAlgo::from_byte(COMPRESSION_SNAPPY),
            Some(CompressionAlgo::Snappy)
        );
        assert_eq!(
            CompressionAlgo::from_byte(COMPRESSION_ZSTD),
            Some(CompressionAlgo::Zstd)
        );
        assert_eq!(CompressionAlgo::from_byte(0xFF), None);
    }

    #[test]
    #[cfg(feature = "compression")]
    fn test_snappy_compress_decompress() {
        let algo = CompressionAlgo::Snappy;
        // 使用更长的重复数据，确保压缩后确实更小
        let data = b"hello world, this is a test message for compression.
                     This message is repeated multiple times to ensure better compression.
                     Lorem ipsum dolor sit amet, consectetur adipiscing elit.
                     Lorem ipsum dolor sit amet, consectetur adipiscing elit.";

        let compressed = algo.compress(data).unwrap();
        let decompressed = algo.decompress(&compressed).unwrap();

        assert_eq!(decompressed, data);
        // Snappy 应该能压缩这个重复的长数据
        assert!(compressed.len() < data.len());
    }

    #[test]
    #[cfg(feature = "compression")]
    fn test_zstd_compress_decompress() {
        let algo = CompressionAlgo::Zstd;
        // 使用更长的重复数据，确保压缩后确实更小
        let data = b"hello world, this is a test message for compression.
                     This message is repeated multiple times to ensure better compression.
                     Lorem ipsum dolor sit amet, consectetur adipiscing elit.
                     Lorem ipsum dolor sit amet, consectetur adipiscing elit.";

        let compressed = algo.compress(data).unwrap();
        let decompressed = algo.decompress(&compressed).unwrap();

        assert_eq!(decompressed, data);
        // Zstd 应该能压缩这个重复的长数据
        assert!(compressed.len() < data.len());
    }

    #[test]
    #[cfg(feature = "compression")]
    fn test_none_compress_decompress() {
        let algo = CompressionAlgo::None;
        let data = b"test data";

        let compressed = algo.compress(data).unwrap();
        let decompressed = algo.decompress(&compressed).unwrap();

        assert_eq!(decompressed, data);
        // 无压缩应该返回原数据
        assert_eq!(compressed, data);
    }

    #[test]
    #[cfg(not(feature = "compression"))]
    fn test_compression_not_enabled() {
        let algo = CompressionAlgo::Snappy;
        let data = b"test data";

        let result = algo.compress(data);
        assert!(result.is_err());

        let result = algo.decompress(data);
        assert!(result.is_err());
    }
}
