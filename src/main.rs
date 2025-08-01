use std::collections::HashMap;
use std::io::{self, Read, Write, BufReader, BufRead};
use std::net::TcpStream;
use std::time::Duration;

/// Kafka API Keys - these identify the type of request
#[derive(Debug, Clone, Copy)]
#[repr(i16)]
enum ApiKey {
    ApiVersions = 18,
    Metadata = 3,
    Fetch = 1,
}

/// Error types for our Kafka client
#[derive(Debug)]
enum KafkaError {
    IoError(io::Error),
    ProtocolError(String),
    InvalidResponse(String),
    ConnectionError(String),
}

impl From<io::Error> for KafkaError {
    fn from(error: io::Error) -> Self {
        KafkaError::IoError(error)
    }
}

/// Represents a Kafka broker connection and basic protocol implementation
struct KafkaClient {
    stream: TcpStream,
    correlation_id: i32,
}

impl KafkaClient {
    /// Connect to Kafka broker
    fn connect(broker_address: &str) -> Result<Self, KafkaError> {
        println!("Connecting to Kafka broker at: {}", broker_address);
        
        let stream = TcpStream::connect(broker_address)
            .map_err(|e| KafkaError::ConnectionError(format!("Failed to connect: {}", e)))?;
        
        // Set read timeout to prevent hanging
        stream.set_read_timeout(Some(Duration::from_secs(10)))?;
        stream.set_write_timeout(Some(Duration::from_secs(10)))?;
        
        println!("Successfully connected to Kafka broker");
        
        Ok(KafkaClient {
            stream,
            correlation_id: 1,
        })
    }
    
    /// Get next correlation ID for request tracking
    fn next_correlation_id(&mut self) -> i32 {
        let id = self.correlation_id;
        self.correlation_id += 1;
        id
    }
    
    /// Send API Versions request to discover supported protocol versions
    /// This demonstrates the basic Kafka request/response pattern
    fn send_api_versions_request(&mut self) -> Result<(), KafkaError> {
        println!("\n=== Sending API Versions Request ===");
        
        let correlation_id = self.next_correlation_id();
        
        // Build API Versions request
        // Kafka protocol structure:
        // [Message Size: 4 bytes] [API Key: 2 bytes] [API Version: 2 bytes] 
        // [Correlation ID: 4 bytes] [Client ID: string] [Request Body...]
        
        let mut request = Vec::new();
        
        // API Key (18 = ApiVersions)
        request.extend_from_slice(&(ApiKey::ApiVersions as i16).to_be_bytes());
        println("API Key (ApiVersions): {}", ApiKey::ApiVersions as i16);
        
        // API Version (we'll use version 3 for ApiVersions)
        let api_version: i16 = 3;
        request.extend_from_slice(&api_version.to_be_bytes());
        println!("API Version: {}", api_version);
        
        // Correlation ID
        request.extend_from_slice(&correlation_id.to_be_bytes());
        println!("Correlation ID: {}", correlation_id);
        
        // Client ID - Kafka string format: [length: 2 bytes] [string data]
        let client_id = "rust-std-client";
        let client_id_len = client_id.len() as i16;
        request.extend_from_slice(&client_id_len.to_be_bytes());
        request.extend_from_slice(client_id.as_bytes());
        println!("Client ID: {} (length: {})", client_id, client_id_len);
        
        // For ApiVersions v3, we need to add tagged fields (empty for basic request)
        // Tagged fields length (0 = no tagged fields)
        request.push(0);
        
        // Calculate total message size (excluding the size field itself)
        let message_size = request.len() as i32;
        
        // Send message size first (big-endian)
        self.stream.write_all(&message_size.to_be_bytes())?;
        println!("Message size: {} bytes", message_size);
        
        // Send the actual request
        self.stream.write_all(&request)?;
        self.stream.flush()?;
        
        println!("API Versions request sent successfully");
        println!("Raw request bytes: {:?}", request);
        
        // Read response
        self.read_api_versions_response(correlation_id)
    }
    
    /// Read and parse API Versions response
    fn read_api_versions_response(&mut self, expected_correlation_id: i32) -> Result<(), KafkaError> {
        println!("\n=== Reading API Versions Response ===");
        
        // Read response size (4 bytes, big-endian)
        let mut size_bytes = [0u8; 4];
        self.stream.read_exact(&mut size_bytes)?;
        let response_size = i32::from_be_bytes(size_bytes);
        println!("Response size: {} bytes", response_size);
        
        if response_size <= 0 || response_size > 1024 * 1024 {
            return Err(KafkaError::ProtocolError(
                format!("Invalid response size: {}", response_size)
            ));
        }
        
        // Read the full response
        let mut response_data = vec![0u8; response_size as usize];
        self.stream.read_exact(&mut response_data)?;
        
        println!("Raw response bytes (first 50): {:?}", 
                &response_data[..std::cmp::min(50, response_data.len())]);
        
        // Parse response header
        let mut offset = 0;
        
        // Correlation ID (4 bytes)
        if response_data.len() < 4 {
            return Err(KafkaError::InvalidResponse("Response too short".to_string()));
        }
        
        let correlation_id = i32::from_be_bytes([
            response_data[offset], response_data[offset + 1],
            response_data[offset + 2], response_data[offset + 3]
        ]);
        offset += 4;
        
        println!("Response correlation ID: {}", correlation_id);
        
        if correlation_id != expected_correlation_id {
            return Err(KafkaError::ProtocolError(
                format!("Correlation ID mismatch: expected {}, got {}", 
                       expected_correlation_id, correlation_id)
            ));
        }
        
        // Error code (2 bytes)
        if response_data.len() < offset + 2 {
            return Err(KafkaError::InvalidResponse("Response too short for error code".to_string()));
        }
        
        let error_code = i16::from_be_bytes([
            response_data[offset], response_data[offset + 1]
        ]);
        offset += 2;
        
        println!("Error code: {}", error_code);
        
        if error_code != 0 {
            return Err(KafkaError::ProtocolError(
                format!("Kafka error code: {}", error_code)
            ));
        }
        
        // Parse API versions array
        // Array length (4 bytes in older versions, but newer versions use compact arrays)
        if response_data.len() < offset + 1 {
            return Err(KafkaError::InvalidResponse("Response too short for array length".to_string()));
        }
        
        // For ApiVersions v3, this uses compact arrays (varint + 1)
        let array_length = self.read_varint(&response_data, &mut offset)? as i32 - 1;
        println!("Number of supported APIs: {}", array_length);
        
        // Parse a few API versions to demonstrate
        for i in 0..std::cmp::min(3, array_length) {
            if offset + 6 > response_data.len() {
                break;
            }
            
            let api_key = i16::from_be_bytes([
                response_data[offset], response_data[offset + 1]
            ]);
            offset += 2;
            
            let min_version = i16::from_be_bytes([
                response_data[offset], response_data[offset + 1]
            ]);
            offset += 2;
            
            let max_version = i16::from_be_bytes([
                response_data[offset], response_data[offset + 1]
            ]);
            offset += 2;
            
            println!("  API {}: {} (versions {}-{})", i, api_key, min_version, max_version);
            
            // Skip tagged fields for this API version entry
            let _tagged_fields = self.read_varint(&response_data, &mut offset)?;
        }
        
        println!("API Versions response parsed successfully");
        Ok(())
    }
    
    /// Send Metadata request to get topic and partition information
    fn send_metadata_request(&mut self, topics: &[&str]) -> Result<(), KafkaError> {
        println!("\n=== Sending Metadata Request ===");
        
        let correlation_id = self.next_correlation_id();
        let mut request = Vec::new();
        
        // API Key (3 = Metadata)
        request.extend_from_slice(&(ApiKey::Metadata as i16).to_be_bytes());
        
        // API Version (using version 9 for Metadata)
        let api_version: i16 = 9;
        request.extend_from_slice(&api_version.to_be_bytes());
        
        // Correlation ID
        request.extend_from_slice(&correlation_id.to_be_bytes());
        
        // Client ID
        let client_id = "rust-std-client";
        let client_id_len = client_id.len() as i16;
        request.extend_from_slice(&client_id_len.to_be_bytes());
        request.extend_from_slice(client_id.as_bytes());
        
        // Topics array (compact array format for v9+)
        // Length + 1 encoded as varint
        self.write_varint(&mut request, (topics.len() + 1) as u32);
        
        println!("Requesting metadata for {} topics", topics.len());
        
        for topic in topics {
            // Topic name (compact string: length as varint + string)
            self.write_varint(&mut request, (topic.len() + 1) as u32);
            request.extend_from_slice(topic.as_bytes());
            println!("  Topic: {}", topic);
        }
        
        // Include all topics flag (false)
        request.push(0);
        
        // Allow auto topic creation (false)
        request.push(0);
        
        // Include cluster authorized operations (false)
        request.push(0);
        
        // Include topic authorized operations (false)
        request.push(0);
        
        // Tagged fields (empty)
        request.push(0);
        
        // Send the request
        let message_size = request.len() as i32;
        self.stream.write_all(&message_size.to_be_bytes())?;
        self.stream.write_all(&request)?;
        self.stream.flush()?;
        
        println!("Metadata request sent successfully");
        
        // Read response (simplified parsing)
        self.read_metadata_response(correlation_id)
    }
    
    /// Read and parse Metadata response (simplified)
    fn read_metadata_response(&mut self, expected_correlation_id: i32) -> Result<(), KafkaError> {
        println!("\n=== Reading Metadata Response ===");
        
        // Read response size
        let mut size_bytes = [0u8; 4];
        self.stream.read_exact(&mut size_bytes)?;
        let response_size = i32::from_be_bytes(size_bytes);
        println!("Response size: {} bytes", response_size);
        
        // Read full response
        let mut response_data = vec![0u8; response_size as usize];
        self.stream.read_exact(&mut response_data)?;
        
        let mut offset = 0;
        
        // Correlation ID
        let correlation_id = i32::from_be_bytes([
            response_data[offset], response_data[offset + 1],
            response_data[offset + 2], response_data[offset + 3]
        ]);
        offset += 4;
        
        println!("Correlation ID: {}", correlation_id);
        
        if correlation_id != expected_correlation_id {
            return Err(KafkaError::ProtocolError("Correlation ID mismatch".to_string()));
        }
        
        // For brevity, we'll just show that we received a response
        // Full metadata parsing would require handling:
        // - Throttle time
        // - Brokers array (with host, port, rack info)
        // - Cluster ID
        // - Controller ID
        // - Topics array (with partitions, replicas, ISR, etc.)
        
        println!("Metadata response received (parsing truncated for demonstration)");
        println!("Raw response preview: {:?}", &response_data[..std::cmp::min(50, response_data.len())]);
        
        println!("\n*** COMPLEXITY DEMONSTRATION ***");
        println!("This simple metadata response contains:");
        println!("- Broker information (host, port, rack)");
        println!("- Topic partition assignments");
        println!("- Replica and ISR (In-Sync Replica) information");
        println!("- Leader election state");
        println!("- Authorization and throttling data");
        println!("- Compact vs standard array formats");
        println!("- Tagged fields for future compatibility");
        println!("A full implementation would need hundreds of lines just for metadata!");
        
        Ok(())
    }
    
    /// Demonstrate a basic Fetch request structure (not fully implemented)
    fn demonstrate_fetch_complexity(&self) {
        println!("\n=== Fetch Request Complexity Demonstration ===");
        println!("A real Fetch request would need to handle:");
        println!("1. Partition assignment and offset management");
        println!("2. Consumer group coordination protocol");
        println!("3. Heartbeat and session management");
        println!("4. Partition rebalancing");
        println!("5. Offset commit/rollback logic");
        println!("6. Message decompression (gzip, snappy, lz4, zstd)");
        println!("7. Message format versions (v0, v1, v2)");
        println!("8. Record batches and transaction support");
        println!("9. Error handling for 30+ different error codes");
        println!("10. Security (SASL, SSL/TLS)");
        println!("11. Idempotent producers and exactly-once semantics");
        println!("12. Quota management and throttling");
        println!("\nThis is why libraries like rdkafka exist!");
    }
    
    /// Helper function to read variable-length integers (varint)
    fn read_varint(&self, data: &[u8], offset: &mut usize) -> Result<u32, KafkaError> {
        let mut result = 0u32;
        let mut shift = 0;
        
        loop {
            if *offset >= data.len() {
                return Err(KafkaError::InvalidResponse("Unexpected end of varint".to_string()));
            }
            
            let byte = data[*offset];
            *offset += 1;
            
            result |= ((byte & 0x7F) as u32) << shift;
            
            if (byte & 0x80) == 0 {
                break;
            }
            
            shift += 7;
            if shift >= 32 {
                return Err(KafkaError::ProtocolError("Varint too long".to_string()));
            }
        }
        
        Ok(result)
    }
    
    /// Helper function to write variable-length integers (varint)
    fn write_varint(&self, buffer: &mut Vec<u8>, mut value: u32) {
        loop {
            let mut byte = (value & 0x7F) as u8;
            value >>= 7;
            
            if value != 0 {
                byte |= 0x80;
            }
            
            buffer.push(byte);
            
            if value == 0 {
                break;
            }
        }
    }
}

/// Demonstrate why the Kafka protocol is complex
fn demonstrate_protocol_complexity() {
    println!("\n{}" , "=".repeat(60));
    println!("KAFKA PROTOCOL COMPLEXITY ANALYSIS");
    println!("=".repeat(60));
    
    println!("\n1. BINARY PROTOCOL CHALLENGES:");
    println!("   - Big-endian byte ordering for all multi-byte values");
    println!("   - Variable-length encoding (varints) for efficiency");
    println!("   - Compact vs standard arrays (protocol evolution)");
    println!("   - Tagged fields for backward/forward compatibility");
    
    println!("\n2. PROTOCOL VERSIONING:");
    println!("   - 50+ different API types, each with multiple versions");
    println!("   - Version negotiation required for compatibility");
    println!("   - Schema evolution with strict compatibility rules");
    
    println!("\n3. MESSAGE FORMATS:");
    println!("   - Multiple record batch formats (v0, v1, v2)");
    println!("   - Compression support (gzip, snappy, lz4, zstd)");
    println!("   - Transaction and idempotency headers");
    println!("   - Timestamp handling and message keys");
    
    println!("\n4. CONSUMER GROUP PROTOCOL:");
    println!("   - Group coordination and member management");
    println!("   - Partition assignment strategies");
    println!("   - Rebalancing with cooperative protocols");
    println!("   - Heartbeat and session timeout handling");
    
    println!("\n5. ERROR HANDLING:");
    println!("   - 30+ different error codes");
    println!("   - Retriable vs non-retriable errors");
    println!("   - Backoff and retry strategies");
    println!("   - Network partition handling");
    
    println!("\n6. SECURITY:");
    println!("   - SASL authentication (PLAIN, SCRAM, GSSAPI, OAUTHBEARER)");
    println!("   - SSL/TLS encryption");
    println!("   - ACL authorization");
    println!("   - Delegation tokens");
    
    println!("\nThis is why most developers use existing libraries!");
    println!("A full implementation would require thousands of lines of code.");
}

fn main() -> Result<(), KafkaError> {
    println!("Kafka Standard Library Consumer Demo");
    println!("=====================================");
    
    demonstrate_protocol_complexity();
    
    // Try to connect to Kafka
    println!("\nAttempting to connect to Kafka...");
    println!("Note: This requires a running Kafka broker on localhost:9092");
    
    match KafkaClient::connect("127.0.0.1:9092") {
        Ok(mut client) => {
            println!("Connected successfully!");
            
            // Send API Versions request
            if let Err(e) = client.send_api_versions_request() {
                println!("API Versions request failed: {:?}", e);
                println!("This is expected if Kafka is not running");
            }
            
            // Send Metadata request
            if let Err(e) = client.send_metadata_request(&["test-topic"]) {
                println!("Metadata request failed: {:?}", e);
            }
            
            // Demonstrate fetch complexity
            client.demonstrate_fetch_complexity();
        }
        Err(e) => {
            println!("Failed to connect to Kafka: {:?}", e);
            println!("\nThis is expected if Kafka is not running.");
            println!("The code demonstrates the protocol structure anyway.");
        }
    }
    
    println!("\n" + "=".repeat(60));
    println!("KEY LEARNINGS FOR RUST BEGINNERS:");
    println!("=".repeat(60));
    println!("1. Binary protocol handling with big-endian byte order");
    println!("2. Manual memory management with Vec<u8> and slices");
    println!("3. Error handling with custom enum types");
    println!("4. Network programming with TcpStream");
    println!("5. Bit manipulation for protocol parsing");
    println!("6. Why abstraction layers (crates) are valuable");
    println!("7. The complexity hidden behind simple APIs");
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_varint_encoding() {
        let client = KafkaClient {
            stream: std::net::TcpStream::connect("127.0.0.1:1").unwrap_or_else(|_| {
                // This will fail, but we just need a dummy client for testing
                panic!("Test requires mock setup")
            }),
            correlation_id: 1,
        };
        
        // Test would go here if we had a proper mock setup
        // This demonstrates the testing challenges with network code
    }
    
    #[test]
    fn test_error_types() {
        let io_error = std::io::Error::new(std::io::ErrorKind::Other, "test");
        let kafka_error = KafkaError::from(io_error);
        
        match kafka_error {
            KafkaError::IoError(_) => assert!(true),
            _ => assert!(false, "Expected IoError variant"),
        }
    }
}
