// use std::io::{Read, Write};
// use std::net::TcpStream;

// // Helper: encode a big-endian i32
// fn write_i32_be(buf: &mut Vec<u8>, value: i32) {
//     buf.extend_from_slice(&value.to_be_bytes());
// }

// fn main() -> std::io::Result<()> {
//     // Change these to match your Kafka cluster
//     let kafka_host = "127.0.0.1";
//     let kafka_port = 9092;
//     let address = format!("{kafka_host}:{kafka_port}");

//     // Connect to the Kafka broker
//     let mut stream = TcpStream::connect(address)?;

//     // Build a minimal ApiVersions request (API key 18, version 3, minimal header)
//     let mut request = Vec::new();

//     // ----- Request Header -----
//     // Placeholder for the request size (to be filled in later)
//     request.extend_from_slice(&[0, 0, 0, 0]); // Will be overwritten with actual length

//     // ApiKey (ApiVersions == 18)
//     write_i32_be(&mut request, 18);
//     // ApiVersion
//     write_i32_be(&mut request, 3);
//     // CorrelationId
//     write_i32_be(&mut request, 1);
//     // ClientId
//     write_i32_be(&mut request, 0); // length 0 string

//     // Fill in the length at the start (request minus the 4 length bytes)
//     let len = (request.len() - 4) as i32;
//     request[0..4].copy_from_slice(&len.to_be_bytes());

//     // Send request
//     stream.write_all(&request)?;

//     // Read and print the response
//     let mut resp_buf = [0u8; 4096];
//     let resp_len = stream.read(&mut resp_buf)?;
//     println!("Received {} bytes from Kafka broker:", resp_len);
//     println!("{:02X?}", &resp_buf[..resp_len]);

//     Ok(())
// }



//! A number guessing game demonstrating modern Rust patterns and idioms.
//! 
//! This game showcases:
//! - Pattern matching and error handling
//! - Ownership and borrowing concepts
//! - Modern Rust 2024 features
//! - Terminal colors for better UX
//! - Idiomatic Rust code structure

use colored::Colorize;
use rand::Rng;
use std::cmp::Ordering;
use std::io::{self, Write};

/// Difficulty levels for the game
#[derive(Debug, Clone, Copy)]
enum Difficulty {
    Easy,   // 1-50
    Medium, // 1-100
    Hard,   // 1-200
}

impl Difficulty {
    /// Returns the maximum number for the difficulty level
    const fn max_number(&self) -> u32 {
        match self {
            Self::Easy => 50,
            Self::Medium => 100,
            Self::Hard => 200,
        }
    }

    /// Returns hints enabled status based on difficulty
    const fn hints_enabled(&self) -> bool {
        matches!(self, Self::Easy | Self::Medium)
    }
}

/// Game statistics to track player performance
#[derive(Debug, Default)]
struct GameStats {
    attempts: u32,
    hints_used: u32,
}

/// Main game structure
struct GuessingGame {
    secret_number: u32,
    difficulty: Difficulty,
    stats: GameStats,
}

impl GuessingGame {
    /// Creates a new game instance with the specified difficulty
    fn new(difficulty: Difficulty) -> Self {
        let secret_number = rand::thread_rng().gen_range(1..=difficulty.max_number());
        
        Self {
            secret_number,
            difficulty,
            stats: GameStats::default(),
        }
    }

    /// Runs the main game loop
    fn play(&mut self) {
        self.display_welcome();
        
        loop {
            match self.get_player_guess() {
                Ok(guess) => {
                    self.stats.attempts += 1;
                    
                    match self.check_guess(guess) {
                        Ordering::Less => {
                            println!("{}", "📈 Too small! Try a bigger number.".yellow());
                            self.maybe_give_hint(guess);
                        }
                        Ordering::Greater => {
                            println!("{}", "📉 Too big! Try a smaller number.".yellow());
                            self.maybe_give_hint(guess);
                        }
                        Ordering::Equal => {
                            self.display_victory();
                            break;
                        }
                    }
                }
                Err(e) => {
                    println!("{} {}", "❌ Error:".red(), e);
                    continue;
                }
            }
            
            println!(); // Add spacing between attempts
        }
    }

    /// Displays the welcome message
    fn display_welcome(&self) {
        println!("\n{}", "🎲 Welcome to the Number Guessing Game! 🎲".green().bold());
        println!("Difficulty: {} (1-{})", 
            format!("{:?}", self.difficulty).cyan(),
            self.difficulty.max_number()
        );
        println!("I'm thinking of a number... Can you guess it?\n");
    }

    /// Gets and validates player input
    fn get_player_guess(&self) -> Result<u32, String> {
        print!("{}", "Enter your guess: ".blue());
        io::stdout().flush().unwrap(); // Ensure prompt is displayed
        
        let mut input = String::new();
        io::stdin()
            .read_line(&mut input)
            .map_err(|_| "Failed to read input".to_string())?;
        
        // Parse and validate the input
        let guess = input
            .trim()
            .parse::<u32>()
            .map_err(|_| "Please enter a valid number".to_string())?;
        
        // Validate range
        if guess < 1 || guess > self.difficulty.max_number() {
            return Err(format!(
                "Number must be between 1 and {}",
                self.difficulty.max_number()
            ));
        }
        
        Ok(guess)
    }

    /// Checks the guess against the secret number
    fn check_guess(&self, guess: u32) -> Ordering {
        println!("You guessed: {}", guess.to_string().bold());
        guess.cmp(&self.secret_number)
    }

    /// Provides hints based on difficulty and proximity
    fn maybe_give_hint(&mut self, guess: u32) {
        if !self.difficulty.hints_enabled() || self.stats.attempts < 3 {
            return;
        }
        
        let difference = (self.secret_number as i32 - guess as i32).abs();
        let max = self.difficulty.max_number() as i32;
        
        // Provide proximity hints
        let hint = match (difference * 100 / max) {
            0..=5 => Some("🔥 You're very close!"),
            6..=15 => Some("🌡️ You're getting warm!"),
            16..=30 => Some("❄️ You're getting cold."),
            _ => None,
        };
        
        if let Some(hint_text) = hint {
            println!("{}", hint_text.magenta());
            self.stats.hints_used += 1;
        }
    }

    /// Displays victory message with statistics
    fn display_victory(&self) {
        println!("\n{}", "🎉 Congratulations! You found it! 🎉".green().bold());
        println!("The number was: {}", self.secret_number.to_string().cyan().bold());
        
        // Performance feedback based on attempts
        let feedback = match (self.stats.attempts, self.difficulty) {
            (1, _) => "🏆 INCREDIBLE! First try!",
            (2..=5, Difficulty::Easy) => "⭐ Excellent performance!",
            (2..=7, Difficulty::Medium) => "⭐ Great job!",
            (2..=10, Difficulty::Hard) => "⭐ Impressive!",
            _ => "✅ Well done!",
        };
        
        println!("{}", feedback.yellow());
        println!(
            "Statistics: {} attempts, {} hints used",
            self.stats.attempts.to_string().bold(),
            self.stats.hints_used.to_string().bold()
        );
    }
}

/// Gets the difficulty selection from the player
fn select_difficulty() -> Difficulty {
    loop {
        println!("{}", "\nSelect difficulty:".cyan().bold());
        println!("1) {} (1-50, with hints)", "Easy".green());
        println!("2) {} (1-100, with hints)", "Medium".yellow());
        println!("3) {} (1-200, no hints)", "Hard".red());
        
        print!("{}", "Your choice (1-3): ".blue());
        io::stdout().flush().unwrap();
        
        let mut input = String::new();
        if io::stdin().read_line(&mut input).is_err() {
            println!("{}", "Failed to read input. Please try again.".red());
            continue;
        }
        
        match input.trim() {
            "1" => return Difficulty::Easy,
            "2" => return Difficulty::Medium,
            "3" => return Difficulty::Hard,
            _ => println!("{}", "Invalid choice. Please enter 1, 2, or 3.".red()),
        }
    }
}

/// Asks if the player wants to play again
fn play_again() -> bool {
    print!("\n{}", "Play again? (y/n): ".cyan());
    io::stdout().flush().unwrap();
    
    let mut input = String::new();
    io::stdin().read_line(&mut input).ok()?;
    
    matches!(input.trim().to_lowercase().as_str(), "y" | "yes")
}

fn main() {
    println!("{}", "🎮 RUST GUESSING GAME 🎮".cyan().bold());
    println!("{}", "====================".cyan());
    
    loop {
        let difficulty = select_difficulty();
        let mut game = GuessingGame::new(difficulty);
        game.play();
        
        if !play_again() {
            break;
        }
    }
    
    println!("\n{}", "Thanks for playing! Goodbye! 👋".green().bold());
}

// ===== How to run this project =====
// 
// 1. Create a new directory for your project:
//    mkdir guessing_game && cd guessing_game
//
// 2. Create the Cargo.toml file with the content above
//
// 3. Create the src directory and main.rs:
//    mkdir src
//    # Then create src/main.rs with the content above
//
// 4. Build and run:
//    cargo run
//
// 5. For optimized release build:
//    cargo build --release
//    ./target/release/guessing_game

// ===== Key Rust 2024 Features & Idioms Used =====
//
// 1. CONST FUNCTIONS:
//    - Used in Difficulty methods for compile-time evaluation
//
// 2. PATTERN MATCHING:
//    - Extensive use of match expressions for control flow
//    - Range patterns (0..=5) for hint proximity
//
// 3. ERROR HANDLING:
//    - Result<T, E> for fallible operations
//    - Custom error messages with String
//    - ? operator for error propagation
//
// 4. OWNERSHIP & BORROWING:
//    - &self for immutable method receivers
//    - &mut self for methods that modify state
//    - No unnecessary cloning or allocation
//
// 5. ENUMS & STRUCTS:
//    - Enum for type-safe difficulty levels
//    - Structs for organizing game state
//    - Default trait implementation
//
// 6. MODERN IDIOMS:
//    - impl blocks for encapsulation
//    - const fn for compile-time computation
//    - Type inference where appropriate
//    - Descriptive variable names
//
// 7. TERMINAL UX:
//    - Colored output for better readability
//    - Emojis for visual feedback
//    - Clear prompts and error messages

