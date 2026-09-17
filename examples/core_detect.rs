//! What the pool will size itself to on this machine.

fn main() {
    println!("available_parallelism: {:?}", std::thread::available_parallelism());
    println!("shared pool workers:   {}", nagoya::runtime::background().pool().workers());
}
