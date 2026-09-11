use std::{env, fs, path::PathBuf};

fn main() {
    let mut output = String::from("Rust examples from the Typst user guide.\n\n");
    for source in ["docs/user-guide.typ", "docs/why-nagoya.typ"] {
        println!("cargo:rerun-if-changed={source}");
        let guide = fs::read_to_string(source).expect("read canonical Typst documentation");
        extract_examples(&guide, &mut output);
    }
    fs::write(
        PathBuf::from(env::var_os("OUT_DIR").unwrap()).join("guide-examples.md"),
        output,
    )
    .expect("write extracted guide examples");
}

fn extract_examples(guide: &str, output: &mut String) {
    let mut in_rust = false;
    let mut no_run = false;
    for line in guide.lines() {
        if !in_rust && line.trim() == "// doctest: no_run" {
            no_run = true;
        } else if !in_rust && line.trim() == "```rust" {
            output.push_str(if no_run {
                "```rust,no_run\n"
            } else {
                "```rust\n"
            });
            output.push_str("# fn main() -> Result<(), Box<dyn std::error::Error>> {\n");
            in_rust = true;
            no_run = false;
        } else if in_rust && line.trim() == "```" {
            output.push_str("# Ok(())\n# }\n```\n\n");
            in_rust = false;
        } else if in_rust {
            output.push_str(line);
            output.push('\n');
        }
    }
    assert!(!in_rust, "unclosed Rust example in guide");
}
