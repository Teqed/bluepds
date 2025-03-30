//! This file is used to tell cargo to recompile the project if the migrations folder changes.
fn main() {
    println!("cargo:rerun-if-changed=migrations");
}
