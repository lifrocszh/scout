fn main() {
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc");
    // Rust 2024 marks process environment mutation unsafe; build scripts are single-threaded here.
    unsafe { std::env::set_var("PROTOC", protoc) };
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/coordinator.proto"], &["proto"])
        .expect("compile coordinator protocol");
    println!("cargo:rerun-if-changed=proto/coordinator.proto");
}
