# Workspace-hack package

This package is meant to collapse the variety of dependencies that rust could retrieve at compilation time depending on 
their activates features and other details. See [cargo-hakari](https://docs.rs/cargo-hakari/latest/cargo_hakari/about/index.html) for rationale and details.

This `workspace-hack` is managed by [`cargo hakari`](https://docs.rs/cargo-hakari/latest/cargo_hakari/about/index.html)
which can be installed by:

```
cargo install --locked cargo-hakari 
```

If you've come here because CI is failing due to the workspace-hack package
needing to be updated you can run the following to update it:

```
cargo hakari generate # workspace-hack Cargo.toml is up-to-date
cargo hakari manage-deps # all workspace crates depend on workspace-hack
```
