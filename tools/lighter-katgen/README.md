# lighter-katgen

Generates the known-answer vectors pinned in
`crates/hexagent-exchange/src/exchange/lighter/signer.rs` (kat module) using
the **official** lighter-go SDK's production signing path.

```sh
git clone --depth 1 https://github.com/elliottech/lighter-go ../lighter-go
go mod tidy && go run .
```

Never adjust the Rust side to make a mismatching vector pass — regenerate
here and investigate the divergence.
