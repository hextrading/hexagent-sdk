module katgen

go 1.23

require (
	github.com/elliottech/lighter-go v0.0.0
	github.com/elliottech/poseidon_crypto v0.0.15
)

// Point at a local checkout of https://github.com/elliottech/lighter-go
replace github.com/elliottech/lighter-go => ../lighter-go
