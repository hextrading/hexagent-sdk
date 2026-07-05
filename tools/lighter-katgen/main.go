package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/elliottech/lighter-go/client"
	"github.com/elliottech/lighter-go/types"
	curve "github.com/elliottech/poseidon_crypto/curve/ecgfp5"
	g "github.com/elliottech/poseidon_crypto/field/goldilocks"
	gFp5 "github.com/elliottech/poseidon_crypto/field/goldilocks_quintic_extension"
	p2gnark "github.com/elliottech/poseidon_crypto/hash/poseidon2_goldilocks"
	p2 "github.com/elliottech/poseidon_crypto/hash/poseidon2_goldilocks_plonky2"
	schnorr "github.com/elliottech/poseidon_crypto/signature/schnorr"
)

const (
	chainID      uint32 = 304
	accountIndex int64  = 281474976640824
	apiKeyIndex  uint8  = 3
	expiredAt    int64  = 1751700000000
)

func i64p(v int64) *int64 { return &v }

func main() {
	out := map[string]any{}

	// Fixed 40-byte private key: 0x01..0x28
	skBytes := make([]byte, 40)
	for i := range skBytes {
		skBytes[i] = byte(i + 1)
	}
	skHex := "0x" + hex.EncodeToString(skBytes)
	sk := curve.ScalarElementFromLittleEndianBytes(skBytes)
	out["sk_bytes"] = hex.EncodeToString(skBytes)
	out["sk_canonical"] = hex.EncodeToString(sk.ToLittleEndianBytes())
	pk := schnorr.SchnorrPkFromSk(sk)
	out["pubkey"] = hex.EncodeToString(pk.ToLittleEndianBytes())

	c, err := client.NewTxClient(nil, skHex, accountIndex, apiKeyIndex, chainID)
	if err != nil {
		panic(err)
	}

	// KAT: create order via production path
	coReq := &types.CreateOrderTxReq{
		MarketIndex:      1,
		ClientOrderIndex: 123456789,
		BaseAmount:       20000,
		Price:            628356,
		IsAsk:            1,
		Type:             0, // limit
		TimeInForce:      2, // post-only
		ReduceOnly:       0,
		TriggerPrice:     0,
		OrderExpiry:      1751700600000,
	}
	coOps := &types.TransactOpts{Nonce: i64p(7), ExpiredAt: expiredAt}
	coTx, err := c.GetCreateOrderTransaction(coReq, coOps)
	if err != nil {
		panic(err)
	}
	coJSON, _ := coTx.GetTxInfo()
	out["create_order_hash"] = coTx.SignedHash
	out["create_order_json"] = coJSON

	// KAT: cancel order
	cnReq := &types.CancelOrderTxReq{MarketIndex: 1, Index: 123456789}
	cnOps := &types.TransactOpts{Nonce: i64p(8), ExpiredAt: expiredAt}
	cnTx, err := c.GetCancelOrderTransaction(cnReq, cnOps)
	if err != nil {
		panic(err)
	}
	cnJSON, _ := cnTx.GetTxInfo()
	out["cancel_order_hash"] = cnTx.SignedHash
	out["cancel_order_json"] = cnJSON

	// KAT: cancel all
	caReq := &types.CancelAllOrdersTxReq{TimeInForce: 0, Time: 0}
	caOps := &types.TransactOpts{Nonce: i64p(9), ExpiredAt: expiredAt}
	caTx, err := c.GetCancelAllOrdersTransaction(caReq, caOps)
	if err != nil {
		panic(err)
	}
	caJSON, _ := caTx.GetTxInfo()
	out["cancel_all_hash"] = caTx.SignedHash
	out["cancel_all_json"] = caJSON

	// KAT: modify order
	moReq := &types.ModifyOrderTxReq{
		MarketIndex:  1,
		Index:        123456789,
		BaseAmount:   30000,
		Price:        628400,
		TriggerPrice: 0,
	}
	moOps := &types.TransactOpts{Nonce: i64p(10), ExpiredAt: expiredAt}
	moTx, err := c.GetModifyOrderTransaction(moReq, moOps)
	if err != nil {
		panic(err)
	}
	moJSON, _ := moTx.GetTxInfo()
	out["modify_order_hash"] = moTx.SignedHash
	out["modify_order_json"] = moJSON

	// Auth-token message hash: replicate ConstructAuthToken internals (gnark p2 variant)
	msg := "1751706000:281474976640824:3"
	msgInField, err := g.ArrayFromCanonicalLittleEndianBytes([]byte(msg))
	if err != nil {
		panic(err)
	}
	fields := make([]uint64, 0, len(msgInField))
	for _, f := range msgInField {
		fields = append(fields, f.Uint64())
	}
	out["auth_msg"] = msg
	out["auth_fields"] = fields
	authHashGnark := p2gnark.HashToQuinticExtension(msgInField)
	b := authHashGnark.ToLittleEndianBytes()
	out["auth_hash_gnark"] = hex.EncodeToString(b[:])

	// Same input through the plonky2 variant, to see if the two agree
	plonkyIn := make([]g.GoldilocksField, len(fields))
	for i, v := range fields {
		plonkyIn[i] = g.GoldilocksField(v)
	}
	authHashPlonky := p2.HashToQuinticExtension(plonkyIn)
	out["auth_hash_plonky2"] = hex.EncodeToString(authHashPlonky.ToLittleEndianBytes())

	// Deterministic signature over the create-order hash with fixed k = 0x2a
	kBytes := make([]byte, 40)
	kBytes[0] = 0x2a
	k := curve.ScalarElementFromLittleEndianBytes(kBytes)
	coHashBytes, _ := hex.DecodeString(coTx.SignedHash)
	coHashElem, err := gFp5.FromCanonicalLittleEndianBytes(coHashBytes)
	if err != nil {
		panic(err)
	}
	sigDet := schnorr.SchnorrSignHashedMessage2(coHashElem, sk, k)
	out["create_order_sig_k42"] = hex.EncodeToString(sigDet.ToBytes())
	out["go_self_verify"] = schnorr.IsSchnorrSignatureValid(pk, coHashElem, sigDet)

	// Full auth token via production path (random k inside, still parseable)
	// Token format: {deadline}:{account}:{apikey}:{sig_hex}
	authOps := &types.TransactOpts{FromAccountIndex: i64p(accountIndex)}
	ak := apiKeyIndex
	authOps.ApiKeyIndex = &ak
	authTok, err := types.ConstructAuthToken(c.GetKeyManager(), time.Unix(1751706000, 0), authOps)
	if err != nil {
		panic(err)
	}
	out["auth_token_example"] = authTok

	j, _ := json.MarshalIndent(out, "", " ")
	fmt.Println(string(j))
}
