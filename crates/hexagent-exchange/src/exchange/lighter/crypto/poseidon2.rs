// Vendored from https://github.com/robustfengbin/lighter-sdk (MIT OR Apache-2.0),
// a pure-Rust port of https://github.com/elliottech/poseidon_crypto (Apache-2.0).
// Correctness is pinned against the official Go implementation by the KAT tests
// in ../signer.rs — regenerate vectors with lighter-go if this file changes.
//! Poseidon2 Hash Function for Goldilocks Field
//!
//! Implementation based on the elliottech/poseidon_crypto Go library.
//! Uses WIDTH=12, RATE=8, and the Poseidon2 permutation with
//! 4 full rounds at the start, 22 partial rounds, and 4 full rounds at the end.

use super::goldilocks::GoldilocksField;
use super::gfp5::GFp5;

// Poseidon2 parameters
const WIDTH: usize = 12;
const RATE: usize = 8;
const ROUNDS_F: usize = 8;
const ROUNDS_F_HALF: usize = 4;
const ROUNDS_P: usize = 22;

// External round constants (8 rounds x 12 elements)
const EXTERNAL_CONSTANTS: [[u64; WIDTH]; ROUNDS_F] = [
    [
        15492826721047263190, 11728330187201910315, 8836021247773420868, 16777404051263952451,
        5510875212538051896, 6173089941271892285, 2927757366422211339, 10340958981325008808,
        8541987352684552425, 9739599543776434497, 15073950188101532019, 12084856431752384512,
    ],
    [
        4584713381960671270, 8807052963476652830, 54136601502601741, 4872702333905478703,
        5551030319979516287, 12889366755535460989, 16329242193178844328, 412018088475211848,
        10505784623379650541, 9758812378619434837, 7421979329386275117, 375240370024755551,
    ],
    [
        3331431125640721931, 15684937309956309981, 578521833432107983, 14379242000670861838,
        17922409828154900976, 8153494278429192257, 15904673920630731971, 11217863998460634216,
        3301540195510742136, 9937973023749922003, 3059102938155026419, 1895288289490976132,
    ],
    [
        5580912693628927540, 10064804080494788323, 9582481583369602410, 10186259561546797986,
        247426333829703916, 13193193905461376067, 6386232593701758044, 17954717245501896472,
        1531720443376282699, 2455761864255501970, 11234429217864304495, 4746959618548874102,
    ],
    [
        13571697342473846203, 17477857865056504753, 15963032953523553760, 16033593225279635898,
        14252634232868282405, 8219748254835277737, 7459165569491914711, 15855939513193752003,
        16788866461340278896, 7102224659693946577, 3024718005636976471, 13695468978618890430,
    ],
    [
        8214202050877825436, 2670727992739346204, 16259532062589659211, 11869922396257088411,
        3179482916972760137, 13525476046633427808, 3217337278042947412, 14494689598654046340,
        15837379330312175383, 8029037639801151344, 2153456285263517937, 8301106462311849241,
    ],
    [
        13294194396455217955, 17394768489610594315, 12847609130464867455, 14015739446356528640,
        5879251655839607853, 9747000124977436185, 8950393546890284269, 10765765936405694368,
        14695323910334139959, 16366254691123000864, 15292774414889043182, 10910394433429313384,
    ],
    [
        17253424460214596184, 3442854447664030446, 3005570425335613727, 10859158614900201063,
        9763230642109343539, 6647722546511515039, 909012944955815706, 18101204076790399111,
        11588128829349125809, 15863878496612806566, 5201119062417750399, 176665553780565743,
    ],
];

// Internal round constants (22 rounds)
const INTERNAL_CONSTANTS: [u64; ROUNDS_P] = [
    11921381764981422944, 10318423381711320787, 8291411502347000766, 229948027109387563,
    9152521390190983261, 7129306032690285515, 15395989607365232011, 8641397269074305925,
    17256848792241043600, 6046475228902245682, 12041608676381094092, 12785542378683951657,
    14546032085337914034, 3304199118235116851, 16499627707072547655, 10386478025625759321,
    13475579315436919170, 16042710511297532028, 1411266850385657080, 9024840976168649958,
    14047056970978379368, 838728605080212101,
];

// Diagonal matrix for internal linear layer (from Plonky3)
const MATRIX_DIAG_12: [u64; WIDTH] = [
    0xc3b6c08e23ba9300, 0xd84b5de94a324fb6, 0x0d0c371c5b35b84f, 0x7964f570e7188037,
    0x5daf18bbd996604b, 0x6743bc47b9595257, 0x5528b9362c59bb70, 0xac45e25b7127b68b,
    0xa2077d7dfbb606b5, 0xf3faac6faee378ae, 0x0c6388b51545e883, 0xd27dbb6944917b60,
];

/// Poseidon2 state
type State = [GoldilocksField; WIDTH];

/// S-box: x^7
#[inline]
fn sbox(x: GoldilocksField) -> GoldilocksField {
    x.pow7()
}

/// External linear layer (MDS matrix)
fn external_linear_layer(state: &mut State) {
    // Process in groups of 4
    for i in 0..3 {
        let idx = i * 4;
        let t0 = state[idx] + state[idx + 1];
        let t1 = state[idx + 2] + state[idx + 3];
        let t2 = t0 + t1;
        let t3 = t2 + state[idx + 1];
        let t4 = t2 + state[idx + 3];
        let t5 = state[idx].double();
        let t6 = state[idx + 2].double();

        state[idx] = t3 + t0;
        state[idx + 1] = t6 + t3;
        state[idx + 2] = t1 + t4;
        state[idx + 3] = t5 + t4;
    }

    // Mix across groups
    let mut sums = [GoldilocksField::ZERO; 4];
    for k in 0..4 {
        for j in (0..WIDTH).step_by(4) {
            sums[k] = sums[k] + state[j + k];
        }
    }

    for i in 0..WIDTH {
        state[i] = state[i] + sums[i % 4];
    }
}

/// Internal linear layer
fn internal_linear_layer(state: &mut State) {
    let mut sum = state[0];
    for i in 1..WIDTH {
        sum = sum + state[i];
    }

    for i in 0..WIDTH {
        let diag = GoldilocksField::new(MATRIX_DIAG_12[i]);
        state[i] = state[i] * diag + sum;
    }
}

/// Add round constants
fn add_rc(state: &mut State, round: usize) {
    for i in 0..WIDTH {
        state[i] = state[i] + GoldilocksField::new(EXTERNAL_CONSTANTS[round][i]);
    }
}

/// Add internal round constant (only to first element)
fn add_rci(state: &mut State, round: usize) {
    state[0] = state[0] + GoldilocksField::new(INTERNAL_CONSTANTS[round]);
}

/// Full rounds
fn full_rounds(state: &mut State, start: usize) {
    for r in start..(start + ROUNDS_F_HALF) {
        add_rc(state, r);
        for i in 0..WIDTH {
            state[i] = sbox(state[i]);
        }
        external_linear_layer(state);
    }
}

/// Partial rounds
fn partial_rounds(state: &mut State) {
    for r in 0..ROUNDS_P {
        add_rci(state, r);
        state[0] = sbox(state[0]);
        internal_linear_layer(state);
    }
}

/// Poseidon2 permutation
pub fn permute(state: &mut State) {
    external_linear_layer(state);
    full_rounds(state, 0);
    partial_rounds(state);
    full_rounds(state, ROUNDS_F_HALF);
}

/// Hash N elements to M elements (no padding)
pub fn hash_n_to_m_no_pad(input: &[GoldilocksField], num_outputs: usize) -> Vec<GoldilocksField> {
    let mut perm = [GoldilocksField::ZERO; WIDTH];

    // Absorb input in chunks of RATE
    for chunk in input.chunks(RATE) {
        for (i, &elem) in chunk.iter().enumerate() {
            perm[i] = elem;
        }
        permute(&mut perm);
    }

    // Squeeze output
    let mut outputs = Vec::with_capacity(num_outputs);
    loop {
        for i in 0..RATE {
            outputs.push(perm[i]);
            if outputs.len() == num_outputs {
                return outputs;
            }
        }
        permute(&mut perm);
    }
}

/// Hash to quintic extension (5 elements)
pub fn hash_to_quintic_extension(input: &[GoldilocksField]) -> GFp5 {
    let result = hash_n_to_m_no_pad(input, 5);
    GFp5([result[0], result[1], result[2], result[3], result[4]])
}

/// Hash N elements to 4-element hash (standard hash output)
pub fn hash_no_pad(input: &[GoldilocksField]) -> [GoldilocksField; 4] {
    let result = hash_n_to_m_no_pad(input, 4);
    [result[0], result[1], result[2], result[3]]
}

/// Hash two 4-element hashes to one
pub fn hash_two_to_one(
    h1: [GoldilocksField; 4],
    h2: [GoldilocksField; 4],
) -> [GoldilocksField; 4] {
    let input: Vec<GoldilocksField> = h1.into_iter().chain(h2).collect();
    hash_no_pad(&input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_permute() {
        // Test vector from Go poseidon2_test.go TestPermute
        let mut state: State = [
            GoldilocksField::new(5417613058500526590),
            GoldilocksField::new(2481548824842427254),
            GoldilocksField::new(6473243198879784792),
            GoldilocksField::new(1720313757066167274),
            GoldilocksField::new(2806320291675974571),
            GoldilocksField::new(7407976414706455446),
            GoldilocksField::new(1105257841424046885),
            GoldilocksField::new(7613435757403328049),
            GoldilocksField::new(3376066686066811538),
            GoldilocksField::new(5888575799323675710),
            GoldilocksField::new(6689309723188675948),
            GoldilocksField::new(2468250420241012720),
        ];

        permute(&mut state);

        let expected: [u64; WIDTH] = [
            5364184781011389007,
            15309475861242939136,
            5983386513087443499,
            886942118604446276,
            14903657885227062600,
            7742650891575941298,
            1962182278500985790,
            10213480816595178755,
            3510799061817443836,
            4610029967627506430,
            7566382334276534836,
            2288460879362380348,
        ];

        for i in 0..WIDTH {
            assert_eq!(
                state[i].to_u64(), expected[i],
                "Permute mismatch at index {}: got {}, expected {}",
                i, state[i].to_u64(), expected[i]
            );
        }
    }

    #[test]
    fn test_hash_to_quintic_extension() {
        // Test vector from Go poseidon2_test.go TestHashToQuinticExtension
        let input: Vec<GoldilocksField> = vec![
            GoldilocksField::new(3451004116618606032),
            GoldilocksField::new(11263134342958518251),
            GoldilocksField::new(10957204882857370932),
            GoldilocksField::new(5369763041201481933),
            GoldilocksField::new(7695734348563036858),
            GoldilocksField::new(1393419330378128434),
            GoldilocksField::new(7387917082382606332),
        ];

        let result = hash_to_quintic_extension(&input);

        let expected: [u64; 5] = [
            17992684813643984528,
            5243896189906434327,
            7705560276311184368,
            2785244775876017560,
            14449776097783372302,
        ];

        for i in 0..5 {
            assert_eq!(
                result.0[i].to_u64(), expected[i],
                "HashToQuinticExtension mismatch at index {}: got {}, expected {}",
                i, result.0[i].to_u64(), expected[i]
            );
        }
    }
}
