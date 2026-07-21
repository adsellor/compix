//! Event provenance: Ed25519 signatures over the immutable event tuple.
//!
//! An origin service signs each event it emits with its private key; any
//! consumer that has pulled the event verifies authorship against the origin's
//! public key (declared per dependency contract). Unlike the transport PSK,
//! which only proves "this peer authenticated", a signature proves "this event
//! was produced by its owner" regardless of which peer relayed it — the trust
//! primitive the mesh's ownership model needs.

const std = @import("std");
const Ed25519 = std.crypto.sign.Ed25519;

pub const seed_length = Ed25519.KeyPair.seed_length;
pub const public_key_length = Ed25519.PublicKey.encoded_length;
pub const signature_length = Ed25519.Signature.encoded_length;

pub const seed_hex_length = 2 * seed_length;
pub const public_key_hex_length = 2 * public_key_length;
pub const signature_hex_length = 2 * signature_length;

pub const zero_signature: [signature_length]u8 = @splat(0);

pub fn is_zero_signature(signature: [signature_length]u8) bool {
    return std.crypto.timing_safe.eql([signature_length]u8, signature, zero_signature);
}

/// Owner-side signing key derived from a configured 32-byte seed.
pub const SigningKey = struct {
    keypair: Ed25519.KeyPair,

    pub fn from_seed(seed: [seed_length]u8) !SigningKey {
        return .{ .keypair = try Ed25519.KeyPair.generateDeterministic(seed) };
    }

    pub fn from_seed_hex(hex: []const u8) !SigningKey {
        var seed: [seed_length]u8 = undefined;
        try parse_hex(&seed, hex);
        return from_seed(seed);
    }

    /// One-shot sign over a contiguous message. Producer-side (not on the hot
    /// ingest path), so a caller buffer is acceptable.
    pub fn sign(self: *const SigningKey, message: []const u8) ![signature_length]u8 {
        const signature = try self.keypair.sign(message, null);
        return signature.toBytes();
    }

    pub fn public_key(self: *const SigningKey) [public_key_length]u8 {
        return self.keypair.public_key.toBytes();
    }
};

/// Streaming verifier, so a large event payload never needs a contiguous
/// buffer on the ingest path. `init` fails on a structurally invalid signature
/// or public key; feed the message via `update`, then call `verify`.
pub const Verifier = struct {
    inner: Ed25519.Verifier,

    pub fn init(
        signature: [signature_length]u8,
        public_key: [public_key_length]u8,
    ) !Verifier {
        const key = try Ed25519.PublicKey.fromBytes(public_key);
        const sig = Ed25519.Signature.fromBytes(signature);
        return .{ .inner = try sig.verifier(key) };
    }

    pub fn update(self: *Verifier, bytes: []const u8) void {
        self.inner.update(bytes);
    }

    pub fn verify(self: *Verifier) bool {
        self.inner.verify() catch return false;
        return true;
    }
};

pub fn parse_public_key_hex(hex: []const u8) ![public_key_length]u8 {
    var bytes: [public_key_length]u8 = undefined;
    try parse_hex(&bytes, hex);
    // Reject a key the Ed25519 group rejects, so a malformed contract fails at
    // load rather than silently rejecting every event at ingest.
    _ = Ed25519.PublicKey.fromBytes(bytes) catch return error.InvalidPublicKey;
    return bytes;
}

pub fn verify(
    signature: [signature_length]u8,
    message: []const u8,
    public_key: [public_key_length]u8,
) bool {
    const key = Ed25519.PublicKey.fromBytes(public_key) catch return false;
    const sig = Ed25519.Signature.fromBytes(signature);
    sig.verify(message, key) catch return false;
    return true;
}

fn parse_hex(out: []u8, hex: []const u8) !void {
    if (hex.len != out.len * 2) return error.InvalidHexLength;
    _ = std.fmt.hexToBytes(out, hex) catch return error.InvalidHex;
}

test "sign and verify round-trip" {
    const seed: [seed_length]u8 = @splat(7);
    const key = try SigningKey.from_seed(seed);
    const message = "compix-event-v1|42|order-service|order.created|{}";
    const signature = try key.sign(message);
    try std.testing.expect(!is_zero_signature(signature));
    try std.testing.expect(verify(signature, message, key.public_key()));
}

test "verify rejects a tampered message" {
    const seed: [seed_length]u8 = @splat(3);
    const key = try SigningKey.from_seed(seed);
    const signature = try key.sign("original");
    try std.testing.expect(!verify(signature, "tampered", key.public_key()));
}

test "verify rejects a signature from a different key" {
    const key_a = try SigningKey.from_seed(@splat(1));
    const key_b = try SigningKey.from_seed(@splat(2));
    const signature = try key_a.sign("message");
    try std.testing.expect(!verify(signature, "message", key_b.public_key()));
}

test "hex parsing round-trips seed and public key" {
    const seed: [seed_length]u8 = @splat(9);
    const key = try SigningKey.from_seed(seed);
    const public_hex = std.fmt.bytesToHex(key.public_key(), .lower);
    const parsed = try parse_public_key_hex(&public_hex);
    try std.testing.expectEqualSlices(u8, &key.public_key(), &parsed);

    const seed_hex = std.fmt.bytesToHex(seed, .lower);
    const from_hex = try SigningKey.from_seed_hex(&seed_hex);
    try std.testing.expectEqualSlices(u8, &key.public_key(), &from_hex.public_key());
}

test "parse_public_key_hex rejects wrong length" {
    try std.testing.expectError(error.InvalidHexLength, parse_public_key_hex("abcd"));
}
