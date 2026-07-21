//! Compressed Bloom filters for per-SSTable key membership, so a point lookup
//! can skip tables that definitely lack the key instead of probing every table.
//!
//! Design follows Mitzenmacher, "Compressed Bloom Filters" (Harvard TR-02-05):
//! because the filter is *stored on disk*, we minimize its compressed size, not
//! its in-memory bit count. The standard optimum (k = (m/n)ln2, half the bits
//! set) maximizes entropy and so compresses worst. Using fewer hash functions
//! with a larger m yields a sparse array that stores much smaller at the same
//! false-positive rate. `chooseParams` picks (m, k) minimizing the estimated
//! compressed size m·H(p) subject to a memory cap; sparse arrays are then
//! stored as delta-varint set-bit positions (a raw-bitmap fallback guarantees
//! we never store more than the bitmap itself).

const std = @import("std");
const assert = std.debug.assert;

pub const target_false_positive_default: f64 = 0.01;
/// Cap on in-memory bits per element. The compressed optimum wants very small k
/// (huge m); this bounds the decompressed filter a reader holds resident while
/// still leaving room for the sparse (k≈2) regime where compression pays off.
pub const bits_per_element_max: u64 = 24;
const hashes_max: u8 = 8;

pub const Params = struct {
    /// Number of bits in the filter (a multiple of 8, at least 64).
    bits: u64,
    /// Number of hash probes.
    hashes: u8,

    pub fn byte_len(self: Params) usize {
        return @intCast(self.bits / 8);
    }
};

/// Choose (bits, hashes) minimizing estimated compressed size for `n` elements
/// at `target_fpr`, subject to the per-element memory cap. Fewer hashes give a
/// sparser, better-compressing filter — the paper's central result.
pub fn chooseParams(n: u64, target_fpr: f64) Params {
    assert(target_fpr > 0 and target_fpr < 1);
    if (n == 0) return .{ .bits = 64, .hashes = 1 };

    const n_float: f64 = @floatFromInt(n);
    const bits_cap: f64 = @floatFromInt(bits_per_element_max * n);

    var best_bits: u64 = 0;
    var best_hashes: u8 = 0;
    var best_cost: f64 = std.math.inf(f64);

    var k: u8 = 1;
    while (k <= hashes_max) : (k += 1) {
        const kf: f64 = @floatFromInt(k);
        // Fraction of zero bits needed so (1 - zero)^k == target_fpr.
        const zero = 1.0 - std.math.pow(f64, target_fpr, 1.0 / kf);
        if (!(zero > 0.0 and zero < 1.0)) continue;
        // m = -k*n / ln(zero), from zero = e^{-k n / m}.
        const m_float = -kf * n_float / @log(zero);
        if (!(m_float > 0)) continue;
        if (m_float > bits_cap) continue; // exceeds memory cap
        const ones = 1.0 - zero;
        const cost = m_float * binaryEntropy(ones); // estimated compressed bits
        if (cost < best_cost) {
            best_cost = cost;
            best_bits = round_up_bits(m_float);
            best_hashes = k;
        }
    }

    if (best_hashes == 0) {
        // Cap binds for every k (very small target FPR). Fall back to the
        // densest filter that fits the cap with the standard optimum shape.
        const m: u64 = bits_per_element_max * n;
        const k_std: f64 = @log(2.0) * @as(f64, @floatFromInt(bits_per_element_max));
        var hashes: u8 = @intFromFloat(@round(k_std));
        if (hashes == 0) hashes = 1;
        if (hashes > hashes_max) hashes = hashes_max;
        return .{ .bits = round_up_bits(@floatFromInt(m)), .hashes = hashes };
    }
    return .{ .bits = best_bits, .hashes = best_hashes };
}

fn round_up_bits(m_float: f64) u64 {
    var m: u64 = @intFromFloat(@ceil(m_float));
    if (m < 64) m = 64;
    // Byte-align.
    return (m + 7) / 8 * 8;
}

fn binaryEntropy(p: f64) f64 {
    if (p <= 0.0 or p >= 1.0) return 0.0;
    return -p * std.math.log2(p) - (1.0 - p) * std.math.log2(1.0 - p);
}

fn probe(key: []const u8, i: u8, bits: u64) u64 {
    // Kirsch-Mitzenmacher double hashing: two base hashes generate k indices.
    const h1 = std.hash.Wyhash.hash(0x9E3779B97F4A7C15, key);
    const h2 = std.hash.Wyhash.hash(0xD1B54A32D192ED03, key);
    return (h1 +% @as(u64, i) *% h2) % bits;
}

fn set_bit(bytes: []u8, index: u64) void {
    bytes[@intCast(index / 8)] |= @as(u8, 1) << @intCast(index % 8);
}

fn get_bit(bytes: []const u8, index: u64) bool {
    return (bytes[@intCast(index / 8)] & (@as(u8, 1) << @intCast(index % 8))) != 0;
}

/// Set the bits for `key` in a caller-owned, zero-initialized `bytes` buffer of
/// `params.byte_len()`. Used by the streaming SSTable writer.
pub fn insert(bytes: []u8, params: Params, key: []const u8) void {
    var i: u8 = 0;
    while (i < params.hashes) : (i += 1) set_bit(bytes, probe(key, i, params.bits));
}

/// True if `key` may be present. False means definitely absent.
pub fn mayContain(bytes: []const u8, params: Params, key: []const u8) bool {
    var i: u8 = 0;
    while (i < params.hashes) : (i += 1) {
        if (!get_bit(bytes, probe(key, i, params.bits))) return false;
    }
    return true;
}

fn popcount(bytes: []const u8) u64 {
    var total: u64 = 0;
    for (bytes) |byte| total += @popCount(byte);
    return total;
}

/// Number of set bits in the filter (hardware popcount, O(bytes)). Useful as a
/// saturation diagnostic: a filter far denser than its target fill ratio has
/// exceeded the element count it was sized for and its false-positive rate has
/// degraded.
pub fn count(bytes: []const u8) u64 {
    return popcount(bytes);
}

/// Fraction of set bits in the filter, in [0, 1].
pub fn fill_ratio(bytes: []const u8, params: Params) f64 {
    if (params.bits == 0) return 0;
    return @as(f64, @floatFromInt(popcount(bytes))) / @as(f64, @floatFromInt(params.bits));
}

// Serialized layout:
//   [1B version][1B hashes][8B bits][8B set_count][1B format][1B rice_param][body]
// format 0 = raw bitmap (byte_len bytes); 1 = Rice-coded gaps between set bits.
// Rice coding of the sorted set positions approaches the filter's entropy for a
// sparse array — this is where the compressed-Bloom saving is realized. Raw is a
// fallback so we never store more than the bitmap. `set_count` lets decode
// verify the reconstruction, turning any codec bug or corruption into an error
// instead of a silent false negative.
const serial_version: u8 = 1;
const header_len: usize = 1 + 1 + 8 + 8 + 1 + 1;
const format_raw: u8 = 0;
const format_rice: u8 = 1;

pub fn serialized_size_max(params: Params) usize {
    // Raw is the ceiling; the Rice encoding is only chosen when it is smaller.
    return header_len + params.byte_len();
}

/// Bit count encoded in a serialized filter header, so a caller can size the
/// decompression buffer before calling `deserialize`.
pub fn bits_of(data: []const u8) !u64 {
    if (data.len < header_len) return error.InvalidBloomFilter;
    if (data[0] != serial_version) return error.UnsupportedBloomVersion;
    const bits = std.mem.readInt(u64, data[2..10], .little);
    if (bits == 0 or bits % 8 != 0) return error.InvalidBloomFilter;
    return bits;
}

fn rice_parameter(bits: u64, set_count: u64) u8 {
    if (set_count == 0) return 0;
    const mean_gap = bits / set_count;
    if (mean_gap < 2) return 0;
    return @intCast(std.math.log2_int(u64, mean_gap));
}

/// Encode a built filter into `out` (>= serialized_size_max). Returns the used
/// slice.
pub fn serialize(bytes: []const u8, params: Params, out: []u8) ![]const u8 {
    assert(out.len >= serialized_size_max(params));
    const ones = popcount(bytes);
    const rice_b = rice_parameter(params.bits, ones);

    out[0] = serial_version;
    out[1] = params.hashes;
    std.mem.writeInt(u64, out[2..10], params.bits, .little);
    std.mem.writeInt(u64, out[10..18], ones, .little);
    out[19] = rice_b;

    // Rice-code the gaps between set bits into the body; fall back to raw if it
    // does not come out smaller (e.g. a dense filter).
    var writer = BitWriter.init(out[header_len..]);
    var previous: u64 = 0;
    var first = true;
    var index: u64 = 0;
    var overflow = false;
    while (index < params.bits) : (index += 1) {
        if (!get_bit(bytes, index)) continue;
        const gap = if (first) index else index - previous;
        first = false;
        previous = index;
        writer.put_rice(gap, rice_b) catch {
            overflow = true;
            break;
        };
    }
    const rice_len = writer.finish();
    if (!overflow and rice_len < params.byte_len()) {
        out[18] = format_rice;
        return out[0 .. header_len + rice_len];
    }
    out[18] = format_raw;
    @memcpy(out[header_len..][0..params.byte_len()], bytes);
    return out[0 .. header_len + params.byte_len()];
}

pub const Decoded = struct {
    params: Params,
    /// Filled into the caller's buffer.
    bytes: []u8,
};

/// Reconstruct a filter from `data` into `out_bytes`. `out_bytes` must be at
/// least `byte_len` for the encoded `bits`; verifies the popcount matches.
pub fn deserialize(data: []const u8, out_bytes: []u8) !Decoded {
    if (data.len < header_len) return error.InvalidBloomFilter;
    if (data[0] != serial_version) return error.UnsupportedBloomVersion;
    const hashes = data[1];
    if (hashes == 0 or hashes > hashes_max) return error.InvalidBloomFilter;
    const bits = std.mem.readInt(u64, data[2..10], .little);
    if (bits == 0 or bits % 8 != 0) return error.InvalidBloomFilter;
    const set_count = std.mem.readInt(u64, data[10..18], .little);
    const format = data[18];
    const rice_b = data[19];
    const params: Params = .{ .bits = bits, .hashes = hashes };
    const byte_len = params.byte_len();
    if (out_bytes.len < byte_len) return error.BloomBufferTooSmall;
    const bitmap = out_bytes[0..byte_len];
    @memset(bitmap, 0);

    const body = data[header_len..];
    switch (format) {
        format_raw => {
            if (body.len != byte_len) return error.InvalidBloomFilter;
            @memcpy(bitmap, body);
        },
        format_rice => {
            if (rice_b >= 64) return error.InvalidBloomFilter;
            var reader = BitReader.init(body);
            var index: u64 = 0;
            var previous: u64 = 0;
            var first = true;
            var decoded: u64 = 0;
            while (decoded < set_count) : (decoded += 1) {
                const gap = try reader.get_rice(rice_b);
                index = if (first) gap else previous + gap;
                first = false;
                if (index >= bits) return error.InvalidBloomFilter;
                if (get_bit(bitmap, index)) return error.InvalidBloomFilter; // non-monotone
                set_bit(bitmap, index);
                previous = index;
            }
        },
        else => return error.InvalidBloomFilter,
    }
    if (popcount(bitmap) != set_count) return error.BloomChecksumMismatch;
    return .{ .params = params, .bytes = bitmap };
}

const BitWriter = struct {
    out: []u8,
    bit_pos: usize = 0,

    fn init(out: []u8) BitWriter {
        @memset(out, 0);
        return .{ .out = out };
    }

    fn put_bit(self: *BitWriter, bit: u1) !void {
        if (self.bit_pos / 8 >= self.out.len) return error.BloomBufferTooSmall;
        if (bit == 1) self.out[self.bit_pos / 8] |= @as(u8, 1) << @intCast(self.bit_pos % 8);
        self.bit_pos += 1;
    }

    fn put_rice(self: *BitWriter, value: u64, b: u8) !void {
        const quotient = value >> @intCast(b);
        var q = quotient;
        while (q > 0) : (q -= 1) try self.put_bit(1);
        try self.put_bit(0);
        var i: u8 = 0;
        while (i < b) : (i += 1) try self.put_bit(@intCast((value >> @intCast(i)) & 1));
    }

    fn finish(self: *BitWriter) usize {
        return (self.bit_pos + 7) / 8;
    }
};

const BitReader = struct {
    data: []const u8,
    bit_pos: usize = 0,

    fn init(data: []const u8) BitReader {
        return .{ .data = data };
    }

    fn get_bit(self: *BitReader) !u1 {
        if (self.bit_pos / 8 >= self.data.len) return error.InvalidBloomFilter;
        const bit: u1 = @intCast((self.data[self.bit_pos / 8] >> @intCast(self.bit_pos % 8)) & 1);
        self.bit_pos += 1;
        return bit;
    }

    fn get_rice(self: *BitReader, b: u8) !u64 {
        var quotient: u64 = 0;
        while ((try self.get_bit()) == 1) {
            quotient += 1;
            if (quotient > self.data.len * 8) return error.InvalidBloomFilter;
        }
        var remainder: u64 = 0;
        var i: u8 = 0;
        while (i < b) : (i += 1) remainder |= @as(u64, try self.get_bit()) << @intCast(i);
        return (quotient << @intCast(b)) | remainder;
    }
};

test "no false negatives for inserted keys" {
    const allocator = std.testing.allocator;
    const n = 500;
    const params = chooseParams(n, 0.01);
    const bytes = try allocator.alloc(u8, params.byte_len());
    defer allocator.free(bytes);
    @memset(bytes, 0);

    var key_buffer: [32]u8 = undefined;
    for (0..n) |i| {
        const key = try std.fmt.bufPrint(&key_buffer, "key-{d}", .{i});
        insert(bytes, params, key);
    }
    for (0..n) |i| {
        const key = try std.fmt.bufPrint(&key_buffer, "key-{d}", .{i});
        try std.testing.expect(mayContain(bytes, params, key));
    }
}

test "false positive rate is near target" {
    const allocator = std.testing.allocator;
    const n = 2000;
    const target = 0.01;
    const params = chooseParams(n, target);
    const bytes = try allocator.alloc(u8, params.byte_len());
    defer allocator.free(bytes);
    @memset(bytes, 0);

    var key_buffer: [32]u8 = undefined;
    for (0..n) |i| {
        const key = try std.fmt.bufPrint(&key_buffer, "present-{d}", .{i});
        insert(bytes, params, key);
    }
    var false_positives: usize = 0;
    const trials = 10000;
    for (0..trials) |i| {
        const key = try std.fmt.bufPrint(&key_buffer, "absent-{d}", .{i});
        if (mayContain(bytes, params, key)) false_positives += 1;
    }
    const rate = @as(f64, @floatFromInt(false_positives)) / @as(f64, @floatFromInt(trials));
    // Generous bound: the estimator targets 1%, allow up to 4%.
    try std.testing.expect(rate < 0.04);
}

test "chooseParams favors few hashes and a sparse filter" {
    const params = chooseParams(1000, 0.01);
    try std.testing.expect(params.hashes >= 1 and params.hashes <= 3);
    try std.testing.expect(params.bits % 8 == 0 and params.bits >= 64);
    try std.testing.expect(params.bits <= bits_per_element_max * 1000);
}

test "serialize/deserialize round-trip preserves membership (sparse)" {
    const allocator = std.testing.allocator;
    const n = 800;
    const params = chooseParams(n, 0.01);
    const bytes = try allocator.alloc(u8, params.byte_len());
    defer allocator.free(bytes);
    @memset(bytes, 0);
    var key_buffer: [32]u8 = undefined;
    for (0..n) |i| {
        const key = try std.fmt.bufPrint(&key_buffer, "k{d}", .{i});
        insert(bytes, params, key);
    }

    const out = try allocator.alloc(u8, serialized_size_max(params));
    defer allocator.free(out);
    const encoded = try serialize(bytes, params, out);
    // Sparse encoding should beat the raw bitmap for a 1% filter.
    try std.testing.expect(encoded.len < header_len + params.byte_len());

    const restored_bytes = try allocator.alloc(u8, params.byte_len());
    defer allocator.free(restored_bytes);
    const decoded = try deserialize(encoded, restored_bytes);
    try std.testing.expectEqual(params.bits, decoded.params.bits);
    try std.testing.expectEqual(params.hashes, decoded.params.hashes);
    try std.testing.expectEqualSlices(u8, bytes, decoded.bytes);
    for (0..n) |i| {
        const key = try std.fmt.bufPrint(&key_buffer, "k{d}", .{i});
        try std.testing.expect(mayContain(decoded.bytes, decoded.params, key));
    }
}

test "empty and all-set filters round-trip" {
    const allocator = std.testing.allocator;
    const params: Params = .{ .bits = 128, .hashes = 2 };
    const bytes = try allocator.alloc(u8, params.byte_len());
    defer allocator.free(bytes);
    const out = try allocator.alloc(u8, serialized_size_max(params));
    defer allocator.free(out);
    const restored = try allocator.alloc(u8, params.byte_len());
    defer allocator.free(restored);

    // All zero.
    @memset(bytes, 0);
    var decoded = try deserialize(try serialize(bytes, params, out), restored);
    try std.testing.expectEqualSlices(u8, bytes, decoded.bytes);

    // All ones (raw fallback should win here).
    @memset(bytes, 0xff);
    decoded = try deserialize(try serialize(bytes, params, out), restored);
    try std.testing.expectEqualSlices(u8, bytes, decoded.bytes);
}

test "deserialize detects a corrupted set_count" {
    const allocator = std.testing.allocator;
    const params: Params = .{ .bits = 256, .hashes = 2 };
    const bytes = try allocator.alloc(u8, params.byte_len());
    defer allocator.free(bytes);
    @memset(bytes, 0);
    insert(bytes, params, "alpha");
    insert(bytes, params, "beta");
    const out = try allocator.alloc(u8, serialized_size_max(params));
    defer allocator.free(out);
    const encoded = try serialize(bytes, params, out);
    const mutable = try allocator.dupe(u8, encoded);
    defer allocator.free(mutable);
    const restored = try allocator.alloc(u8, params.byte_len());
    defer allocator.free(restored);

    // Bumping the stored set_count makes reconstruction read past the body
    // (Rice) or mismatch the popcount (raw) — either way an error, never a
    // silent wrong filter.
    mutable[10] +%= 1;
    try std.testing.expect(std.meta.isError(deserialize(mutable, restored)));
}

test "raw format popcount mismatch is caught" {
    const allocator = std.testing.allocator;
    // A dense filter serializes raw; flipping a body bit changes the popcount.
    const params: Params = .{ .bits = 64, .hashes = 2 };
    const bytes = try allocator.alloc(u8, params.byte_len());
    defer allocator.free(bytes);
    @memset(bytes, 0xff);
    const out = try allocator.alloc(u8, serialized_size_max(params));
    defer allocator.free(out);
    const encoded = try serialize(bytes, params, out);
    try std.testing.expectEqual(format_raw, encoded[18]);
    const mutable = try allocator.dupe(u8, encoded);
    defer allocator.free(mutable);
    mutable[header_len] = 0x00; // clear a byte that was all ones
    const restored = try allocator.alloc(u8, params.byte_len());
    defer allocator.free(restored);
    try std.testing.expectError(error.BloomChecksumMismatch, deserialize(mutable, restored));
}

test "n=0 params are valid and match nothing meaningfully" {
    const params = chooseParams(0, 0.01);
    try std.testing.expect(params.bits == 64 and params.hashes == 1);
}

test "count and fill_ratio track set bits" {
    const allocator = std.testing.allocator;
    const n = 1000;
    const params = chooseParams(n, 0.01);
    const bytes = try allocator.alloc(u8, params.byte_len());
    defer allocator.free(bytes);
    @memset(bytes, 0);
    try std.testing.expectEqual(@as(u64, 0), count(bytes));
    try std.testing.expectEqual(@as(f64, 0), fill_ratio(bytes, params));

    var key_buffer: [32]u8 = undefined;
    for (0..n) |i| {
        const key = try std.fmt.bufPrint(&key_buffer, "k{d}", .{i});
        insert(bytes, params, key);
    }
    const set = count(bytes);
    try std.testing.expect(set > 0 and set <= params.bits);
    // A well-sized 1% filter is sparse — far from saturated.
    const ratio = fill_ratio(bytes, params);
    try std.testing.expect(ratio > 0.0 and ratio < 0.5);
}
