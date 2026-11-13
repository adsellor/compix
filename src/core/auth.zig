//! Bounded pre-shared-key service authentication and session key derivation.

const std = @import("std");

const Hmac = std.crypto.auth.hmac.sha2.HmacSha256;

pub const nonce_length: u8 = 32;
pub const proof_length: u8 = Hmac.mac_length;
pub const secret_length_min: u8 = 32;
pub const secret_length_max: u8 = 128;
pub const credentials_max: u16 = 256;
pub const replay_capacity_default: u32 = 65_536;
pub const replay_window_seconds: i64 = 5 * 60;

pub const ReplayEntry = struct {
    fingerprint: [16]u8 = @splat(0),
    expires_at: i64 = 0,
    occupied: bool = false,
};

const client_label = "compix/client/v5";
const server_label = "compix/server/v5";
const session_label = "compix/session/v5";
const client_key_label = "compix/key/client-to-server/v5";
const server_key_label = "compix/key/server-to-client/v5";

pub const Permission = enum {
    append,
    read_stream,
    read_log,
    recover,
    observe,
    replicate,
};

pub const Permissions = bitpack struct(u8) {
    append: bool = false,
    read_stream: bool = false,
    read_log: bool = false,
    recover: bool = false,
    observe: bool = false,
    replicate: bool = false,
    reserved: u2 = 0,

    pub fn from_list(values: []const Permission) !Permissions {
        var result: Permissions = .{};
        for (values) |permission| {
            switch (permission) {
                .append => {
                    if (result.append) return error.DuplicatePermission;
                    result.append = true;
                },
                .read_stream => {
                    if (result.read_stream) return error.DuplicatePermission;
                    result.read_stream = true;
                },
                .read_log => {
                    if (result.read_log) return error.DuplicatePermission;
                    result.read_log = true;
                },
                .recover => {
                    if (result.recover) return error.DuplicatePermission;
                    result.recover = true;
                },
                .observe => {
                    if (result.observe) return error.DuplicatePermission;
                    result.observe = true;
                },
                .replicate => {
                    if (result.replicate) return error.DuplicatePermission;
                    result.replicate = true;
                },
            }
        }
        return result;
    }
};

pub const Credential = struct {
    service_name: []const u8,
    secret: []const u8,
    permissions: Permissions,
};

pub const SessionKeys = struct {
    client_to_server: [32]u8,
    server_to_client: [32]u8,
};

pub fn validate_secret(secret: []const u8) !void {
    if (secret.len < secret_length_min) return error.SecretTooShort;
    if (secret.len > secret_length_max) return error.SecretTooLong;
}

pub fn find(credentials: []const Credential, service_name: []const u8) ?*const Credential {
    for (credentials) |*credential| {
        if (std.mem.eql(u8, credential.service_name, service_name)) return credential;
    }
    return null;
}

/// Claims an authenticated client hello for the configured replay window.
/// The table must have a power-of-two length. It never evicts a live entry:
/// saturation rejects new handshakes instead of allowing an old proof to replay.
pub fn claim_client_hello(
    entries: []ReplayEntry,
    service_name: []const u8,
    nonce: [nonce_length]u8,
    now_seconds: i64,
) !void {
    if (entries.len == 0 or !std.math.isPowerOfTwo(entries.len))
        return error.InvalidReplayCapacity;
    const fingerprint = replay_fingerprint(service_name, nonce);
    const start: usize = @intCast(std.mem.readInt(u64, fingerprint[0..8], .little) &
        @as(u64, @intCast(entries.len - 1)));
    var available: ?usize = null;
    for (0..entries.len) |probe| {
        const index = (start + probe) & (entries.len - 1);
        const entry = &entries[index];
        if (!entry.occupied) {
            available = available orelse index;
            break;
        }
        if (std.mem.eql(u8, &entry.fingerprint, &fingerprint)) {
            if (entry.expires_at > now_seconds) return error.ReplayedHandshake;
            available = index;
            break;
        }
        if (entry.expires_at <= now_seconds and available == null) available = index;
    }
    const index = available orelse return error.ReplayCacheFull;
    entries[index] = .{
        .fingerprint = fingerprint,
        .expires_at = std.math.add(i64, now_seconds, replay_window_seconds) catch
            std.math.maxInt(i64),
        .occupied = true,
    };
}

fn replay_fingerprint(service_name: []const u8, nonce: [nonce_length]u8) [16]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    var length: [2]u8 = undefined;
    std.mem.writeInt(u16, &length, @intCast(service_name.len), .big);
    hash.update(&length);
    hash.update(service_name);
    hash.update(&nonce);
    var digest: [std.crypto.hash.Blake3.digest_length]u8 = undefined;
    hash.final(&digest);
    return digest[0..16].*;
}

pub fn make_client_proof(
    secret: []const u8,
    service_name: []const u8,
    instance_id: []const u8,
    client_nonce: [nonce_length]u8,
) [proof_length]u8 {
    var mac = Hmac.init(secret);
    update_field(&mac, client_label);
    update_field(&mac, service_name);
    update_field(&mac, instance_id);
    mac.update(&client_nonce);
    var result: [proof_length]u8 = undefined;
    mac.final(&result);
    return result;
}

pub fn verify_client_proof(
    secret: []const u8,
    service_name: []const u8,
    instance_id: []const u8,
    client_nonce: [nonce_length]u8,
    proof: [proof_length]u8,
) bool {
    const expected = make_client_proof(secret, service_name, instance_id, client_nonce);
    return std.crypto.timing_safe.eql([proof_length]u8, expected, proof);
}

pub fn make_server_proof(
    secret: []const u8,
    client_nonce: [nonce_length]u8,
    server_nonce: [nonce_length]u8,
    server_name: []const u8,
    server_instance: []const u8,
) [proof_length]u8 {
    var mac = Hmac.init(secret);
    update_field(&mac, server_label);
    mac.update(&client_nonce);
    mac.update(&server_nonce);
    update_field(&mac, server_name);
    update_field(&mac, server_instance);
    var result: [proof_length]u8 = undefined;
    mac.final(&result);
    return result;
}

pub fn verify_server_proof(
    secret: []const u8,
    client_nonce: [nonce_length]u8,
    server_nonce: [nonce_length]u8,
    server_name: []const u8,
    server_instance: []const u8,
    proof: [proof_length]u8,
) bool {
    const expected = make_server_proof(
        secret,
        client_nonce,
        server_nonce,
        server_name,
        server_instance,
    );
    return std.crypto.timing_safe.eql([proof_length]u8, expected, proof);
}

pub fn derive_session_keys(
    secret: []const u8,
    client_nonce: [nonce_length]u8,
    server_nonce: [nonce_length]u8,
    client_name: []const u8,
    client_instance: []const u8,
    server_name: []const u8,
    server_instance: []const u8,
) SessionKeys {
    var mac = Hmac.init(secret);
    update_field(&mac, session_label);
    mac.update(&client_nonce);
    mac.update(&server_nonce);
    update_field(&mac, client_name);
    update_field(&mac, client_instance);
    update_field(&mac, server_name);
    update_field(&mac, server_instance);
    var master: [Hmac.mac_length]u8 = undefined;
    mac.final(&master);
    defer std.crypto.secureZero(u8, &master);
    return .{
        .client_to_server = derive_key(&master, client_key_label),
        .server_to_client = derive_key(&master, server_key_label),
    };
}

fn derive_key(master: *const [Hmac.mac_length]u8, label: []const u8) [32]u8 {
    var result: [32]u8 = undefined;
    Hmac.create(&result, label, master);
    return result;
}

fn update_field(mac: *Hmac, bytes: []const u8) void {
    std.debug.assert(bytes.len <= std.math.maxInt(u16));
    var length: [2]u8 = undefined;
    std.mem.writeInt(u16, &length, @intCast(bytes.len), .big);
    mac.update(&length);
    mac.update(bytes);
}

test "proofs authenticate every identity and nonce field" {
    const secret = "0123456789abcdef0123456789abcdef";
    const client_nonce: [nonce_length]u8 = @splat(0x11);
    const server_nonce: [nonce_length]u8 = @splat(0x22);
    const client = make_client_proof(secret, "watch", "watch-1", client_nonce);
    try std.testing.expect(verify_client_proof(
        secret,
        "watch",
        "watch-1",
        client_nonce,
        client,
    ));
    try std.testing.expect(!verify_client_proof(
        secret,
        "watch",
        "watch-2",
        client_nonce,
        client,
    ));
    const server = make_server_proof(
        secret,
        client_nonce,
        server_nonce,
        "compix",
        "node-1",
    );
    try std.testing.expect(verify_server_proof(
        secret,
        client_nonce,
        server_nonce,
        "compix",
        "node-1",
        server,
    ));
    var tampered_nonce = server_nonce;
    tampered_nonce[0] ^= 1;
    try std.testing.expect(!verify_server_proof(
        secret,
        client_nonce,
        tampered_nonce,
        "compix",
        "node-1",
        server,
    ));
}

test "session keys are directional and transcript bound" {
    const secret = "0123456789abcdef0123456789abcdef";
    const client_nonce: [nonce_length]u8 = @splat(0x33);
    const server_nonce: [nonce_length]u8 = @splat(0x44);
    const first = derive_session_keys(
        secret,
        client_nonce,
        server_nonce,
        "source-api",
        "api-1",
        "compix",
        "node-1",
    );
    try std.testing.expect(!std.mem.eql(
        u8,
        &first.client_to_server,
        &first.server_to_client,
    ));
    const second = derive_session_keys(
        secret,
        client_nonce,
        server_nonce,
        "source-api",
        "api-2",
        "compix",
        "node-1",
    );
    try std.testing.expect(!std.mem.eql(
        u8,
        &first.client_to_server,
        &second.client_to_server,
    ));
}

test "permission lists reject ambiguity" {
    const permissions = try Permissions.from_list(&.{ .append, .read_log, .observe });
    try std.testing.expect(permissions.append);
    try std.testing.expect(permissions.read_log);
    try std.testing.expect(!permissions.read_stream);
    try std.testing.expect(permissions.observe);
    try std.testing.expectError(
        error.DuplicatePermission,
        Permissions.from_list(&.{ .append, .append }),
    );
}

test "replay cache rejects live proofs and permits them only after expiry" {
    var entries: [8]ReplayEntry = @splat(.{});
    const nonce: [nonce_length]u8 = @splat(0x77);
    try claim_client_hello(&entries, "watch", nonce, 100);
    try std.testing.expectError(
        error.ReplayedHandshake,
        claim_client_hello(&entries, "watch", nonce, 101),
    );
    try claim_client_hello(&entries, "watch", nonce, 100 + replay_window_seconds);
}

test "replay cache refuses saturation without evicting live proofs" {
    var entries: [2]ReplayEntry = @splat(.{});
    try claim_client_hello(&entries, "watch", @splat(1), 100);
    try claim_client_hello(&entries, "watch", @splat(2), 100);
    try std.testing.expectError(
        error.ReplayCacheFull,
        claim_client_hello(&entries, "watch", @splat(3), 100),
    );
    try std.testing.expectError(
        error.ReplayedHandshake,
        claim_client_hello(&entries, "watch", @splat(1), 101),
    );
}
