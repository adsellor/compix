//! Stopped-node, checksummed backup and verified restore.

const std = @import("std");
const config_mod = @import("config.zig");
const limits = @import("limits.zig");
const protocol = @import("protocol.zig");
const store_mod = @import("store.zig");
const time_mod = @import("time.zig");

const Allocator = std.mem.Allocator;
const Io = std.Io;
const IoDir = Io.Dir;
const IoFile = Io.File;
const LoadedConfig = config_mod.LoadedConfig;
const EventStore = store_mod.EventStore;
const checksum_size: u8 = 32;
const copy_buffer_size: u32 = 1024 * 1024;
const backup_manifest_name = "backup.manifest";
const backup_state_suffix = ".backup-state";
const backup_state_size_max: u16 = 256;

pub const Summary = struct {
    files: u16,
    bytes: u64,
    committed_log_position: u64,
};

pub const State = struct {
    completed: u64 = 0,
    last_success_unix_seconds: i64 = 0,
    last_committed_log_position: u64 = 0,
};

const FileRecord = struct {
    kind: Kind,
    level: u32,
    id: u64,
    size: u64,
    checksum: [checksum_size]u8,
    payload_name: []const u8,

    const Kind = enum { wal, manifest, sst };
};

pub fn create(
    allocator: Allocator,
    io: Io,
    config: *const LoadedConfig,
    store: *EventStore,
    destination: []const u8,
) !Summary {
    if (destination.len == 0 or destination.len > limits.wal_path_length_max)
        return error.InvalidBackupPath;
    const verification = try store.verify_integrity();
    const table_count = store.lsm.manifest.live.count;
    const file_count: u16 = @intCast(table_count + 2);
    try IoDir.cwd().createDir(io, destination, .default_dir);
    try sync_parent_directory(destination, io);

    const manifest_path = try std.fmt.allocPrint(
        allocator,
        "{s}/{s}",
        .{ destination, backup_manifest_name },
    );
    defer allocator.free(manifest_path);
    const backup_manifest = try IoDir.cwd().createFile(io, manifest_path, .{
        .exclusive = true,
        .read = true,
    });
    defer backup_manifest.close(io);
    var manifest_position: u64 = 0;
    var manifest_hash = std.crypto.hash.Blake3.init(.{});
    const header_buffer_size = 2 * limits.service_name_length_max +
        2 * limits.instance_id_length_max + 128;
    const header_buffer = try allocator.alloc(u8, header_buffer_size);
    defer allocator.free(header_buffer);
    const service_hex_size = 2 * config.identity.name.len;
    const instance_hex_size = 2 * config.identity.instance_id.len;
    const service_hex = header_buffer[0..service_hex_size];
    write_hex(service_hex, config.identity.name);
    const instance_hex = header_buffer[service_hex_size..][0..instance_hex_size];
    write_hex(instance_hex, config.identity.instance_id);
    const header = try std.fmt.bufPrint(
        header_buffer[service_hex_size + instance_hex_size ..],
        "HEADER 1 {s} {s} {d} {d}",
        .{
            service_hex,
            instance_hex,
            verification.committed_log_position,
            file_count,
        },
    );
    try append_manifest_line(
        backup_manifest,
        io,
        &manifest_position,
        &manifest_hash,
        header,
    );

    const copy_buffer = try allocator.alloc(u8, copy_buffer_size);
    defer allocator.free(copy_buffer);
    const base = try derive_base(allocator, config.wal_path);
    defer allocator.free(base);
    const source_manifest_path = try std.fmt.allocPrint(allocator, "{s}.manifest", .{base});
    defer allocator.free(source_manifest_path);
    var summary: Summary = .{
        .files = file_count,
        .bytes = 0,
        .committed_log_position = verification.committed_log_position,
    };
    var record_index: u16 = 0;
    try backup_file(
        allocator,
        io,
        backup_manifest,
        &manifest_position,
        &manifest_hash,
        destination,
        record_index,
        .wal,
        0,
        0,
        config.wal_path,
        copy_buffer,
        &summary.bytes,
    );
    record_index += 1;
    try backup_file(
        allocator,
        io,
        backup_manifest,
        &manifest_position,
        &manifest_hash,
        destination,
        record_index,
        .manifest,
        0,
        0,
        source_manifest_path,
        copy_buffer,
        &summary.bytes,
    );
    record_index += 1;
    for (store.lsm.manifest.live.items_const()) |table| {
        const source = try std.fmt.allocPrint(
            allocator,
            "{s}_L{d}_{d:0>8}.sst",
            .{ base, table.level, table.id },
        );
        defer allocator.free(source);
        try backup_file(
            allocator,
            io,
            backup_manifest,
            &manifest_position,
            &manifest_hash,
            destination,
            record_index,
            .sst,
            table.level,
            table.id,
            source,
            copy_buffer,
            &summary.bytes,
        );
        record_index += 1;
    }
    if (record_index != file_count) return error.BackupFileCountMismatch;
    const manifest_checksum = hash_final(manifest_hash);
    const manifest_checksum_hex = std.fmt.bytesToHex(manifest_checksum, .lower);
    var trailer_buffer: [16 + 2 * checksum_size]u8 = undefined;
    const trailer = try std.fmt.bufPrint(
        &trailer_buffer,
        "CHECKSUM {s}\n",
        .{&manifest_checksum_hex},
    );
    try backup_manifest.writePositionalAll(io, trailer, manifest_position);
    try backup_manifest.sync(io);
    try sync_directory(destination, io);
    try record_successful_backup(
        allocator,
        io,
        config.wal_path,
        verification.committed_log_position,
    );
    return summary;
}

pub fn read_state(allocator: Allocator, io: Io, wal_path: []const u8) !State {
    const path = try state_path(allocator, wal_path);
    defer allocator.free(path);
    const file = IoDir.cwd().openFile(io, path, .{ .mode = .read_only }) catch |err| switch (err) {
        error.FileNotFound => return .{},
        else => return err,
    };
    defer file.close(io);
    const size = (try file.stat(io)).size;
    if (size == 0 or size > backup_state_size_max) return error.InvalidBackupState;
    var bytes: [backup_state_size_max]u8 = undefined;
    const data = bytes[0..size];
    if (try file.readPositionalAll(io, data, 0) != data.len)
        return error.TruncatedBackupState;
    if (data[data.len - 1] != '\n') return error.TruncatedBackupState;
    const line = data[0 .. data.len - 1];
    const checksum_separator = std.mem.lastIndexOfScalar(u8, line, ' ') orelse
        return error.InvalidBackupState;
    const content = line[0..checksum_separator];
    const checksum_hex = line[checksum_separator + 1 ..];
    if (checksum_hex.len != 2 * checksum_size) return error.InvalidBackupState;
    var expected: [checksum_size]u8 = undefined;
    _ = std.fmt.hexToBytes(&expected, checksum_hex) catch
        return error.InvalidBackupState;
    const actual = checksum_bytes(content);
    if (!std.crypto.timing_safe.eql([checksum_size]u8, actual, expected))
        return error.BackupStateChecksumMismatch;
    var fields = std.mem.splitScalar(u8, content, ' ');
    if (!std.mem.eql(u8, fields.next() orelse return error.InvalidBackupState, "STATE"))
        return error.InvalidBackupState;
    if (!std.mem.eql(u8, fields.next() orelse return error.InvalidBackupState, "1"))
        return error.UnsupportedBackupStateVersion;
    const result: State = .{
        .completed = try std.fmt.parseInt(
            u64,
            fields.next() orelse return error.InvalidBackupState,
            10,
        ),
        .last_success_unix_seconds = try std.fmt.parseInt(
            i64,
            fields.next() orelse return error.InvalidBackupState,
            10,
        ),
        .last_committed_log_position = try std.fmt.parseInt(
            u64,
            fields.next() orelse return error.InvalidBackupState,
            10,
        ),
    };
    if (fields.next() != null or result.completed == 0 or
        result.last_success_unix_seconds <= 0)
        return error.InvalidBackupState;
    return result;
}

fn record_successful_backup(
    allocator: Allocator,
    io: Io,
    wal_path: []const u8,
    committed_log_position: u64,
) !void {
    const previous = try read_state(allocator, io, wal_path);
    const completed = std.math.add(u64, previous.completed, 1) catch
        return error.BackupCountOverflow;
    const timestamp = time_mod.Instant.now(io).timestamp.sec;
    if (timestamp <= 0) return error.InvalidSystemClock;
    var content_buffer: [128]u8 = undefined;
    const content = try std.fmt.bufPrint(
        &content_buffer,
        "STATE 1 {d} {d} {d}",
        .{ completed, timestamp, committed_log_position },
    );
    const checksum = checksum_bytes(content);
    const checksum_hex = std.fmt.bytesToHex(checksum, .lower);
    var record_buffer: [backup_state_size_max]u8 = undefined;
    const record = try std.fmt.bufPrint(
        &record_buffer,
        "{s} {s}\n",
        .{ content, &checksum_hex },
    );
    const path = try state_path(allocator, wal_path);
    defer allocator.free(path);
    var atomic = try IoDir.cwd().createFileAtomic(io, path, .{ .replace = true });
    errdefer atomic.deinit(io);
    try atomic.file.writePositionalAll(io, record, 0);
    try atomic.file.sync(io);
    try atomic.replace(io);
    try sync_parent_directory(path, io);
    atomic.deinit(io);
}

fn state_path(allocator: Allocator, wal_path: []const u8) ![]u8 {
    const base = try derive_base(allocator, wal_path);
    defer allocator.free(base);
    return std.fmt.allocPrint(allocator, "{s}{s}", .{ base, backup_state_suffix });
}

pub fn restore(
    allocator: Allocator,
    io: Io,
    config: *const LoadedConfig,
    source: []const u8,
) !Summary {
    if (source.len == 0 or source.len > limits.wal_path_length_max)
        return error.InvalidBackupPath;
    const manifest_path = try std.fmt.allocPrint(
        allocator,
        "{s}/{s}",
        .{ source, backup_manifest_name },
    );
    defer allocator.free(manifest_path);
    const manifest_file = try IoDir.cwd().openFile(io, manifest_path, .{ .mode = .read_only });
    defer manifest_file.close(io);
    const manifest_size = (try manifest_file.stat(io)).size;
    if (manifest_size == 0 or manifest_size > limits.manifest_file_size_max)
        return error.InvalidBackupManifest;
    const manifest = try allocator.alloc(u8, manifest_size);
    defer allocator.free(manifest);
    const manifest_read = try manifest_file.readPositionalAll(io, manifest, 0);
    if (manifest_read != manifest.len) return error.TruncatedBackupManifest;
    const parsed = try verify_manifest(config, manifest);

    const base = try derive_base(allocator, config.wal_path);
    defer allocator.free(base);
    const copy_buffer = try allocator.alloc(u8, copy_buffer_size);
    defer allocator.free(copy_buffer);

    // Validate every source payload and every target before publishing any file.
    var records = record_iterator(manifest);
    while (try records.next()) |record| {
        const payload_path = try std.fmt.allocPrint(
            allocator,
            "{s}/{s}",
            .{ source, record.payload_name },
        );
        defer allocator.free(payload_path);
        try verify_payload(io, payload_path, record.size, record.checksum, copy_buffer);
        const target_path = try target_path_for(allocator, config, base, record);
        defer allocator.free(target_path);
        try require_absent(io, target_path);
    }

    // SSTables first, WAL second, manifest last. A failed restore cannot publish
    // a manifest that references missing tables.
    inline for (.{ FileRecord.Kind.sst, .wal, .manifest }) |kind| {
        records = record_iterator(manifest);
        while (try records.next()) |record| {
            if (record.kind != kind) continue;
            const payload_path = try std.fmt.allocPrint(
                allocator,
                "{s}/{s}",
                .{ source, record.payload_name },
            );
            defer allocator.free(payload_path);
            const target_path = try target_path_for(allocator, config, base, record);
            defer allocator.free(target_path);
            const copied = try copy_file(
                io,
                payload_path,
                target_path,
                true,
                copy_buffer,
            );
            if (copied.size != record.size or
                !std.mem.eql(u8, &copied.checksum, &record.checksum))
                return error.BackupPayloadChanged;
            try sync_parent_directory(target_path, io);
        }
    }
    return .{
        .files = parsed.files,
        .bytes = parsed.bytes,
        .committed_log_position = parsed.committed_log_position,
    };
}

const ParsedManifest = struct {
    files: u16,
    bytes: u64,
    committed_log_position: u64,
};

fn verify_manifest(config: *const LoadedConfig, manifest: []const u8) !ParsedManifest {
    if (manifest[manifest.len - 1] != '\n') return error.TruncatedBackupManifest;
    var iterator = std.mem.splitScalar(u8, manifest, '\n');
    const header = iterator.next() orelse return error.InvalidBackupManifest;
    var header_fields = std.mem.splitScalar(u8, header, ' ');
    if (!std.mem.eql(u8, header_fields.next() orelse return error.InvalidBackupManifest, "HEADER"))
        return error.InvalidBackupManifest;
    if (!std.mem.eql(u8, header_fields.next() orelse return error.InvalidBackupManifest, "1"))
        return error.UnsupportedBackupVersion;
    const service_hex = header_fields.next() orelse return error.InvalidBackupManifest;
    const instance_hex = header_fields.next() orelse return error.InvalidBackupManifest;
    if (!hex_matches(service_hex, config.identity.name) or
        !hex_matches(instance_hex, config.identity.instance_id))
        return error.BackupIdentityMismatch;
    const committed_position = try std.fmt.parseInt(
        u64,
        header_fields.next() orelse return error.InvalidBackupManifest,
        10,
    );
    const expected_files = try std.fmt.parseInt(
        u16,
        header_fields.next() orelse return error.InvalidBackupManifest,
        10,
    );
    if (header_fields.next() != null or expected_files < 2 or
        expected_files > limits.sstables_max + 4)
        return error.InvalidBackupManifest;

    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update(header);
    hash.update("\n");
    var files: u16 = 0;
    var bytes: u64 = 0;
    var wal_files: u8 = 0;
    var manifest_files: u8 = 0;
    var trailer_seen = false;
    while (iterator.next()) |line| {
        if (line.len == 0) break;
        if (std.mem.startsWith(u8, line, "CHECKSUM ")) {
            if (trailer_seen) return error.InvalidBackupManifest;
            trailer_seen = true;
            var expected: [checksum_size]u8 = undefined;
            const encoded = line["CHECKSUM ".len..];
            if (encoded.len != 2 * checksum_size) return error.InvalidBackupManifest;
            _ = std.fmt.hexToBytes(&expected, encoded) catch
                return error.InvalidBackupManifest;
            const actual = hash_final(hash);
            if (!std.crypto.timing_safe.eql([checksum_size]u8, actual, expected))
                return error.BackupManifestChecksumMismatch;
            continue;
        }
        if (trailer_seen) return error.InvalidBackupManifest;
        const record = try parse_file_record(line);
        const payload_index = try std.fmt.parseInt(
            u16,
            record.payload_name["payload-".len..],
            10,
        );
        if (payload_index != files) return error.InvalidBackupManifest;
        switch (record.kind) {
            .wal => wal_files += 1,
            .manifest => manifest_files += 1,
            .sst => {},
        }
        bytes = std.math.add(u64, bytes, record.size) catch
            return error.InvalidBackupManifest;
        files += 1;
        hash.update(line);
        hash.update("\n");
    }
    if (!trailer_seen or files != expected_files) return error.BackupFileCountMismatch;
    if (wal_files != 1 or manifest_files != 1)
        return error.InvalidBackupManifest;
    try validate_unique_targets(manifest);
    return .{
        .files = files,
        .bytes = bytes,
        .committed_log_position = committed_position,
    };
}

fn validate_unique_targets(manifest: []const u8) !void {
    var outer = record_iterator(manifest);
    var outer_index: u16 = 0;
    while (try outer.next()) |record| : (outer_index += 1) {
        var inner = record_iterator(manifest);
        var inner_index: u16 = 0;
        while (try inner.next()) |other| : (inner_index += 1) {
            if (inner_index >= outer_index) break;
            if (record.kind != .sst or other.kind != .sst) continue;
            if (record.level == other.level and record.id == other.id)
                return error.InvalidBackupManifest;
        }
    }
}

const RecordIterator = struct {
    lines: std.mem.SplitIterator(u8, .scalar),
    header_skipped: bool = false,

    fn next(self: *RecordIterator) !?FileRecord {
        if (!self.header_skipped) {
            _ = self.lines.next() orelse return error.InvalidBackupManifest;
            self.header_skipped = true;
        }
        while (self.lines.next()) |line| {
            if (line.len == 0 or std.mem.startsWith(u8, line, "CHECKSUM ")) return null;
            return try parse_file_record(line);
        }
        return null;
    }
};

fn record_iterator(manifest: []const u8) RecordIterator {
    return .{ .lines = std.mem.splitScalar(u8, manifest, '\n') };
}

fn parse_file_record(line: []const u8) !FileRecord {
    var fields = std.mem.splitScalar(u8, line, ' ');
    if (!std.mem.eql(u8, fields.next() orelse return error.InvalidBackupManifest, "FILE"))
        return error.InvalidBackupManifest;
    const kind_text = fields.next() orelse return error.InvalidBackupManifest;
    const kind: FileRecord.Kind = if (std.mem.eql(u8, kind_text, "WAL"))
        .wal
    else if (std.mem.eql(u8, kind_text, "MANIFEST"))
        .manifest
    else if (std.mem.eql(u8, kind_text, "SST"))
        .sst
    else
        return error.InvalidBackupManifest;
    const level = try std.fmt.parseInt(
        u32,
        fields.next() orelse return error.InvalidBackupManifest,
        10,
    );
    const id = try std.fmt.parseInt(
        u64,
        fields.next() orelse return error.InvalidBackupManifest,
        10,
    );
    const size = try std.fmt.parseInt(
        u64,
        fields.next() orelse return error.InvalidBackupManifest,
        10,
    );
    const checksum_hex = fields.next() orelse return error.InvalidBackupManifest;
    var checksum: [checksum_size]u8 = undefined;
    if (checksum_hex.len != 2 * checksum_size) return error.InvalidBackupManifest;
    _ = std.fmt.hexToBytes(&checksum, checksum_hex) catch
        return error.InvalidBackupManifest;
    const payload_name = fields.next() orelse return error.InvalidBackupManifest;
    if (!valid_payload_name(payload_name) or fields.next() != null)
        return error.InvalidBackupManifest;
    switch (kind) {
        .wal, .manifest => if (level != 0 or id != 0)
            return error.InvalidBackupManifest,
        .sst => if (id == 0) return error.InvalidBackupManifest,
    }
    return .{
        .kind = kind,
        .level = level,
        .id = id,
        .size = size,
        .checksum = checksum,
        .payload_name = payload_name,
    };
}

fn valid_payload_name(name: []const u8) bool {
    if (name.len != "payload-0000".len or !std.mem.startsWith(u8, name, "payload-"))
        return false;
    for (name["payload-".len..]) |byte| {
        if (!std.ascii.isDigit(byte)) return false;
    }
    return true;
}

fn backup_file(
    allocator: Allocator,
    io: Io,
    manifest: IoFile,
    manifest_position: *u64,
    manifest_hash: *std.crypto.hash.Blake3,
    destination: []const u8,
    index: u16,
    kind: FileRecord.Kind,
    level: u32,
    id: u64,
    source: []const u8,
    copy_buffer: []u8,
    bytes_total: *u64,
) !void {
    var payload_name_buffer: ["payload-0000".len]u8 = undefined;
    const payload_name = try std.fmt.bufPrint(&payload_name_buffer, "payload-{d:0>4}", .{index});
    const target = try std.fmt.allocPrint(
        allocator,
        "{s}/{s}",
        .{ destination, payload_name },
    );
    defer allocator.free(target);
    const copied = try copy_file(io, source, target, true, copy_buffer);
    bytes_total.* = std.math.add(u64, bytes_total.*, copied.size) catch
        return error.BackupTooLarge;
    const checksum_hex = std.fmt.bytesToHex(copied.checksum, .lower);
    var line_buffer: [256]u8 = undefined;
    const line = try std.fmt.bufPrint(
        &line_buffer,
        "FILE {s} {d} {d} {d} {s} {s}",
        .{
            switch (kind) {
                .wal => "WAL",
                .manifest => "MANIFEST",
                .sst => "SST",
            },
            level,
            id,
            copied.size,
            &checksum_hex,
            payload_name,
        },
    );
    try append_manifest_line(manifest, io, manifest_position, manifest_hash, line);
}

const CopyResult = struct { size: u64, checksum: [checksum_size]u8 };

fn copy_file(
    io: Io,
    source_path: []const u8,
    target_path: []const u8,
    exclusive: bool,
    buffer: []u8,
) !CopyResult {
    const source = try IoDir.cwd().openFile(io, source_path, .{ .mode = .read_only });
    defer source.close(io);
    const size = (try source.stat(io)).size;
    const target = try IoDir.cwd().createFile(io, target_path, .{
        .truncate = false,
        .exclusive = exclusive,
    });
    errdefer target.close(io);
    var hash = std.crypto.hash.Blake3.init(.{});
    var offset: u64 = 0;
    while (offset < size) {
        const count: usize = @intCast(@min(@as(u64, buffer.len), size - offset));
        const read = try source.readPositionalAll(io, buffer[0..count], offset);
        if (read != count) return error.TruncatedSourceFile;
        hash.update(buffer[0..count]);
        try target.writePositionalAll(io, buffer[0..count], offset);
        offset += count;
    }
    try target.sync(io);
    target.close(io);
    return .{ .size = size, .checksum = hash_final(hash) };
}

fn verify_payload(
    io: Io,
    path: []const u8,
    expected_size: u64,
    expected_checksum: [checksum_size]u8,
    buffer: []u8,
) !void {
    const file = try IoDir.cwd().openFile(io, path, .{ .mode = .read_only });
    defer file.close(io);
    const size = (try file.stat(io)).size;
    if (size != expected_size) return error.BackupPayloadSizeMismatch;
    var hash = std.crypto.hash.Blake3.init(.{});
    var offset: u64 = 0;
    while (offset < size) {
        const count: usize = @intCast(@min(@as(u64, buffer.len), size - offset));
        const read = try file.readPositionalAll(io, buffer[0..count], offset);
        if (read != count) return error.TruncatedSourceFile;
        hash.update(buffer[0..count]);
        offset += count;
    }
    const actual = hash_final(hash);
    if (!std.crypto.timing_safe.eql([checksum_size]u8, actual, expected_checksum))
        return error.BackupPayloadChecksumMismatch;
}

fn append_manifest_line(
    file: IoFile,
    io: Io,
    position: *u64,
    hash: *std.crypto.hash.Blake3,
    line: []const u8,
) !void {
    try file.writePositionalAll(io, line, position.*);
    position.* += line.len;
    try file.writePositionalAll(io, "\n", position.*);
    position.* += 1;
    hash.update(line);
    hash.update("\n");
}

fn target_path_for(
    allocator: Allocator,
    config: *const LoadedConfig,
    base: []const u8,
    record: FileRecord,
) ![]u8 {
    return switch (record.kind) {
        .wal => allocator.dupe(u8, config.wal_path),
        .manifest => std.fmt.allocPrint(allocator, "{s}.manifest", .{base}),
        .sst => std.fmt.allocPrint(
            allocator,
            "{s}_L{d}_{d:0>8}.sst",
            .{ base, record.level, record.id },
        ),
    };
}

fn require_absent(io: Io, path: []const u8) !void {
    const file = IoDir.cwd().openFile(io, path, .{ .mode = .read_only }) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    file.close(io);
    return error.RestoreTargetExists;
}

fn derive_base(allocator: Allocator, wal_path: []const u8) ![]u8 {
    const base = if (std.mem.endsWith(u8, wal_path, ".wal"))
        wal_path[0 .. wal_path.len - 4]
    else
        wal_path;
    return allocator.dupe(u8, base);
}

fn write_hex(target: []u8, source: []const u8) void {
    std.debug.assert(target.len == 2 * source.len);
    const alphabet = "0123456789abcdef";
    for (source, 0..) |byte, index| {
        target[2 * index] = alphabet[byte >> 4];
        target[2 * index + 1] = alphabet[byte & 0x0f];
    }
}

fn hex_matches(encoded: []const u8, expected: []const u8) bool {
    if (encoded.len != 2 * expected.len) return false;
    var mismatch: u8 = 0;
    const alphabet = "0123456789abcdef";
    for (expected, 0..) |byte, index| {
        mismatch |= encoded[2 * index] ^ alphabet[byte >> 4];
        mismatch |= encoded[2 * index + 1] ^ alphabet[byte & 0x0f];
    }
    return mismatch == 0;
}

fn hash_final(hash_start: std.crypto.hash.Blake3) [checksum_size]u8 {
    var hash = hash_start;
    var digest: [checksum_size]u8 = undefined;
    hash.final(&digest);
    return digest;
}

fn checksum_bytes(bytes: []const u8) [checksum_size]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update(bytes);
    return hash_final(hash);
}

fn sync_parent_directory(path: []const u8, io: Io) !void {
    return sync_directory(std.fs.path.dirname(path) orelse ".", io);
}

fn sync_directory(path: []const u8, io: Io) !void {
    const handle = try std.posix.openat(
        std.posix.AT.FDCWD,
        path,
        .{ .ACCMODE = .RDONLY, .DIRECTORY = true, .CLOEXEC = true },
        0,
    );
    const file: IoFile = .{
        .handle = handle,
        .flags = .{ .nonblocking = false },
    };
    defer file.close(io);
    file.sync(io) catch return error.DirectorySyncFailed;
}

test "identity encoding comparison is exact" {
    var encoded: [8]u8 = undefined;
    write_hex(&encoded, "name");
    try std.testing.expect(hex_matches(&encoded, "name"));
    try std.testing.expect(!hex_matches(&encoded, "Name"));
}

fn cleanup_test_store(io: Io, base: []const u8) void {
    var path_buffer: [256]u8 = undefined;
    inline for (.{ ".wal", ".crumb", ".manifest" }) |suffix| {
        const path = std.fmt.bufPrint(&path_buffer, "{s}{s}", .{ base, suffix }) catch
            unreachable;
        IoDir.cwd().deleteFile(io, path) catch {};
    }
    const backup_state = std.fmt.bufPrint(
        &path_buffer,
        "{s}{s}",
        .{ base, backup_state_suffix },
    ) catch unreachable;
    IoDir.cwd().deleteFile(io, backup_state) catch {};
    for (0..3) |level| {
        for (1..9) |id| {
            const path = std.fmt.bufPrint(
                &path_buffer,
                "{s}_L{d}_{d:0>8}.sst",
                .{ base, level, id },
            ) catch unreachable;
            IoDir.cwd().deleteFile(io, path) catch {};
        }
    }
}

fn flip_first_byte(io: Io, path: []const u8) !void {
    const file = try IoDir.cwd().openFile(io, path, .{ .mode = .read_write });
    defer file.close(io);
    var byte: [1]u8 = undefined;
    if (try file.readPositionalAll(io, &byte, 0) != 1) return error.EmptyTestPayload;
    byte[0] ^= 1;
    try file.writePositionalAll(io, &byte, 0);
    try file.sync(io);
}

test "backup validates corruption and identity then restores a replayable store" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    const source_base = "data/test_backup_source";
    const target_base = "data/test_backup_target";
    const backup_path = "data/test_backup_archive";
    cleanup_test_store(io, source_base);
    cleanup_test_store(io, target_base);
    IoDir.cwd().deleteTree(io, backup_path) catch {};
    defer cleanup_test_store(io, source_base);
    defer cleanup_test_store(io, target_base);
    defer IoDir.cwd().deleteTree(io, backup_path) catch {};

    const source_json =
        \\{
        \\  "service_name":"backup-service",
        \\  "instance_id":"node-1",
        \\  "listen_address":"127.0.0.1:4200",
        \\  "wal_path":"data/test_backup_source.wal",
        \\  "client_secret":"0123456789abcdef0123456789abcdef",
        \\  "credentials":[{"service_name":"client",
        \\    "secret":"abcdef0123456789abcdef0123456789",
        \\    "permissions":["append"]}],
        \\  "stream_capacity":4096,
        \\  "stream_id_bytes_capacity":4194304,
        \\  "contracts":[]
        \\}
    ;
    const target_json =
        \\{
        \\  "service_name":"backup-service",
        \\  "instance_id":"node-1",
        \\  "listen_address":"127.0.0.1:4201",
        \\  "wal_path":"data/test_backup_target.wal",
        \\  "client_secret":"0123456789abcdef0123456789abcdef",
        \\  "credentials":[{"service_name":"client",
        \\    "secret":"abcdef0123456789abcdef0123456789",
        \\    "permissions":["append"]}],
        \\  "stream_capacity":4096,
        \\  "stream_id_bytes_capacity":4194304,
        \\  "contracts":[]
        \\}
    ;
    const wrong_identity_json =
        \\{
        \\  "service_name":"different-service",
        \\  "instance_id":"node-1",
        \\  "listen_address":"127.0.0.1:4202",
        \\  "wal_path":"data/test_backup_target.wal",
        \\  "client_secret":"0123456789abcdef0123456789abcdef",
        \\  "credentials":[{"service_name":"client",
        \\    "secret":"abcdef0123456789abcdef0123456789",
        \\    "permissions":["append"]}],
        \\  "stream_capacity":4096,
        \\  "stream_id_bytes_capacity":4194304,
        \\  "contracts":[]
        \\}
    ;
    var source_config = try config_mod.load_from_slice(allocator, source_json);
    defer source_config.deinit();
    var target_config = try config_mod.load_from_slice(allocator, target_json);
    defer target_config.deinit();
    var wrong_identity = try config_mod.load_from_slice(allocator, wrong_identity_json);
    defer wrong_identity.deinit();

    const events = [_]protocol.AppendEvent{
        .{ .event_type = "source.generation.requested", .schema_version = 1, .payload = "one" },
        .{ .event_type = "source.discovery.requested", .schema_version = 1, .payload = "two" },
    };
    const request: protocol.AppendStream = .{
        .stream_id = "source-generation/backup-test",
        .expected_version = .empty,
        .command_id = "backup-command-1",
        .events = &events,
    };
    const saved = saved: {
        var source_store = try EventStore.init(allocator, source_config.wal_path, io, .{
            .identity = source_config.identity.name,
            .contracts = source_config.contracts,
            .stream_capacity = source_config.stream_capacity,
            .stream_id_bytes_capacity = source_config.stream_id_bytes_capacity,
        });
        defer source_store.deinit();
        const response = try source_store.append_stream(request, 100);
        try std.testing.expect(response == .committed);
        try source_store.lsm.flush();
        break :saved try create(allocator, io, &source_config, &source_store, backup_path);
    };
    try std.testing.expectEqual(@as(u16, 3), saved.files);
    try std.testing.expectEqual(@as(u64, 2), saved.committed_log_position);
    const state = try read_state(allocator, io, source_config.wal_path);
    try std.testing.expectEqual(@as(u64, 1), state.completed);
    try std.testing.expect(state.last_success_unix_seconds > 0);
    try std.testing.expectEqual(saved.committed_log_position, state.last_committed_log_position);

    try std.testing.expectError(
        error.BackupIdentityMismatch,
        restore(allocator, io, &wrong_identity, backup_path),
    );
    const nonempty_payload = "data/test_backup_archive/payload-0001";
    try flip_first_byte(io, nonempty_payload);
    try std.testing.expectError(
        error.BackupPayloadChecksumMismatch,
        restore(allocator, io, &target_config, backup_path),
    );
    try flip_first_byte(io, nonempty_payload);

    const missing_payload = "data/test_backup_archive/payload-0002";
    const held_payload = "data/test_backup_archive/payload-held";
    try IoDir.cwd().renamePreserve(missing_payload, IoDir.cwd(), held_payload, io);
    try std.testing.expectError(
        error.FileNotFound,
        restore(allocator, io, &target_config, backup_path),
    );
    try IoDir.cwd().renamePreserve(held_payload, IoDir.cwd(), missing_payload, io);

    {
        const truncated_file = try IoDir.cwd().openFile(
            io,
            nonempty_payload,
            .{ .mode = .read_write },
        );
        defer truncated_file.close(io);
        const original_size = (try truncated_file.stat(io)).size;
        var final_byte: [1]u8 = undefined;
        if (try truncated_file.readPositionalAll(io, &final_byte, original_size - 1) != 1)
            return error.TruncatedTestPayload;
        try truncated_file.setLength(io, original_size - 1);
        try truncated_file.sync(io);
        try std.testing.expectError(
            error.BackupPayloadSizeMismatch,
            restore(allocator, io, &target_config, backup_path),
        );
        try truncated_file.setLength(io, original_size);
        try truncated_file.writePositionalAll(io, &final_byte, original_size - 1);
        try truncated_file.sync(io);
    }

    const restored = try restore(allocator, io, &target_config, backup_path);
    try std.testing.expectEqual(saved.files, restored.files);
    try std.testing.expectEqual(saved.bytes, restored.bytes);
    try std.testing.expectEqual(saved.committed_log_position, restored.committed_log_position);
    {
        var target_store = try EventStore.init(allocator, target_config.wal_path, io, .{
            .identity = target_config.identity.name,
            .contracts = target_config.contracts,
            .stream_capacity = target_config.stream_capacity,
            .stream_id_bytes_capacity = target_config.stream_id_bytes_capacity,
        });
        defer target_store.deinit();
        const verification = try target_store.verify_integrity();
        try std.testing.expectEqual(@as(u64, 1), verification.batches);
        try std.testing.expectEqual(@as(u64, 2), verification.events);
        try std.testing.expectEqual(@as(u64, 2), verification.committed_log_position);
        const replayed = try target_store.append_stream(request, 200);
        try std.testing.expect(replayed == .replayed);
    }
    try std.testing.expectError(
        error.RestoreTargetExists,
        restore(allocator, io, &target_config, backup_path),
    );
}
