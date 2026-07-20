const std = @import("std");
const assert = std.debug.assert;

pub const ByteCount = u32;
pub const CollectionCount = u32;
pub const CollectionIndex = u32;
pub const ConnectionCount = u16;
pub const EventCount = u16;
pub const MemtableEntryCount = u32;
pub const MemtableEntryIndex = u32;
pub const MessageCount = u16;
pub const NodeCount = u32;
pub const NodeIndex = u32;
pub const Sequence = u64;
pub const SSTableCount = u16;
pub const SSTableIndex = u16;
pub const StreamCount = u32;
pub const StreamSlotIndex = u32;
pub const Timestamp = i64;

pub const config_file_size_max: ByteCount = 1024 * 1024;
pub const append_events_max: EventCount = 256;
pub const connection_capacity_default: ConnectionCount = 32;
pub const connection_capacity_test_default: ConnectionCount = 16;
pub const connections_max: ConnectionCount = 1024;
pub const compaction_fan_in_max: SSTableCount = 4;
pub const contracts_max: EventCount = 256;
pub const event_types_per_contract_max: EventCount = 256;
pub const event_type_length_max: ByteCount = 1024;
pub const event_serialized_size_max: ByteCount = 512 * 1024;
pub const instance_id_length_max: ByteCount = 256;
pub const key_length_max: ByteCount = 4096;
pub const manifest_file_size_max: ByteCount = 64 * 1024 * 1024;
pub const memtable_bytes_max: ByteCount = 16 * 1024 * 1024;
pub const memtable_entries_max: MemtableEntryCount = 50_000;
pub const messages_per_connection_max: MessageCount = 1024;
pub const payload_size_max: ByteCount = 256 * 1024;
pub const peer_address_length_max: ByteCount = 512;
pub const pull_events_max: EventCount = 256;
pub const read_events_max: EventCount = 256;
pub const radix_nodes_max: NodeCount = 2 * memtable_entries_max + 1;
pub const recovery_tasks_max: u32 =
    @as(u32, contracts_max) * @as(u32, event_types_per_contract_max);
pub const service_name_length_max: ByteCount = 1024;
pub const stream_capacity_default: StreamCount = 1_048_576;
pub const stream_capacity_test_default: StreamCount = 4096;
pub const streams_max: StreamCount = 16_777_216;
pub const stream_id_bytes_capacity_default: ByteCount = 64 * 1024 * 1024;
pub const stream_id_bytes_capacity_test_default: ByteCount = 4 * 1024 * 1024;
pub const stream_id_bytes_capacity_max: ByteCount = std.math.maxInt(ByteCount);
pub const snapshot_size_default_max: u64 = 64 * 1024 * 1024 * 1024;
pub const snapshot_size_hard_max: u64 = 1024 * 1024 * 1024 * 1024;
pub const snapshot_chunk_size_max: ByteCount = 512 * 1024;
pub const sstables_max: u16 = 1024;
pub const sstable_block_size_max: ByteCount = 512 * 1024;
pub const sstable_index_size_max: ByteCount = memtable_bytes_max + 1024 * 1024;
pub const wal_path_length_max: ByteCount = 4096;
pub const wal_entry_size_max: ByteCount = 4 * 1024 * 1024;

comptime {
    assert(config_file_size_max > 0);
    assert(append_events_max > 0);
    assert(connection_capacity_default > 0);
    assert(connection_capacity_default <= connections_max);
    assert(connection_capacity_test_default <= connection_capacity_default);
    assert(compaction_fan_in_max >= 2);
    assert(contracts_max > 0);
    assert(event_types_per_contract_max > 0);
    assert(event_serialized_size_max > payload_size_max + event_type_length_max);
    assert(key_length_max > event_type_length_max);
    assert(memtable_bytes_max > payload_size_max);
    assert(memtable_entries_max > 0);
    assert(messages_per_connection_max > 0);
    assert(payload_size_max >= event_type_length_max);
    assert(pull_events_max > 0);
    assert(read_events_max > 0);
    assert(radix_nodes_max > memtable_entries_max);
    assert(recovery_tasks_max >= contracts_max);
    assert(stream_capacity_default >= 1_000_000);
    assert(stream_capacity_default <= streams_max);
    assert(stream_capacity_test_default < stream_capacity_default);
    assert(stream_id_bytes_capacity_default >= stream_capacity_default);
    assert(stream_id_bytes_capacity_default <= stream_id_bytes_capacity_max);
    assert(stream_id_bytes_capacity_test_default >= stream_capacity_test_default);
    assert(streams_max > 0);
    assert(sstables_max > 0);
    assert(sstable_block_size_max > payload_size_max + key_length_max);
    assert(sstable_index_size_max > memtable_bytes_max);
    assert(wal_entry_size_max > payload_size_max);
}

pub fn byte_count(value: usize) error{LimitExceeded}!ByteCount {
    if (value > std.math.maxInt(ByteCount)) return error.LimitExceeded;
    return @intCast(value);
}

pub fn usize_from(value: anytype) usize {
    return @intCast(value);
}
