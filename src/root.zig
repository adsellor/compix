//! Public module surface of the compix library.
pub const store = @import("core/store.zig");
pub const server = @import("core/server.zig");
pub const config = @import("core/config.zig");
pub const protocol = @import("core/protocol.zig");
pub const transport = @import("core/transport.zig");
pub const contract = @import("core/contract.zig");
pub const event = @import("core/event.zig");
pub const identity = @import("core/identity.zig");

test {
    _ = store;
}
