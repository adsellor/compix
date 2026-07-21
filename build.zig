const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    _ = b.addModule("compix_driver", .{
        .root_source_file = b.path("src/driver.zig"),
        .target = target,
        .optimize = optimize,
    });

    const exe = b.addExecutable(.{
        .name = "compix",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    b.installArtifact(exe);

    const run_step = b.step("run", "Run the app");
    const run_cmd = b.addRunArtifact(exe);
    run_step.dependOn(&run_cmd.step);
    run_cmd.step.dependOn(b.getInstallStep());
    run_cmd.addPassthruArgs();

    // test.zig keeps the driver and core files in one Zig module while the
    // store's top-level test block imports every core module.
    const store_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/test.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_store_tests = b.addRunArtifact(store_tests);
    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_store_tests.step);

    const backup_process = b.addExecutable(.{
        .name = "backup-process-test",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/backup_process_test.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const prepare_backup = b.addRunArtifact(backup_process);
    prepare_backup.addArg("prepare");
    const backup_command = b.addRunArtifact(exe);
    backup_command.addArgs(&.{ "backup", source_config_path, archive_path });
    backup_command.step.dependOn(&prepare_backup.step);
    const restore_command = b.addRunArtifact(exe);
    restore_command.addArgs(&.{ "restore", target_config_path, archive_path });
    restore_command.step.dependOn(&backup_command.step);
    const verify_command = b.addRunArtifact(exe);
    verify_command.addArgs(&.{ "verify", target_config_path });
    verify_command.step.dependOn(&restore_command.step);
    const check_backup = b.addRunArtifact(backup_process);
    check_backup.addArg("check");
    check_backup.step.dependOn(&verify_command.step);
    test_step.dependOn(&check_backup.step);

    const bench_exe = b.addExecutable(.{
        .name = "bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bench.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });
    const run_bench = b.addRunArtifact(bench_exe);
    run_bench.addPassthruArgs();
    const bench_step = b.step("bench", "Run performance benchmarks");
    bench_step.dependOn(&run_bench.step);
}

const source_config_path = "test/backup_source.json";
const target_config_path = "test/backup_target.json";
const archive_path = "data/process_backup_archive";
