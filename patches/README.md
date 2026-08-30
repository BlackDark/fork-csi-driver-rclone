# Dependency patches

In-tree `vendor/` / `third_party/` trees are gone. Fixes ship via `go.mod` replaces + a small driver change.

| Former in-tree patch | Resolution |
|---|---|
| `vfs-disable-reuse` / `vfs-refresh-all-duplicate-vfs` | `fs.NewFs(context.WithoutCancel(mountCtx), …)` in `pkg/rclone/nodeserver.go` — OAuth/`Fs` lifetime not tied to mount cancel; stock rclone VFS reuse is fine. |
| `go-fuse-v2` (fh/nodeIndex panic guards) | `replace github.com/hanwen/go-fuse/v2 => github.com/BlackDark/fork-go-fuse/v2 v2.12.0-csi.5` |
| (bip39) deleted tyler-smith module | `replace github.com/tyler-smith/go-bip39 => github.com/BlackDark/go-bip39 v1.1.0-csi.1` |

Builds use the module proxy (`go mod download` in Docker). No patch apply / vendor-sync make targets.
