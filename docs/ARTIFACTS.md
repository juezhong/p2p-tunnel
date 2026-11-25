# CI 构建产物命名规范

本规范定义可下载构建产物的名字、内容与校验方式。参考实现是本仓库的
`.github/workflows/build-binaries.yml`（七个平台的 release 二进制只在这里构建）；
`.github/workflows/release.yml` 在 `v*` tag 上调用同一工作流并发布到 GitHub
Release。后续 `p2p-net`、`p2p-transfer`、`p2p-sdk` 接入 CI 时沿用同一套规则，
不各写一套。

## 1. 平台矩阵

三个系统、七个产物：Linux 四个（架构 × libc）、Android 一个、Windows 一个、macOS 一个。

| 产物名 | target triple | 构建机 | 用途 |
| --- | --- | --- | --- |
| `p2p-tunnel-linux-x86_64-gnu` | `x86_64-unknown-linux-gnu` | ubuntu | 普通 Linux（glibc） |
| `p2p-tunnel-linux-aarch64-gnu` | `aarch64-unknown-linux-gnu` | ubuntu（cross） | ARM64 Linux（glibc） |
| `p2p-tunnel-linux-x86_64-musl` | `x86_64-unknown-linux-musl` | ubuntu（cross） | 静态 x86_64 |
| `p2p-tunnel-linux-aarch64-musl` | `aarch64-unknown-linux-musl` | ubuntu（cross） | 静态 ARM64 Linux |
| `p2p-tunnel-android-aarch64` | `aarch64-linux-android` | ubuntu（NDK） | Termux / Android |
| `p2p-tunnel-windows-x86_64` | `x86_64-pc-windows-msvc` | windows | Windows |
| `p2p-tunnel-macos-aarch64` | `aarch64-apple-darwin` | macos-15 | Apple Silicon |

## 2. 命名格式

```
<binary>-<platform>-<arch>[-<libc>]                  开发构建
<binary>-v<semver>-<platform>-<arch>[-<libc>]        发布构建
```

- `<binary>`：`Cargo.toml` 里 `[[bin]] name` 的原值，如 `p2p-tunnel`。
- `<semver>`：workspace 版本或 tag 去掉前缀 `v`，如 `0.1.0`。
- `<platform>`：`linux`、`android`、`windows`、`macos`。
- `<arch>`：`x86_64`、`aarch64`。
- `<libc>`：仅 Linux 需要，`gnu` 或 `musl`；Android 直接链接 bionic，不写 libc 后缀。
- 全小写，字段用 `-`；不加空格、下划线。Windows 可执行文件保留必需的 `.exe` 后缀。

target triple 只出现在文档和 CI 的 `target` 字段里，**不进入产物名**。

## 3. 平台规则

1. **Linux** 区分架构和 libc：`x86_64`/`aarch64` × `gnu`/`musl`，共四个。
2. **Termux 用 Android 产物**：`p2p-tunnel-android-aarch64`，target 为
   `aarch64-linux-android`，用 NDK 构建。Termux 运行在 bionic 上，不是 glibc
   也不是 musl；`linux-aarch64-gnu` 会因 TLS 段对齐被 bionic 拒绝，
   `linux-aarch64-musl` 会因非 PIE 被 Android 内核拒绝。
3. **Android 构建约束**：必须 PIE（ET_DYN），TLS 段对齐 64 字节（Android 16 起
   强制），并带 `-Wl,-z,max-page-size=16384` 以兼容 16 KiB 页设备。CI 在冒烟
   步骤里校验 PIE 与 TLS 对齐，不通过直接失败。
4. **Windows** 只出 `x86_64`，libc 固定 msvc，不写后缀。
5. **macOS** 只出 `aarch64`（Apple Silicon），不出 `x86_64`。CI 会做 ad-hoc
   `codesign`；从浏览器下载后仍带 quarantine 属性，首次运行需
   `xattr -d com.apple.quarantine ./p2p-tunnel`，或在“系统设置 → 隐私与安全性”
   里选择“仍要打开”。正式分发需要 Apple Developer 签名与公证。

## 4. 为什么不用 runner 镜像名

`ubuntu-latest`、`windows-latest`、`macos-latest` 是 GitHub 托管 runner 的镜像标签，
不是构建目标：

- 镜像会随 GitHub 升级轮换；
- 一个镜像能构建多个 target（同一台 ubuntu 构建 4 个 Linux 产物）；
- 下载者关心的是平台、架构、libc，镜像名给不出这些信息。

镜像名只出现在 `runs-on`，产物名一律由平台矩阵决定。

## 5. 产物内容

每个 artifact 只放一个平台，结构固定：

```
p2p-tunnel            （Windows 为 p2p-tunnel.exe）
SHA256SUMS
```

- 二进制放归档根目录，不再套 `bin/` 或平台子目录。
- `SHA256SUMS` 用 `sha256sum` 兼容格式：`<hex>  <binary>`。
- 上传前必须做冒烟测试：能在构建机原生运行的直接跑 `--version`/`--help`；
  aarch64-musl 用 QEMU（`docker/setup-qemu-action` 注册 binfmt）跑；
  Android 产物在 Linux 上无法执行，改为校验 PIE 与 TLS 对齐；aarch64-gnu 没有
  本机 loader 时记录为“未执行”，不允许假装验证过。
- 发布版可额外附 `LICENSE`、`README.md`，不改变二进制路径。
- 不打包 `target/`、调试符号、测试数据、本机路径。

## 6. 保留与发布

- 分支/PR 产物：Actions artifact，保留 90 天，用于真机验收与回归对比。
- 发布产物：tag 触发的 workflow 附加到 GitHub Release，长期保留。
- `v0.1.0` tag 对应 `p2p-tunnel-v0.1.0-<platform>-<arch>[-<libc>]`（Windows 带 `.exe`），二进制与汇总 `SHA256SUMS` 作为 Release 资产。

## 7. 示例

```text
p2p-tunnel-linux-x86_64-gnu
p2p-tunnel-linux-aarch64-gnu
p2p-tunnel-linux-x86_64-musl
p2p-tunnel-linux-aarch64-musl
p2p-tunnel-android-aarch64
p2p-tunnel-windows-x86_64
p2p-tunnel-macos-aarch64
p2p-tunnel-v0.1.0-android-aarch64
p2p-tunnel-v0.1.0-macos-aarch64
```

反例：

```text
p2p-tunnel-ubuntu-latest     # runner 镜像名
p2p-tunnel-linux             # 缺架构和 libc
p2p-tunnel-windows-x86_64-msvc   # msvc 是固定项，不写后缀
p2p-tunnel-macos-x86_64      # 不出 Intel macOS
p2p_tunnel-linux-x86_64      # 下划线
```

## 8. CI 必须校验

1. artifact 名由平台矩阵拼接生成，不在 `name:` 里硬编码平台词。
2. 二进制存在且可执行（Windows 检查 `.exe`）。
3. 生成并上传 `SHA256SUMS`。
4. 下载端先校验再运行：

```bash
sha256sum -c SHA256SUMS
```

## 9. 其他项目接入清单

1. matrix 用 `include: [{ os, platform, target }]`，产物名 `name: <binary>-${{ matrix.platform }}`。
2. 本机可构建的直接 `cargo build`；跨架构/跨 libc 用 `houseabsolute/actions-rust-cross`。
3. 发布 job 复用同一平台矩阵，前缀加 `v<semver>-`。
4. 在本项目文档中链接本文件，不复制矩阵。
