// swift-tools-version:5.6
//
// Concurrency probes for `URLSessionTransport`, kept in a separate package so they are not part
// of the library's package graph. They run under Thread Sanitizer without Xcode, which the Quick
// specs cannot do here:
//
//   swift run --sanitize=thread TransportRaceHarness
//   swift run --sanitize=thread HeartbeatDeadlockProbe
//   swift run FeedLifecycleProbe [shipped|sync-hop|claim-then-act]

import PackageDescription

let package = Package(
    name: "SwiftPhoenixClientTools",
    platforms: [
        .macOS(.v10_15)
    ],
    dependencies: [
        .package(path: "..")
    ],
    targets: [
        .executableTarget(
            name: "TransportRaceHarness",
            dependencies: [.product(name: "SwiftPhoenixClient", package: "SwiftPhoenixClient")],
            path: "TransportRaceHarness"),
        .executableTarget(
            name: "HeartbeatDeadlockProbe",
            dependencies: [.product(name: "SwiftPhoenixClient", package: "SwiftPhoenixClient")],
            path: "HeartbeatDeadlockProbe"),
        .executableTarget(
            name: "FeedLifecycleProbe",
            dependencies: [.product(name: "SwiftPhoenixClient", package: "SwiftPhoenixClient")],
            path: "FeedLifecycleProbe"),
    ]
)
