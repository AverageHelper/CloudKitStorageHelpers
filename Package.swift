// swift-tools-version:5.2

import PackageDescription

let package = Package(
    name: "CloudKitStorageHelpers",
    platforms: [
        .macOS(.v10_15),
        .iOS(.v13),
        .tvOS(.v13),
        .watchOS(.v6),
    ],
    products: [
        .library(
            name: "CloudKitStorageHelpers",
            targets: ["CloudKitStorageHelpers"]
        ),
    ],
    dependencies: [
        .package(
            name: "CloudStorage",
            url: "https://github.com/AverageHelper/CloudStorage.git",
            .upToNextMinor(from: "0.1.0")
        ),
        .package(
            name: "CloudKitMocks",
            url: "https://github.com/AverageHelper/CloudKitMocks.git",
            .upToNextMinor(from: "0.1.0")
        ),
    ],
    targets: [
        .target(
            name: "CloudKitStorageHelpers",
            dependencies: ["CloudStorage"]
        ),
        .testTarget(
            name: "CloudKitStorageHelpersTests",
            dependencies: ["CloudKitStorageHelpers", "CloudKitMocks"]
        ),
    ]
)
