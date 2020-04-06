// swift-tools-version:5.2

import PackageDescription

let package = Package(
    name: "CloudKitStorageHelpers",
    products: [
        .library(
            name: "CloudKitStorageHelpers",
            targets: ["CloudKitStorageHelpers"]),
    ],
    dependencies: [
        .package(name: "CloudStorage", url: "https://github.com/AverageHelper/CloudStorage.git", .upToNextMinor(from: "0.1.0")),
    ],
    targets: [
        .target(
            name: "CloudKitStorageHelpers",
            dependencies: ["CloudStorage"]),
        .testTarget(
            name: "CloudKitStorageHelpersTests",
            dependencies: ["CloudKitStorageHelpers"]),
    ]
)
