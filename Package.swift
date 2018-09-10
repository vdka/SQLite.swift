// swift-tools-version:4.0
import PackageDescription

let package = Package(
    name: "SQLiteYourself",
    products: [
        .library(name: "SQLiteYourself", targets: ["SQLiteYourself"]),
    ],
    targets: [
        .target(name: "SQLiteYourself"),
        .testTarget(name: "SQLiteYourselfTests", dependencies: ["SQLiteYourself"]),
    ]
)
