// swift-tools-version:3.1

import PackageDescription

let package = Package(
    name: "SQLiteYourself",
    dependencies: [
        .Package(url: "https://github.com/Zewo/Reflection.git", majorVersion: 0, minor: 14),
    ]
)
