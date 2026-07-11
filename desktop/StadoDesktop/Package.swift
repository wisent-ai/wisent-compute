// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "StadoDesktop",
    platforms: [.macOS(.v14)],
    products: [
        .executable(name: "Stado", targets: ["Stado"]),
    ],
    targets: [
        .executableTarget(
            name: "Stado",
            path: "Sources/Stado"
        ),
        .testTarget(
            name: "StadoTests",
            dependencies: ["Stado"],
            path: "Tests/StadoTests"
        )
    ]
)
