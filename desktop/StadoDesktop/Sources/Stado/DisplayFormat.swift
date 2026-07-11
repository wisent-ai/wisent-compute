import Foundation

@MainActor
enum DisplayFormat {
    private static let bytesFormatter: ByteCountFormatter = {
        let formatter = ByteCountFormatter()
        formatter.countStyle = .file
        formatter.allowedUnits = [.useKB, .useMB, .useGB, .useTB]
        formatter.includesUnit = true
        formatter.isAdaptive = true
        return formatter
    }()

    private static let fractionalDateStrategy = Date.ISO8601FormatStyle(
        includingFractionalSeconds: true
    )

    private static let dateStrategy = Date.ISO8601FormatStyle()

    static func bytes(_ value: Int?) -> String {
        guard let value else { return "Unavailable" }
        return bytesFormatter.string(fromByteCount: Int64(value))
    }

    static func date(_ value: String?) -> Date? {
        guard let value else { return nil }
        return (try? fractionalDateStrategy.parse(value)) ?? (try? dateStrategy.parse(value))
    }

    static func duration(milliseconds: Int) -> String {
        if milliseconds < 1_000 {
            return "\(milliseconds) ms"
        }
        return String(format: "%.1f s", Double(milliseconds) / 1_000)
    }
}
