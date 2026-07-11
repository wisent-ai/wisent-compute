import Foundation

struct CleanupResponse: Codable, Sendable {
    let ok: Bool
    let service: String
    let report: CleanupReport
}

struct CleanupReport: Codable, Sendable {
    let version: Int
    let mode: String?
    let checkIntervalSeconds: Int?
    let startedAt: String?
    let durationMs: Int
    let outcome: String
    let freeBytesBefore: Int?
    let freeBytesAfter: Int?
    let lowBytes: Int?
    let targetBytes: Int?
    let pressureActive: Bool?
    let cleaners: CleanupCleaners
    let caps: CleanupCaps
    let lockBusy: Bool
    let activeSlotCount: Int
    let lastSuccessAt: String?
    let errors: [String]

    enum CodingKeys: String, CodingKey {
        case version, mode, outcome, cleaners, caps, errors
        case checkIntervalSeconds = "check_interval_seconds"
        case startedAt = "started_at"
        case durationMs = "duration_ms"
        case freeBytesBefore = "free_bytes_before"
        case freeBytesAfter = "free_bytes_after"
        case lowBytes = "low_bytes"
        case targetBytes = "target_bytes"
        case pressureActive = "pressure_active"
        case lockBusy = "lock_busy"
        case activeSlotCount = "active_slot_count"
        case lastSuccessAt = "last_success_at"
    }

    var reclaimedBytes: Int {
        guard let before = freeBytesBefore, let after = freeBytesAfter else { return 0 }
        return max(0, after - before)
    }

    var outcomePresentation: OutcomePresentation {
        switch outcome {
        case "never_run":
            OutcomePresentation(title: "No cleanup pass yet", detail: "The dashboard has no completed cleanup report.", symbol: "clock.badge.questionmark", severity: .neutral)
        case "healthy_noop":
            OutcomePresentation(title: "Healthy", detail: "Free space is above the cleanup threshold.", symbol: "checkmark.circle.fill", severity: .healthy)
        case "reclaimed_target":
            OutcomePresentation(title: "Target restored", detail: "Cleanup restored the registry target.", symbol: "checkmark.circle.fill", severity: .healthy)
        case "interval_noop":
            OutcomePresentation(title: "Checked recently", detail: "The registry-controlled interval has not elapsed.", symbol: "clock.fill", severity: .neutral)
        case "report_only":
            OutcomePresentation(title: "Report only", detail: "Policy observed pressure without deleting data.", symbol: "doc.text.magnifyingglass", severity: .warning)
        case "blocked_active":
            OutcomePresentation(title: "Waiting for active work", detail: "Cleanup is blocked while compute slots are active.", symbol: "pause.circle.fill", severity: .warning)
        case "cap_reached":
            OutcomePresentation(title: "Pass limit reached", detail: "Pressure remains after a bounded cleanup pass.", symbol: "gauge.with.dots.needle.67percent", severity: .warning)
        case "no_eligible_items":
            OutcomePresentation(title: "No eligible items", detail: "Pressure remains, but policy authorized no deletions.", symbol: "exclamationmark.triangle.fill", severity: .warning)
        case "lock_busy":
            OutcomePresentation(title: "Cleanup already running", detail: "Another registry-controlled pass holds the cleanup lock.", symbol: "hourglass", severity: .neutral)
        case "partial_error":
            OutcomePresentation(title: "Cleanup incomplete", detail: "The pass completed with sanitized errors.", symbol: "exclamationmark.triangle.fill", severity: .critical)
        case "invalid_or_unavailable_policy":
            OutcomePresentation(title: "Policy unavailable", detail: "Cleanup failed closed because registry policy could not be validated.", symbol: "xmark.shield.fill", severity: .critical)
        case "runtime_error":
            OutcomePresentation(title: "Cleanup failed", detail: "The cleanup service reported a sanitized runtime failure.", symbol: "xmark.octagon.fill", severity: .critical)
        default:
            OutcomePresentation(title: outcome.humanizedIdentifier, detail: "The cleanup service returned this outcome.", symbol: "info.circle.fill", severity: .neutral)
        }
    }
}

struct CleanupCleaners: Codable, Sendable {
    let huggingFaceCache: CleanerReport
    let welesRecordings: CleanerReport

    enum CodingKeys: String, CodingKey {
        case huggingFaceCache = "huggingface_cache"
        case welesRecordings = "weles_recordings"
    }

    var namedReports: [(String, CleanerReport)] {
        [
            ("Hugging Face cache", huggingFaceCache),
            ("Weles recordings", welesRecordings),
        ]
    }
}

struct CleanerReport: Codable, Sendable {
    let scannedItems: Int
    let eligibleItems: Int
    let deletedItems: Int
    let expectedBytes: Int
    let actualFreeDeltaBytes: Int
    let skipped: [String: Int]

    enum CodingKeys: String, CodingKey {
        case skipped
        case scannedItems = "scanned_items"
        case eligibleItems = "eligible_items"
        case deletedItems = "deleted_items"
        case expectedBytes = "expected_bytes"
        case actualFreeDeltaBytes = "actual_free_delta_bytes"
    }
}

struct CleanupCaps: Codable, Sendable {
    let bytes: Bool
    let items: Bool
    let scan: Bool
    let deadline: Bool

    var activeLabels: [String] {
        [
            bytes ? "byte limit" : nil,
            items ? "item limit" : nil,
            scan ? "scan limit" : nil,
            deadline ? "time limit" : nil,
        ].compactMap { $0 }
    }
}

struct OutcomePresentation: Sendable {
    enum Severity: Sendable {
        case healthy
        case neutral
        case warning
        case critical
    }

    let title: String
    let detail: String
    let symbol: String
    let severity: Severity
}

extension String {
    var humanizedIdentifier: String {
        replacingOccurrences(of: "_", with: " ")
            .split(separator: " ")
            .map { $0.lowercased() }
            .joined(separator: " ")
            .capitalized
    }
}
