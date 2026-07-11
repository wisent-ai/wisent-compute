import AppKit
import SwiftUI

struct CleanupMenuView: View {
    @ObservedObject var store: CleanupStore

    var body: some View {
        VStack(spacing: 0) {
            header
            Divider()
            ScrollView {
                VStack(alignment: .leading, spacing: 14) {
                    if let errorMessage = store.errorMessage {
                        ErrorBanner(message: errorMessage)
                    }

                    if let report = store.report {
                        ReportContent(report: report)
                    } else if store.isRefreshing {
                        LoadingView()
                    } else {
                        EmptyViewContent()
                    }
                }
                .padding(16)
            }
            Divider()
            footer
        }
        .frame(width: 390, height: 590)
    }

    private var header: some View {
        HStack(spacing: 10) {
            Image(systemName: "externaldrive.fill.badge.checkmark")
                .font(.title2)
                .foregroundStyle(.tint)
            VStack(alignment: .leading, spacing: 1) {
                Text("Stado")
                    .font(.headline)
                if let updated = store.lastUpdated {
                    Text("Updated \(updated, style: .relative)")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                } else {
                    Text("Disk cleanup operator")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
            }
            Spacer()
            Button {
                Task { await store.refresh() }
            } label: {
                if store.isRefreshing {
                    ProgressView()
                        .controlSize(.small)
                } else {
                    Image(systemName: "arrow.clockwise")
                }
            }
            .buttonStyle(.borderless)
            .help("Refresh cleanup status")
            .disabled(store.isRefreshing || store.isRunningCleanup)
        }
        .padding(14)
    }

    private var footer: some View {
        VStack(spacing: 10) {
            Button {
                Task { await store.runCleanup() }
            } label: {
                HStack {
                    if store.isRunningCleanup {
                        ProgressView()
                            .controlSize(.small)
                        Text("Running registry-controlled pass…")
                    } else if store.report?.lockBusy == true {
                        Image(systemName: "hourglass")
                        Text("Cleanup Already Running")
                    } else {
                        Image(systemName: "sparkles")
                        Text("Run Cleanup Pass")
                    }
                }
                .frame(maxWidth: .infinity)
            }
            .buttonStyle(.borderedProminent)
            .disabled(store.isRefreshing || store.isRunningCleanup || store.report?.lockBusy == true || store.dashboardAddress == nil)

            HStack {
                Button("Open Dashboard", systemImage: "safari") {
                    guard let url = store.dashboardAddress?.dashboardURL else { return }
                    NSWorkspace.shared.open(url)
                }
                .disabled(store.dashboardAddress == nil)

                Spacer()

                SettingsLink {
                    Label("Settings", systemImage: "gearshape")
                }
                Button("Quit", systemImage: "power") {
                    NSApplication.shared.terminate(nil)
                }
            }
            .buttonStyle(.borderless)
            .font(.caption)
        }
        .padding(14)
    }
}

private struct ReportContent: View {
    let report: CleanupReport

    var body: some View {
        VStack(alignment: .leading, spacing: 14) {
            OutcomeCard(report: report)
            PressureCard(report: report)
            CleanerSummary(report: report)

            if !report.caps.activeLabels.isEmpty {
                Label("Pass bounded by \(report.caps.activeLabels.joined(separator: ", "))", systemImage: "gauge.with.dots.needle.67percent")
                    .font(.caption)
                    .foregroundStyle(.orange)
            }

            if !report.errors.isEmpty {
                VStack(alignment: .leading, spacing: 5) {
                    Label("Sanitized errors", systemImage: "exclamationmark.triangle.fill")
                        .font(.subheadline.weight(.semibold))
                        .foregroundStyle(.red)
                    ForEach(report.errors, id: \.self) { error in
                        Text(error)
                            .font(.caption.monospaced())
                            .textSelection(.enabled)
                    }
                }
                .padding(12)
                .frame(maxWidth: .infinity, alignment: .leading)
                .background(.red.opacity(0.08), in: RoundedRectangle(cornerRadius: 10))
            }

            HStack {
                Label("\(report.activeSlotCount) active \(report.activeSlotCount == 1 ? "slot" : "slots")", systemImage: "cpu")
                Spacer()
                Text(DisplayFormat.duration(milliseconds: report.durationMs))
            }
            .font(.caption)
            .foregroundStyle(.secondary)
        }
    }
}

private struct OutcomeCard: View {
    let report: CleanupReport

    private var presentation: OutcomePresentation { report.outcomePresentation }

    private var color: Color {
        switch presentation.severity {
        case .healthy: .green
        case .neutral: .blue
        case .warning: .orange
        case .critical: .red
        }
    }

    var body: some View {
        HStack(alignment: .top, spacing: 11) {
            Image(systemName: presentation.symbol)
                .font(.title2)
                .foregroundStyle(color)
            VStack(alignment: .leading, spacing: 3) {
                Text(presentation.title)
                    .font(.headline)
                Text(presentation.detail)
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
            }
            Spacer(minLength: 0)
        }
        .padding(12)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(color.opacity(0.09), in: RoundedRectangle(cornerRadius: 10))
    }
}

private struct PressureCard: View {
    let report: CleanupReport

    private var pressureLabel: String {
        switch report.pressureActive {
        case true: "Pressure active"
        case false: "Storage healthy"
        case nil: "Pressure unknown"
        }
    }

    private var pressureColor: Color {
        switch report.pressureActive {
        case true: .orange
        case false: .green
        case nil: .secondary
        }
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack {
                Label(pressureLabel, systemImage: report.pressureActive == true ? "externaldrive.fill.badge.exclamationmark" : "externaldrive.fill.badge.checkmark")
                    .font(.subheadline.weight(.semibold))
                    .foregroundStyle(pressureColor)
                Spacer()
                if let mode = report.mode {
                    Text(mode.humanizedIdentifier)
                        .font(.caption.weight(.medium))
                        .padding(.horizontal, 7)
                        .padding(.vertical, 3)
                        .background(.quaternary, in: Capsule())
                }
            }

            HStack(spacing: 0) {
                Metric(title: "Free", value: DisplayFormat.bytes(report.freeBytesAfter))
                Divider().frame(height: 34)
                Metric(title: "Low threshold", value: DisplayFormat.bytes(report.lowBytes))
                Divider().frame(height: 34)
                Metric(title: "Target", value: DisplayFormat.bytes(report.targetBytes))
                Divider().frame(height: 34)
                Metric(title: "Reclaimed", value: DisplayFormat.bytes(report.reclaimedBytes))
            }

            if let started = DisplayFormat.date(report.startedAt) {
                Text("Pass started \(started, style: .relative)")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
            if let success = DisplayFormat.date(report.lastSuccessAt) {
                Text("Last successful pass \(success, style: .relative)")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
        }
        .padding(12)
        .background(.quaternary.opacity(0.5), in: RoundedRectangle(cornerRadius: 10))
    }
}

private struct Metric: View {
    let title: String
    let value: String

    var body: some View {
        VStack(alignment: .leading, spacing: 2) {
            Text(title)
                .font(.caption2)
                .foregroundStyle(.secondary)
            Text(value)
                .font(.caption.weight(.semibold))
                .lineLimit(1)
                .minimumScaleFactor(0.75)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding(.horizontal, 8)
    }
}

private struct CleanerSummary: View {
    let report: CleanupReport

    private var cleaners: [(String, CleanerReport)] {
        report.cleaners.namedReports
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text("Cleaners")
                .font(.subheadline.weight(.semibold))
            ForEach(cleaners, id: \.0) { item in
                let (name, cleaner) = item
                HStack {
                    VStack(alignment: .leading, spacing: 1) {
                        Text(name)
                            .font(.caption.weight(.medium))
                        Text("\(cleaner.scannedItems) scanned · \(cleaner.eligibleItems) eligible")
                            .font(.caption2)
                            .foregroundStyle(.secondary)
                    }
                    Spacer()
                    VStack(alignment: .trailing, spacing: 1) {
                        Text("\(cleaner.deletedItems) deleted")
                            .font(.caption.weight(.medium))
                        Text(DisplayFormat.bytes(cleaner.actualFreeDeltaBytes))
                            .font(.caption2)
                            .foregroundStyle(.secondary)
                    }
                }
            }
        }
    }
}

private struct ErrorBanner: View {
    let message: String

    var body: some View {
        Label {
            Text(message)
                .fixedSize(horizontal: false, vertical: true)
        } icon: {
            Image(systemName: "wifi.exclamationmark")
        }
        .font(.caption)
        .foregroundStyle(.red)
        .padding(10)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(.red.opacity(0.08), in: RoundedRectangle(cornerRadius: 9))
    }
}

private struct LoadingView: View {
    var body: some View {
        VStack(spacing: 10) {
            ProgressView()
            Text("Loading cleanup status…")
                .font(.caption)
                .foregroundStyle(.secondary)
        }
        .frame(maxWidth: .infinity)
        .padding(.vertical, 60)
    }
}

private struct EmptyViewContent: View {
    var body: some View {
        ContentUnavailableView(
            "No Cleanup Report",
            systemImage: "externaldrive.badge.questionmark",
            description: Text("Refresh after the Stado dashboard is available.")
        )
        .padding(.vertical, 35)
    }
}
