import SwiftUI

struct EventsView: View {
    let snapshot: DashboardSnapshot
    @State private var searchText = ""

    private var completed: [CompletedJob] {
        snapshot.completedRecent.filter { matches([$0.jobID, $0.model, $0.task]) }
    }

    private var failed: [FailedJob] {
        snapshot.recentFailed.filter { matches([$0.jobID, $0.model, $0.task, $0.error]) }
    }

    private var hasSearchResults: Bool {
        !completed.isEmpty || !failed.isEmpty
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            header
                .padding(StadoTheme.Space.lg)

            if !searchText.isEmpty && !hasSearchResults {
                ContentUnavailableView.search(text: searchText)
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
            } else {
                List {
                    Section {
                        UnavailableNotice(
                            title: "Dedicated log stream unavailable",
                            detail: "The established state endpoint publishes recent job outcomes, not coordinator or worker log lines. This view never substitutes synthetic logs or reads local secret-bearing files.",
                            symbol: "doc.text.magnifyingglass"
                        )
                    }

                    Section("Failures · \(failed.count)") {
                        if failed.isEmpty {
                            EmptyListRow(
                                title: "No recent failure events",
                                detail: "No sanitized failure outcomes were published in this snapshot."
                            )
                        } else {
                            ForEach(Array(failed.enumerated()), id: \.offset) { _, job in
                                EventRow(
                                    symbol: "exclamationmark.triangle.fill",
                                    tone: .critical,
                                    title: "Job \(job.jobID) failed",
                                    context: [job.task, job.model].compactMap { $0 }.joined(separator: " · "),
                                    message: job.error,
                                    date: nil
                                )
                            }
                        }
                    }

                    Section("Completions · \(completed.count)") {
                        if completed.isEmpty {
                            EmptyListRow(
                                title: "No recent completion events",
                                detail: "No completed outcomes were published in this snapshot."
                            )
                        } else {
                            ForEach(Array(completed.enumerated()), id: \.offset) { _, job in
                                EventRow(
                                    symbol: "checkmark.circle.fill",
                                    tone: .healthy,
                                    title: "Job \(job.jobID) completed",
                                    context: [job.task, job.model].compactMap { $0 }.joined(separator: " · "),
                                    message: job.wallSeconds.map { "Wall time: \(StadoFormat.duration($0))" },
                                    date: StadoFormat.date(job.completedAt)
                                )
                            }
                        }
                    }
                }
                .listStyle(.inset)
            }
        }
        .searchable(text: $searchText, prompt: "Search outcomes, models, tasks, or errors")
    }

    private var header: some View {
        HStack(alignment: .firstTextBaseline) {
            VStack(alignment: .leading, spacing: StadoTheme.Space.xxs) {
                Text("Operational events")
                    .font(.largeTitle.weight(.semibold))
                Text("Recent queue outcomes exposed by the dashboard state interface")
                    .font(.subheadline)
                    .foregroundStyle(.secondary)
            }
            Spacer()
            StatusPill(
                label: "\(snapshot.completedRecent.count + snapshot.recentFailed.count) outcomes",
                tone: snapshot.recentFailed.isEmpty ? .neutral : .warning
            )
        }
    }

    private func matches(_ values: [String?]) -> Bool {
        let query = searchText.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !query.isEmpty else { return true }
        return values.compactMap { $0 }.contains { $0.localizedCaseInsensitiveContains(query) }
    }
}

private struct EventRow: View {
    let symbol: String
    let tone: StatusTone
    let title: String
    let context: String
    let message: String?
    let date: Date?

    var body: some View {
        HStack(alignment: .top, spacing: StadoTheme.Space.md) {
            Image(systemName: symbol)
                .foregroundStyle(tone.color)
                .accessibilityHidden(true)
            VStack(alignment: .leading, spacing: StadoTheme.Space.xxs) {
                Text(title)
                    .font(.subheadline.weight(.semibold))
                    .textSelection(.enabled)
                Text(context.isEmpty ? "Task and model unavailable" : context)
                    .font(.caption)
                    .foregroundStyle(.secondary)
                if let message, !message.isEmpty {
                    Text(message)
                        .font(.caption.monospaced())
                        .foregroundStyle(tone == .critical ? .red : .secondary)
                        .textSelection(.enabled)
                } else if tone == .critical {
                    Text("Sanitized error unavailable")
                        .font(.caption)
                        .foregroundStyle(.tertiary)
                }
            }
            Spacer()
            if let date {
                Text(date, style: .relative)
                    .font(.caption)
                    .foregroundStyle(.secondary)
            } else {
                Text("Timestamp unavailable")
                    .font(.caption)
                    .foregroundStyle(.tertiary)
            }
        }
        .padding(.vertical, StadoTheme.Space.xs)
        .accessibilityElement(children: .contain)
    }
}
