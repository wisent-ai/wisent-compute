import SwiftUI

struct JobsView: View {
    let snapshot: DashboardSnapshot
    @State private var searchText = ""

    private var currentWork: [(model: String, counts: JobCounts)] {
        snapshot.byModelState
            .filter { model, counts in
                (counts.queue > 0 || counts.running > 0) && matches([model])
            }
            .map { (model: $0.key, counts: $0.value) }
            .sorted {
                let lhsActive = $0.counts.running + $0.counts.queue
                let rhsActive = $1.counts.running + $1.counts.queue
                return lhsActive == rhsActive ? $0.model < $1.model : lhsActive > rhsActive
            }
    }

    private var completed: [CompletedJob] {
        snapshot.completedRecent.filter { matches([$0.jobID, $0.model, $0.task]) }
    }

    private var failed: [FailedJob] {
        snapshot.recentFailed.filter { matches([$0.jobID, $0.model, $0.task, $0.error]) }
    }

    private var hasSearchResults: Bool {
        !currentWork.isEmpty || !completed.isEmpty || !failed.isEmpty
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
                            title: "Individual current-job details unavailable",
                            detail: "The dashboard state interface publishes queued and running work by model, but does not expose individual current job records."
                        )
                    }

                    Section("Current work by model") {
                        if currentWork.isEmpty {
                            EmptyListRow(
                                title: "No queued or running work",
                                detail: searchText.isEmpty ? "The latest snapshot reports no active model groups." : "No active model groups match the search."
                            )
                        } else {
                            ForEach(currentWork, id: \.model) { item in
                                CurrentWorkRow(model: item.model, counts: item.counts)
                            }
                        }
                    }

                    Section("Recent completed · \(completed.count)") {
                        if completed.isEmpty {
                            EmptyListRow(title: "No recent completions", detail: "The dashboard has not published completed jobs for this view.")
                        } else {
                            ForEach(Array(completed.enumerated()), id: \.offset) { _, job in
                                CompletedJobRow(job: job)
                            }
                        }
                    }

                    Section("Recent failed · \(failed.count)") {
                        if failed.isEmpty {
                            EmptyListRow(title: "No recent failures", detail: "The dashboard has not published failed jobs for this view.")
                        } else {
                            ForEach(Array(failed.enumerated()), id: \.offset) { _, job in
                                FailedJobRow(job: job)
                            }
                        }
                    }
                }
                .listStyle(.inset)
            }
        }
        .searchable(text: $searchText, prompt: "Search job ID, model, task, or failure")
    }

    private var header: some View {
        HStack(alignment: .firstTextBaseline) {
            VStack(alignment: .leading, spacing: StadoTheme.Space.xxs) {
                Text("Jobs")
                    .font(.largeTitle.weight(.semibold))
                Text("Current model-level work and recent queue outcomes")
                    .font(.subheadline)
                    .foregroundStyle(.secondary)
            }
            Spacer()
            HStack(spacing: StadoTheme.Space.xs) {
                StatusPill(label: "\(snapshot.counts.running) running", tone: snapshot.counts.running > 0 ? .healthy : .neutral)
                StatusPill(label: "\(snapshot.counts.queue) queued", tone: snapshot.counts.queue > 0 ? .warning : .neutral)
            }
        }
    }

    private func matches(_ values: [String?]) -> Bool {
        let query = searchText.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !query.isEmpty else { return true }
        return values.compactMap { $0 }.contains { $0.localizedCaseInsensitiveContains(query) }
    }
}

private struct CurrentWorkRow: View {
    let model: String
    let counts: JobCounts

    var body: some View {
        HStack(spacing: StadoTheme.Space.md) {
            Image(systemName: "shippingbox")
                .foregroundStyle(.secondary)
                .accessibilityHidden(true)
            VStack(alignment: .leading, spacing: StadoTheme.Space.xxs) {
                Text(model)
                    .font(.subheadline.weight(.medium))
                    .textSelection(.enabled)
                Text("Model group")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
            Spacer()
            LabeledCount(label: "Running", value: counts.running, tone: .healthy)
            LabeledCount(label: "Queued", value: counts.queue, tone: .warning)
        }
        .padding(.vertical, StadoTheme.Space.xs)
        .accessibilityElement(children: .combine)
    }
}

private struct LabeledCount: View {
    let label: String
    let value: Int
    let tone: StatusTone

    var body: some View {
        VStack(alignment: .trailing, spacing: StadoTheme.Space.xxs) {
            Text(value.formatted())
                .font(.subheadline.weight(.semibold))
                .monospacedDigit()
                .foregroundStyle(value > 0 ? tone.color : .secondary)
            Text(label)
                .font(.caption2)
                .foregroundStyle(.secondary)
        }
        .accessibilityElement(children: .combine)
    }
}

private struct CompletedJobRow: View {
    let job: CompletedJob

    var body: some View {
        HStack(spacing: StadoTheme.Space.md) {
            Image(systemName: "checkmark.circle.fill")
                .foregroundStyle(.green)
                .accessibilityHidden(true)
            JobIdentity(jobID: job.jobID, model: job.model, task: job.task)
            Spacer()
            VStack(alignment: .trailing, spacing: StadoTheme.Space.xxs) {
                Text(StadoFormat.duration(job.wallSeconds))
                    .font(.caption.monospacedDigit())
                if let date = StadoFormat.date(job.completedAt) {
                    Text(date, style: .relative)
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                } else {
                    Text("Completion time unavailable")
                        .font(.caption2)
                        .foregroundStyle(.tertiary)
                }
            }
        }
        .padding(.vertical, StadoTheme.Space.xs)
        .accessibilityElement(children: .contain)
    }
}

private struct FailedJobRow: View {
    let job: FailedJob

    var body: some View {
        HStack(alignment: .top, spacing: StadoTheme.Space.md) {
            Image(systemName: "xmark.circle.fill")
                .foregroundStyle(.red)
                .accessibilityHidden(true)
            JobIdentity(jobID: job.jobID, model: job.model, task: job.task)
            Spacer()
            Text(job.error?.isEmpty == false ? job.error! : "Sanitized error unavailable")
                .font(.caption.monospaced())
                .foregroundStyle(.secondary)
                .lineLimit(3)
                .frame(maxWidth: 360, alignment: .leading)
                .textSelection(.enabled)
        }
        .padding(.vertical, StadoTheme.Space.xs)
        .accessibilityElement(children: .contain)
    }
}

private struct JobIdentity: View {
    let jobID: String
    let model: String?
    let task: String?

    var body: some View {
        VStack(alignment: .leading, spacing: StadoTheme.Space.xxs) {
            Text(jobID)
                .font(.subheadline.weight(.semibold).monospaced())
                .textSelection(.enabled)
            Text([task, model].compactMap { $0 }.joined(separator: " · ").nilIfEmpty ?? "Job details unavailable")
                .font(.caption)
                .foregroundStyle(.secondary)
                .lineLimit(2)
        }
    }
}

struct EmptyListRow: View {
    let title: String
    let detail: String

    var body: some View {
        VStack(alignment: .leading, spacing: StadoTheme.Space.xxs) {
            Text(title)
                .font(.subheadline.weight(.medium))
            Text(detail)
                .font(.caption)
                .foregroundStyle(.secondary)
        }
        .padding(.vertical, StadoTheme.Space.xs)
        .accessibilityElement(children: .combine)
    }
}

private extension String {
    var nilIfEmpty: String? { isEmpty ? nil : self }
}
