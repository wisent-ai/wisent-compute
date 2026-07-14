import SwiftUI

struct OverviewView: View {
    let snapshot: DashboardSnapshot
    let lastUpdated: Date?

    private let metricColumns = [
        GridItem(.adaptive(minimum: StadoTheme.Layout.metricMinimumWidth), spacing: StadoTheme.Space.sm),
    ]

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: StadoTheme.Space.lg) {
                header

                LazyVGrid(columns: metricColumns, alignment: .leading, spacing: StadoTheme.Space.sm) {
                    MetricCard(
                        title: "Queued",
                        value: snapshot.counts.queue.formatted(),
                        detail: "Jobs waiting for capacity",
                        symbol: "clock",
                        tone: snapshot.counts.queue > 0 ? .warning : .neutral
                    )
                    MetricCard(
                        title: "Running",
                        value: snapshot.counts.running.formatted(),
                        detail: "Jobs currently executing",
                        symbol: "play.circle.fill",
                        tone: snapshot.counts.running > 0 ? .healthy : .neutral
                    )
                    MetricCard(
                        title: "Live workers",
                        value: snapshot.liveAgents.count.formatted(),
                        detail: snapshot.staleAgents.isEmpty ? "No stale worker reports" : "\(snapshot.staleAgents.count) stale reports",
                        symbol: "server.rack",
                        tone: snapshot.liveAgents.isEmpty ? .warning : .healthy
                    )
                    MetricCard(
                        title: "Free slots",
                        value: snapshot.throughput.liveTotalFreeSlots.formatted(),
                        detail: "Reported by live workers",
                        symbol: "gauge.with.dots.needle.50percent",
                        tone: snapshot.throughput.liveTotalFreeSlots > 0 ? .healthy : .neutral
                    )
                }

                HStack(alignment: .top, spacing: StadoTheme.Space.md) {
                    capacityCard
                    throughputCard
                }

                recentActivity
            }
            .frame(maxWidth: StadoTheme.Layout.contentMaximumWidth, alignment: .leading)
            .frame(maxWidth: .infinity, alignment: .topLeading)
            .padding(StadoTheme.Space.lg)
        }
    }

    private var header: some View {
        HStack(alignment: .firstTextBaseline) {
            VStack(alignment: .leading, spacing: StadoTheme.Space.xxs) {
                Text("Operations overview")
                    .font(.largeTitle.weight(.semibold))
                Text(sourceDescription)
                    .font(.subheadline)
                    .foregroundStyle(.secondary)
            }
            Spacer()
            StatusPill(label: "State available", tone: .healthy)
        }
    }

    private var sourceDescription: String {
        let source = snapshot.bucket.map { "Queue source: \($0)" } ?? "Queue source unavailable"
        guard let lastUpdated else { return source }
        return "\(source) · refreshed \(lastUpdated.formatted(.relative(presentation: .named)))"
    }

    private var capacityCard: some View {
        StadoCard {
            VStack(alignment: .leading, spacing: StadoTheme.Space.sm) {
                Label("Capacity", systemImage: "memorychip")
                    .font(.headline)

                if let memory = aggregateMemory {
                    HStack(alignment: .firstTextBaseline) {
                        Text("GPU memory allocated")
                            .font(.subheadline)
                        Spacer()
                        Text("\(StadoFormat.decimal(memory.used)) / \(StadoFormat.decimal(memory.total)) GB")
                            .font(.caption.monospacedDigit())
                            .foregroundStyle(.secondary)
                    }
                    ProgressView(value: memory.used, total: memory.total)
                        .accessibilityLabel("GPU memory allocated")
                        .accessibilityValue("\(StadoFormat.decimal(memory.used)) of \(StadoFormat.decimal(memory.total)) gigabytes")
                    Text("Calculated only from live workers that publish total and free VRAM.")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                } else {
                    UnavailableNotice(
                        title: "Utilization unavailable",
                        detail: "Live capacity reports do not currently include both total and free VRAM. Slot totals and busy-slot counts are not published by this state interface."
                    )
                }
            }
        }
        .frame(maxWidth: .infinity, alignment: .top)
    }

    private var throughputCard: some View {
        StadoCard {
            VStack(alignment: .leading, spacing: StadoTheme.Space.sm) {
                Label("Throughput", systemImage: "chart.line.uptrend.xyaxis")
                    .font(.headline)

                LabeledContent("Average completion", value: StadoFormat.duration(snapshot.throughput.averageWallSecondsPerCompletedJob))
                LabeledContent("Samples", value: snapshot.throughput.samples.formatted())

                if let projection = snapshot.throughput.projectedRemainingSeconds {
                    LabeledContent("Queue projection", value: StadoFormat.duration(projection))
                    Text("Projection uses observed completion time, queue depth, and currently free slots.")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                } else {
                    UnavailableNotice(
                        title: "Queue projection unavailable",
                        detail: "A projection requires completed-job timing samples and at least one free live-worker slot."
                    )
                }
            }
        }
        .frame(maxWidth: .infinity, alignment: .top)
    }

    private var recentActivity: some View {
        StadoCard {
            VStack(alignment: .leading, spacing: StadoTheme.Space.sm) {
                HStack {
                    Label("Recent outcomes", systemImage: "clock.arrow.circlepath")
                        .font(.headline)
                    Spacer()
                    Text("Published by dashboard")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }

                if snapshot.completedRecent.isEmpty && snapshot.recentFailed.isEmpty {
                    UnavailableNotice(
                        title: "No recent outcomes",
                        detail: "The dashboard has not published recent completed or failed jobs."
                    )
                } else {
                    ForEach(Array(snapshot.recentFailed.prefix(3).enumerated()), id: \.offset) { _, job in
                        OutcomeRow(
                            symbol: "xmark.circle.fill",
                            tone: .critical,
                            title: "Job \(job.jobID) failed",
                            detail: job.task ?? job.model ?? "Task details unavailable",
                            date: nil
                        )
                    }
                    ForEach(Array(snapshot.completedRecent.prefix(3).enumerated()), id: \.offset) { _, job in
                        OutcomeRow(
                            symbol: "checkmark.circle.fill",
                            tone: .healthy,
                            title: "Job \(job.jobID) completed",
                            detail: job.task ?? job.model ?? "Task details unavailable",
                            date: StadoFormat.date(job.completedAt)
                        )
                    }
                }
            }
        }
    }

    private var aggregateMemory: (used: Double, total: Double)? {
        let reports = snapshot.liveAgents.compactMap { worker -> (Double, Double)? in
            guard let total = worker.totalVRAMGB,
                  let free = worker.freeVRAMGB,
                  total > 0,
                  free >= 0
            else { return nil }
            return (max(0, total - min(free, total)), total)
        }
        guard !reports.isEmpty else { return nil }
        return (
            reports.reduce(0) { $0 + $1.0 },
            reports.reduce(0) { $0 + $1.1 }
        )
    }
}

private struct OutcomeRow: View {
    let symbol: String
    let tone: StatusTone
    let title: String
    let detail: String
    let date: Date?

    var body: some View {
        HStack(spacing: StadoTheme.Space.sm) {
            Image(systemName: symbol)
                .foregroundStyle(tone.color)
                .accessibilityHidden(true)
            VStack(alignment: .leading, spacing: StadoTheme.Space.xxs) {
                Text(title)
                    .font(.subheadline.weight(.medium))
                Text(detail)
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
            }
            Spacer()
            if let date {
                Text(date, style: .relative)
                    .font(.caption)
                    .foregroundStyle(.secondary)
            } else {
                Text("Time unavailable")
                    .font(.caption)
                    .foregroundStyle(.tertiary)
            }
        }
        .accessibilityElement(children: .combine)
    }
}
