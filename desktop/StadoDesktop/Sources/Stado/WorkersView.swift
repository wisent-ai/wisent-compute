import SwiftUI

struct WorkersView: View {
    let snapshot: DashboardSnapshot
    @State private var searchText = ""

    private var liveWorkers: [WorkerNode] {
        filter(snapshot.liveAgents)
    }

    private var staleWorkers: [WorkerNode] {
        filter(snapshot.staleAgents)
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            header
                .padding(StadoTheme.Space.lg)

            if snapshot.liveAgents.isEmpty && snapshot.staleAgents.isEmpty {
                ContentUnavailableView {
                    Label("No worker reports", systemImage: "server.rack")
                } description: {
                    Text("No capacity beacons were discovered in the current dashboard snapshot. Start a worker capacity publisher to populate this view.")
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity)
            } else if liveWorkers.isEmpty && staleWorkers.isEmpty {
                ContentUnavailableView.search(text: searchText)
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
            } else {
                List {
                    if !liveWorkers.isEmpty {
                        Section("Live · \(liveWorkers.count)") {
                            ForEach(Array(liveWorkers.enumerated()), id: \.offset) { _, worker in
                                WorkerRow(worker: worker, isLive: true)
                            }
                        }
                    }
                    if !staleWorkers.isEmpty {
                        Section("Stale · \(staleWorkers.count)") {
                            ForEach(Array(staleWorkers.enumerated()), id: \.offset) { _, worker in
                                WorkerRow(worker: worker, isLive: false)
                            }
                        }
                    }
                }
                .listStyle(.inset)
            }
        }
        .searchable(text: $searchText, prompt: "Search workers, kinds, or slot types")
    }

    private var header: some View {
        HStack(alignment: .firstTextBaseline) {
            VStack(alignment: .leading, spacing: StadoTheme.Space.xxs) {
                Text("Workers and nodes")
                    .font(.largeTitle.weight(.semibold))
                Text("Capacity reports discovered by the Stado dashboard")
                    .font(.subheadline)
                    .foregroundStyle(.secondary)
            }
            Spacer()
            StatusPill(
                label: "\(snapshot.liveAgents.count) live",
                tone: snapshot.liveAgents.isEmpty ? .warning : .healthy
            )
        }
    }

    private func filter(_ workers: [WorkerNode]) -> [WorkerNode] {
        let query = searchText.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !query.isEmpty else { return workers }
        return workers.filter { worker in
            [worker.consumerID, worker.kind]
                .compactMap { $0 }
                .contains { $0.localizedCaseInsensitiveContains(query) }
                || worker.freeSlots.keys.contains { $0.localizedCaseInsensitiveContains(query) }
        }
    }
}

private struct WorkerRow: View {
    let worker: WorkerNode
    let isLive: Bool

    var body: some View {
        VStack(alignment: .leading, spacing: StadoTheme.Space.sm) {
            HStack(alignment: .firstTextBaseline) {
                VStack(alignment: .leading, spacing: StadoTheme.Space.xxs) {
                    HStack(spacing: StadoTheme.Space.xs) {
                        Text(worker.displayName)
                            .font(.headline)
                            .textSelection(.enabled)
                        StatusPill(label: isLive ? "Live" : "Stale", tone: isLive ? .healthy : .warning)
                    }
                    Text(worker.kind?.humanizedIdentifier ?? "Worker kind unavailable")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
                Spacer()
                VStack(alignment: .trailing, spacing: StadoTheme.Space.xxs) {
                    Text("\(worker.availableSlots) free slots")
                        .font(.subheadline.weight(.semibold))
                        .monospacedDigit()
                    Text(ageLabel)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
            }

            if worker.freeSlots.isEmpty {
                Text("Slot breakdown unavailable")
                    .font(.caption)
                    .foregroundStyle(.tertiary)
            } else {
                ScrollView(.horizontal, showsIndicators: false) {
                    HStack(spacing: StadoTheme.Space.xs) {
                        ForEach(worker.freeSlots.keys.sorted(), id: \.self) { slotType in
                            Text("\(slotType): \(worker.freeSlots[slotType, default: 0])")
                                .font(.caption.monospacedDigit())
                                .padding(.horizontal, StadoTheme.Space.xs)
                                .padding(.vertical, StadoTheme.Space.xxs)
                                .background(.quaternary, in: Capsule())
                        }
                    }
                }
                .accessibilityLabel("Free slot breakdown")
            }

            if let total = worker.totalVRAMGB,
               let free = worker.freeVRAMGB,
               total > 0,
               free >= 0 {
                let used = max(0, total - min(free, total))
                HStack(spacing: StadoTheme.Space.sm) {
                    ProgressView(value: used, total: total)
                        .frame(width: StadoTheme.Layout.progressWidth)
                    Text("\(StadoFormat.decimal(used)) of \(StadoFormat.decimal(total)) GB VRAM allocated")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
                .accessibilityElement(children: .combine)
            } else {
                Text("VRAM utilization unavailable")
                    .font(.caption)
                    .foregroundStyle(.tertiary)
            }
        }
        .padding(.vertical, StadoTheme.Space.xs)
        .accessibilityElement(children: .contain)
    }

    private var ageLabel: String {
        if let age = worker.ageSeconds {
            return "Reported \(StadoFormat.duration(age)) ago"
        }
        if let date = StadoFormat.date(worker.publishedAt) {
            return "Reported \(date.formatted(.relative(presentation: .named)))"
        }
        return "Report time unavailable"
    }
}

