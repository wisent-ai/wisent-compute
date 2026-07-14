import SwiftUI

enum ConsoleSection: String, CaseIterable, Identifiable {
    case overview
    case workers
    case jobs
    case events

    var id: Self { self }

    var title: String {
        switch self {
        case .overview: "Overview"
        case .workers: "Workers"
        case .jobs: "Jobs"
        case .events: "Events"
        }
    }

    var symbol: String {
        switch self {
        case .overview: "rectangle.3.group"
        case .workers: "server.rack"
        case .jobs: "list.bullet.rectangle"
        case .events: "waveform.path.ecg"
        }
    }
}

struct ConsoleView: View {
    @ObservedObject var store: OperationsStore
    @State private var selection: ConsoleSection? = .overview

    var body: some View {
        NavigationSplitView {
            List(ConsoleSection.allCases, selection: $selection) { section in
                NavigationLink(value: section) {
                    Label(section.title, systemImage: section.symbol)
                }
                .accessibilityLabel(section.title)
            }
            .navigationTitle("Stado")
            .safeAreaInset(edge: .bottom) {
                sourceFooter
            }
            .navigationSplitViewColumnWidth(
                min: StadoTheme.Layout.sidebarMinimum,
                ideal: StadoTheme.Layout.sidebarIdeal,
                max: StadoTheme.Layout.sidebarMaximum
            )
        } detail: {
            detail
                .navigationTitle((selection ?? .overview).title)
                .toolbar {
                    ToolbarItemGroup(placement: .primaryAction) {
                        SettingsLink {
                            Label("Settings", systemImage: "gearshape")
                        }
                        .help("Configure the Stado dashboard endpoint")

                        Button {
                            Task { await store.refresh() }
                        } label: {
                            if store.isRefreshing {
                                ProgressView()
                                    .controlSize(.small)
                                    .accessibilityLabel("Refreshing Stado state")
                            } else {
                                Label("Refresh", systemImage: "arrow.clockwise")
                            }
                        }
                        .help("Refresh Stado state")
                        .keyboardShortcut("r", modifiers: .command)
                        .disabled(store.isRefreshing)
                    }
                }
        }
        .frame(
            minWidth: StadoTheme.Layout.windowMinimumWidth,
            minHeight: StadoTheme.Layout.windowMinimumHeight
        )
        .task {
            if store.snapshot == nil {
                await store.refresh()
            }
        }
    }

    @ViewBuilder
    private var detail: some View {
        VStack(spacing: 0) {
            if let error = store.errorMessage {
                ErrorBanner(message: error, isStale: store.isShowingStaleSnapshot)
                    .padding(.horizontal, StadoTheme.Space.lg)
                    .padding(.top, StadoTheme.Space.md)
            }

            if let snapshot = store.snapshot {
                if snapshot.ready {
                    selectedContent(snapshot)
                } else {
                    ContentUnavailableView {
                        Label("Dashboard is preparing state", systemImage: "hourglass")
                    } description: {
                        Text("The endpoint is reachable, but its first queue snapshot is not ready yet. Refresh after the dashboard completes its background scan.")
                    }
                    .frame(minHeight: StadoTheme.Layout.emptyStateMinimumHeight)
                }
            } else if store.isRefreshing {
                VStack(spacing: StadoTheme.Space.sm) {
                    ProgressView()
                        .controlSize(.large)
                    Text("Loading local Stado state…")
                        .foregroundStyle(.secondary)
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity)
                .accessibilityElement(children: .combine)
            } else {
                ContentUnavailableView {
                    Label("Stado state unavailable", systemImage: "network.slash")
                } description: {
                    Text("Start the Stado dashboard or configure its HTTPS endpoint in Settings. No operational data is fabricated while the source is unavailable.")
                } actions: {
                    Button("Refresh") {
                        Task { await store.refresh() }
                    }
                }
                .frame(minHeight: StadoTheme.Layout.emptyStateMinimumHeight)
            }
        }
    }

    @ViewBuilder
    private func selectedContent(_ snapshot: DashboardSnapshot) -> some View {
        switch selection ?? .overview {
        case .overview:
            OverviewView(snapshot: snapshot, lastUpdated: store.lastUpdated)
        case .workers:
            WorkersView(snapshot: snapshot)
        case .jobs:
            JobsView(snapshot: snapshot)
        case .events:
            EventsView(snapshot: snapshot)
        }
    }

    private var sourceFooter: some View {
        VStack(alignment: .leading, spacing: StadoTheme.Space.xs) {
            Divider()
            HStack(spacing: StadoTheme.Space.xs) {
                Circle()
                    .fill(sourceTone.color)
                    .frame(width: StadoTheme.Layout.statusDot, height: StadoTheme.Layout.statusDot)
                    .accessibilityHidden(true)
                VStack(alignment: .leading, spacing: StadoTheme.Space.xxs) {
                    Text(sourceLabel)
                        .font(.caption.weight(.semibold))
                    Text(store.dashboardAddress?.baseURL.host() ?? "Endpoint unavailable")
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                        .lineLimit(1)
                }
            }
            .accessibilityElement(children: .combine)
        }
        .padding(StadoTheme.Space.sm)
        .background(.bar)
    }

    private var sourceTone: StatusTone {
        if store.errorMessage != nil { return .critical }
        if store.snapshot?.ready == true { return .healthy }
        return .neutral
    }

    private var sourceLabel: String {
        if store.errorMessage != nil { return store.snapshot == nil ? "Disconnected" : "Refresh failed" }
        if store.snapshot?.ready == true { return "Dashboard connected" }
        return "Waiting for dashboard"
    }
}

private struct ErrorBanner: View {
    let message: String
    let isStale: Bool

    var body: some View {
        HStack(alignment: .top, spacing: StadoTheme.Space.sm) {
            Image(systemName: "exclamationmark.triangle.fill")
                .foregroundStyle(.red)
                .accessibilityHidden(true)
            VStack(alignment: .leading, spacing: StadoTheme.Space.xxs) {
                Text(isStale ? "Refresh failed — showing the last snapshot" : "State unavailable")
                    .font(.subheadline.weight(.semibold))
                Text(message)
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
            Spacer(minLength: 0)
        }
        .padding(StadoTheme.Space.sm)
        .background(Color.red.opacity(0.08), in: RoundedRectangle(cornerRadius: StadoTheme.Radius.small))
        .overlay {
            RoundedRectangle(cornerRadius: StadoTheme.Radius.small)
                .stroke(Color.red.opacity(0.25), lineWidth: 1)
        }
        .accessibilityElement(children: .combine)
    }
}
