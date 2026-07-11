import Foundation
import Combine

@MainActor
final class CleanupStore: ObservableObject {
    @Published private(set) var response: CleanupResponse?
    @Published private(set) var isRefreshing = false
    @Published private(set) var isRunningCleanup = false
    @Published private(set) var errorMessage: String?
    @Published private(set) var lastUpdated: Date?
    @Published private(set) var dashboardURLString: String

    private let client: CleanupClient
    private let defaults: UserDefaults
    private var pollingTask: Task<Void, Never>?
    private var requestGeneration = 0

    private static let dashboardURLKey = "dashboardBaseURL"

    init(
        defaults: UserDefaults = .standard,
        client: CleanupClient = CleanupClient(),
        startsPolling: Bool = true
    ) {
        self.defaults = defaults
        self.client = client
        dashboardURLString = defaults.string(forKey: Self.dashboardURLKey)
            ?? DashboardAddress.defaultString

        guard startsPolling else { return }
        pollingTask = Task { [weak self] in
            await self?.refresh()
            while !Task.isCancelled {
                do {
                    try await Task.sleep(for: .seconds(60))
                } catch {
                    return
                }
                await self?.refresh()
            }
        }
    }

    deinit {
        pollingTask?.cancel()
    }

    var report: CleanupReport? { response?.report }

    var dashboardAddress: DashboardAddress? {
        try? DashboardAddress(dashboardURLString)
    }

    func refresh() async {
        guard !isRefreshing, !isRunningCleanup else { return }
        guard let address = dashboardAddress else {
            errorMessage = CleanupClientError.invalidDashboardURL.localizedDescription
            return
        }
        let generation = requestGeneration

        isRefreshing = true
        defer {
            if requestGeneration == generation {
                isRefreshing = false
            }
        }
        do {
            let response = try await client.currentReport(at: address)
            guard requestGeneration == generation else { return }
            apply(response)
        } catch {
            guard requestGeneration == generation else { return }
            errorMessage = Self.displayMessage(for: error)
        }
    }

    func runCleanup() async {
        guard !isRunningCleanup, !isRefreshing else { return }
        guard let address = dashboardAddress else {
            errorMessage = CleanupClientError.invalidDashboardURL.localizedDescription
            return
        }
        let generation = requestGeneration

        isRunningCleanup = true
        defer {
            if requestGeneration == generation {
                isRunningCleanup = false
            }
        }
        do {
            let response = try await client.runCleanup(at: address)
            guard requestGeneration == generation else { return }
            apply(response)
        } catch {
            guard requestGeneration == generation else { return }
            errorMessage = Self.displayMessage(for: error)
        }
    }

    func saveDashboardURL(_ value: String) throws {
        let address = try DashboardAddress(value)
        requestGeneration &+= 1
        isRefreshing = false
        isRunningCleanup = false
        response = nil
        lastUpdated = nil
        dashboardURLString = address.displayString
        defaults.set(dashboardURLString, forKey: Self.dashboardURLKey)
        errorMessage = nil
        Task { await refresh() }
    }

    private func apply(_ response: CleanupResponse) {
        self.response = response
        lastUpdated = Date()
        errorMessage = response.service == "error"
            ? "The cleanup service returned a sanitized error response."
            : nil
    }

    private static func displayMessage(for error: Error) -> String {
        if error is URLError {
            return "The dashboard could not be reached."
        }
        if let localized = error as? LocalizedError,
           let message = localized.errorDescription {
            return message
        }
        return "The dashboard could not be reached."
    }
}
