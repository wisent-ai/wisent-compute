import Combine
import Foundation

@MainActor
final class OperationsStore: ObservableObject {
    @Published private(set) var snapshot: DashboardSnapshot?
    @Published private(set) var isRefreshing = false
    @Published private(set) var errorMessage: String?
    @Published private(set) var lastUpdated: Date?
    @Published private(set) var dashboardURLString: String

    private static let dashboardURLKey = "dashboardBaseURL"

    private let defaults: UserDefaults
    private let client: OperationsClient
    private var requestGeneration = 0

    init(defaults: UserDefaults = .standard, client: OperationsClient = OperationsClient()) {
        self.defaults = defaults
        self.client = client
        dashboardURLString = defaults.string(forKey: Self.dashboardURLKey) ?? OperationsDashboardAddress.localDefault
    }

    var dashboardAddress: OperationsDashboardAddress? {
        try? OperationsDashboardAddress(dashboardURLString)
    }

    var isShowingStaleSnapshot: Bool {
        snapshot != nil && errorMessage != nil
    }

    func refresh() async {
        guard !isRefreshing else { return }
        guard let address = dashboardAddress else {
            errorMessage = OperationsClientError.invalidDashboardURL.localizedDescription
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
            let newSnapshot = try await client.fetchState(from: address)
            guard requestGeneration == generation, !Task.isCancelled else { return }
            snapshot = newSnapshot
            lastUpdated = Date()
            errorMessage = nil
        } catch is CancellationError {
            return
        } catch let error as URLError where error.code == .cancelled {
            return
        } catch {
            guard requestGeneration == generation else { return }
            errorMessage = Self.displayMessage(for: error)
        }
    }

    func saveDashboardURL(_ value: String) throws {
        let address = try OperationsDashboardAddress(value)
        requestGeneration &+= 1
        dashboardURLString = address.displayString
        defaults.set(address.displayString, forKey: Self.dashboardURLKey)
        snapshot = nil
        lastUpdated = nil
        errorMessage = nil
        isRefreshing = false
        Task { await refresh() }
    }

    private static func displayMessage(for error: Error) -> String {
        if let urlError = error as? URLError {
            switch urlError.code {
            case .cannotConnectToHost, .cannotFindHost, .dnsLookupFailed, .networkConnectionLost, .notConnectedToInternet, .timedOut:
                return "The Stado dashboard could not be reached. Start the local dashboard or update the endpoint in Settings."
            default:
                return "The Stado dashboard request failed."
            }
        }
        if let localized = error as? LocalizedError, let description = localized.errorDescription {
            return description
        }
        return "The Stado dashboard request failed."
    }
}
