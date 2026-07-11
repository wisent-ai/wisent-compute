import SwiftUI

@main
struct StadoApp: App {
    @StateObject private var store = CleanupStore()

    var body: some Scene {
        MenuBarExtra {
            CleanupMenuView(store: store)
        } label: {
            Label("Stado", systemImage: menuBarSymbol)
        }
        .menuBarExtraStyle(.window)

        Settings {
            SettingsView(store: store)
        }
    }

    private var menuBarSymbol: String {
        guard let report = store.report else {
            return store.errorMessage == nil
                ? "externaldrive.badge.questionmark"
                : "externaldrive.fill.badge.exclamationmark"
        }
        switch report.outcomePresentation.severity {
        case .healthy:
            return "externaldrive.fill.badge.checkmark"
        case .neutral:
            return "externaldrive.fill"
        case .warning, .critical:
            return "externaldrive.fill.badge.exclamationmark"
        }
    }
}
