import SwiftUI

@main
struct StadoApp: App {
    @StateObject private var operationsStore = OperationsStore()
    @StateObject private var cleanupStore = CleanupStore()

    var body: some Scene {
        WindowGroup("Stado Operations Console") {
            ConsoleView(store: operationsStore)
        }
        .defaultSize(
            width: StadoTheme.Layout.windowMinimumWidth,
            height: StadoTheme.Layout.windowMinimumHeight
        )
        .windowResizability(.contentMinSize)

        MenuBarExtra {
            CleanupMenuView(store: cleanupStore)
        } label: {
            Label("Stado", systemImage: menuBarSymbol)
        }
        .menuBarExtraStyle(.window)

        Settings {
            SettingsView(operationsStore: operationsStore, cleanupStore: cleanupStore)
        }
    }

    private var menuBarSymbol: String {
        guard let report = cleanupStore.report else {
            return cleanupStore.errorMessage == nil
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
