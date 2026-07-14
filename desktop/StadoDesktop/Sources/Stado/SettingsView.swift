import SwiftUI

struct SettingsView: View {
    @ObservedObject var operationsStore: OperationsStore
    @ObservedObject var cleanupStore: CleanupStore
    @Environment(\.dismiss) private var dismiss
    @State private var draftURL = ""
    @State private var validationMessage: String?

    var body: some View {
        Form {
            Section("Dashboard state source") {
                TextField("Base URL", text: $draftURL, prompt: Text(OperationsDashboardAddress.localDefault))
                    .textFieldStyle(.roundedBorder)
                    .accessibilityLabel("Stado dashboard base URL")
                    .onSubmit(save)

                Text("Stado reads the existing /api/state.json interface. Plain HTTP is accepted only for IPv4 and IPv6 loopback addresses; remote sources require HTTPS. URLs containing credentials, query parameters, or fragments are rejected.")
                    .font(.caption)
                    .foregroundStyle(.secondary)

                if let validationMessage {
                    Label(validationMessage, systemImage: "exclamationmark.triangle.fill")
                        .font(.caption)
                        .foregroundStyle(.red)
                        .accessibilityLabel("Invalid dashboard URL: \(validationMessage)")
                }
            }

            Section("Privacy") {
                Label("The app uses an ephemeral URL session and does not request or persist dashboard credentials.", systemImage: "lock.shield")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }

            HStack {
                Button("Use Local Default") {
                    draftURL = OperationsDashboardAddress.localDefault
                    validationMessage = nil
                }
                Spacer()
                Button("Cancel") {
                    dismiss()
                }
                Button("Save") {
                    save()
                }
                .buttonStyle(.borderedProminent)
            }
        }
        .formStyle(.grouped)
        .frame(width: 520)
        .fixedSize(horizontal: false, vertical: true)
        .onAppear {
            draftURL = operationsStore.dashboardURLString
        }
    }

    private func save() {
        do {
            try operationsStore.saveDashboardURL(draftURL)
            try cleanupStore.saveDashboardURL(draftURL)
            validationMessage = nil
            dismiss()
        } catch {
            validationMessage = (error as? LocalizedError)?.errorDescription ?? "Enter a valid dashboard URL."
        }
    }
}
