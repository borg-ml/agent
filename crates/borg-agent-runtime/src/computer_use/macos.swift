// Borg-owned macOS accessibility worker. JSONL on stdin/stdout; diagnostics on stderr.
// Same contract as linux.py: element handles live only for this process; every
// effect consumes the observation it was issued against.
import AppKit
import ApplicationServices
import Foundation

struct Failure: Error { let message: String }

struct Handle: Hashable {
    let element: AXUIElement
    static func == (a: Handle, b: Handle) -> Bool { CFEqual(a.element, b.element) }
    func hash(into hasher: inout Hasher) { hasher.combine(CFHash(element)) }
}

struct Bounds: Codable, Equatable {
    let x: Double, y: Double, width: Double, height: Double
}

struct Node: Codable, Equatable {
    let id: String
    let parent: String?
    let role: String
    let name: String
    let enabled: Bool
    let focused: Bool
    let showing: Bool
    let bounds: Bounds?
    let text: String?
    let actions: [String]?
}

struct Observation {
    let observationId: String
    let nodes: [String: Node]
    let order: [String]
}

let epoch = String(UUID().uuidString.replacingOccurrences(of: "-", with: "").lowercased().prefix(12))
var objects: [String: AXUIElement] = [:]
var objectIds: [Handle: String] = [:]
var nextId = 0
var observations: [String: Observation] = [:]

func identify(_ element: AXUIElement) throws -> String {
    let handle = Handle(element: element)
    if let id = objectIds[handle] { return id }
    if objects.count >= 10000 { throw Failure(message: "element handle limit reached; restart the desktop session") }
    nextId += 1
    let id = "\(epoch):\(nextId)"
    objects[id] = element
    objectIds[handle] = id
    return id
}

func attribute(_ element: AXUIElement, _ name: String) -> AnyObject? {
    var value: CFTypeRef?
    let status = AXUIElementCopyAttributeValue(element, name as CFString, &value)
    return status == .success ? value : nil
}

func string(_ element: AXUIElement, _ name: String) -> String? {
    attribute(element, name) as? String
}

func boolean(_ element: AXUIElement, _ name: String) -> Bool {
    (attribute(element, name) as? Bool) ?? false
}

func children(_ element: AXUIElement) -> [AXUIElement] {
    (attribute(element, kAXChildrenAttribute) as? [AXUIElement]) ?? []
}

func axValue<T>(_ value: AnyObject?, _ kind: AXValueType, _ zero: T) -> T? {
    guard let value = value, CFGetTypeID(value) == AXValueGetTypeID() else { return nil }
    var out = zero
    return AXValueGetValue(value as! AXValue, kind, &out) ? out : nil
}

func alive(_ element: AXUIElement) -> Bool {
    var value: CFTypeRef?
    return AXUIElementCopyAttributeValue(element, kAXRoleAttribute as CFString, &value) == .success
}

func actionNames(_ element: AXUIElement) -> [String]? {
    var names: CFArray?
    guard AXUIElementCopyActionNames(element, &names) == .success, let names = names as? [String] else { return nil }
    return names
}

struct WindowEntry {
    let id: String
    let element: AXUIElement
    let pid: pid_t
    let title: String
    let application: String
    let active: Bool
}

func windows() throws -> [WindowEntry] {
    var result: [WindowEntry] = []
    for app in NSWorkspace.shared.runningApplications where app.activationPolicy == .regular {
        let axApp = AXUIElementCreateApplication(app.processIdentifier)
        guard let wins = attribute(axApp, kAXWindowsAttribute) as? [AXUIElement] else { continue }
        for win in wins.prefix(256) where alive(win) {
            let id = try identify(win)
            result.append(WindowEntry(
                id: id, element: win, pid: app.processIdentifier,
                title: string(win, kAXTitleAttribute) ?? "",
                application: app.localizedName ?? "",
                active: app.isActive && boolean(win, kAXMainAttribute)))
        }
        if result.count >= 256 { break }
    }
    return result
}

func window(_ id: String) throws -> WindowEntry {
    guard let entry = try windows().first(where: { $0.id == id }) else {
        throw Failure(message: "stale or unknown window_id; list_windows again")
    }
    return entry
}

func describe(_ element: AXUIElement, parent: String?) throws -> Node {
    let role = string(element, kAXRoleAttribute) ?? ""
    var bounds: Bounds?
    if let origin = axValue(attribute(element, kAXPositionAttribute), .cgPoint, CGPoint.zero),
       let size = axValue(attribute(element, kAXSizeAttribute), .cgSize, CGSize.zero),
       size.width > 0, size.height > 0 {
        bounds = Bounds(x: origin.x, y: origin.y, width: size.width, height: size.height)
    }
    var text: String?
    if role != kAXSecureTextFieldRole as String, let value = attribute(element, kAXValueAttribute) as? String {
        text = String(value.prefix(2048))
    }
    let name = string(element, kAXTitleAttribute) ?? string(element, kAXDescriptionAttribute) ?? ""
    let id = try identify(element)
    return Node(
        id: id, parent: parent, role: role, name: String(name.prefix(1024)),
        enabled: (attribute(element, kAXEnabledAttribute) as? Bool) ?? true,
        focused: boolean(element, kAXFocusedAttribute),
        showing: bounds != nil, bounds: bounds, text: text, actions: actionNames(element))
}

func tree(_ root: AXUIElement, limit: Int) throws -> ([String: Node], [String], Bool) {
    var nodes: [String: Node] = [:]
    var order: [String] = []
    var queue: [(AXUIElement, String?, Int)] = [(root, nil, 0)]
    var truncated = false
    var index = 0
    while index < queue.count && nodes.count < limit {
        let (element, parent, depth) = queue[index]
        index += 1
        guard alive(element) else { continue }
        let node = try describe(element, parent: parent)
        nodes[node.id] = node
        order.append(node.id)
        let kids = children(element)
        let pending = queue.count - index
        let budget = depth < 32 ? max(0, limit - nodes.count - pending) : 0
        truncated = truncated || kids.count > budget
        for child in kids.prefix(budget) { queue.append((child, node.id, depth + 1)) }
    }
    return (nodes, order, truncated || index < queue.count)
}

func jsonObject<T: Encodable>(_ value: T) throws -> Any {
    try JSONSerialization.jsonObject(with: JSONEncoder().encode(value))
}

func run(_ command: [String], timeout: Double) throws -> (Int32, Data, String) {
    let process = Process()
    process.executableURL = URL(fileURLWithPath: command[0])
    process.arguments = Array(command.dropFirst())
    let out = Pipe(), err = Pipe()
    process.standardOutput = out
    process.standardError = err
    try process.run()
    let deadline = Date().addingTimeInterval(timeout)
    while process.isRunning && Date() < deadline { Thread.sleep(forTimeInterval: 0.05) }
    if process.isRunning { process.terminate(); throw Failure(message: "\(command[0]) timed out") }
    return (process.terminationStatus, out.fileHandleForReading.readDataToEndOfFile(),
            String(decoding: err.fileHandleForReading.readDataToEndOfFile(), as: UTF8.self))
}

func windowNumber(_ entry: WindowEntry) throws -> CGWindowID {
    guard let list = CGWindowListCopyWindowInfo([.optionOnScreenOnly, .excludeDesktopElements], kCGNullWindowID) as? [[String: Any]] else {
        throw Failure(message: "window list unavailable; grant Screen Recording permission")
    }
    let matches = list.filter { info in
        (info[kCGWindowOwnerPID as String] as? pid_t) == entry.pid
            && (info[kCGWindowLayer as String] as? Int) == 0
            && ((info[kCGWindowName as String] as? String) ?? "") == entry.title
    }
    guard matches.count == 1, let number = matches[0][kCGWindowNumber as String] as? CGWindowID else {
        throw Failure(message: matches.isEmpty
            ? "window is not on screen or its title is unavailable (Screen Recording permission required)"
            : "window title is ambiguous; isolated capture refused")
    }
    return number
}

func screenshot(_ scope: String?, windowId: String?) throws -> [String: Any] {
    let path = NSTemporaryDirectory() + "borg-cua-\(UUID().uuidString).png"
    defer { try? FileManager.default.removeItem(atPath: path) }
    var command = ["/usr/sbin/screencapture", "-x", "-t", "png"]
    let space: String
    switch scope {
    case "desktop":
        space = "screenshot pixels of the whole visible desktop (Retina scaled), not AX screen points"
    case "window":
        guard let windowId = windowId else { throw Failure(message: "window scope requires window_id") }
        let number = try windowNumber(try window(windowId))
        command += ["-l", String(number)]
        space = "screenshot pixels of one window (Retina scaled), not AX screen points"
    default:
        throw Failure(message: "scope must be \"desktop\" or \"window\"")
    }
    command.append(path)
    let (status, _, stderr) = try run(command, timeout: 10)
    guard status == 0, var data = FileManager.default.contents(atPath: path) else {
        throw Failure(message: "capture failed (Screen Recording permission?): " + String(stderr.prefix(1024)))
    }
    var scaled = false
    if data.count > 4 * 1024 * 1024 {
        _ = try run(["/usr/bin/sips", "-Z", "1920", path], timeout: 10)
        guard let smaller = FileManager.default.contents(atPath: path) else { throw Failure(message: "downscale failed") }
        data = smaller
        scaled = true
    }
    guard data.count >= 24, data.prefix(8) == Data([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]) else {
        throw Failure(message: "capture did not return a PNG")
    }
    guard data.count <= 4 * 1024 * 1024 else { throw Failure(message: "screenshot exceeds 4 MiB") }
    let width = data[16..<20].reduce(UInt32(0)) { ($0 << 8) | UInt32($1) }
    let height = data[20..<24].reduce(UInt32(0)) { ($0 << 8) | UInt32($1) }
    return ["scope": scope ?? "", "width": Int(width), "height": Int(height), "downscaled": scaled,
            "coordinate_space": space,
            "borg_attachments": [["media_type": "image/png", "data_base64": data.base64EncodedString()]]]
}

func snapshot(_ args: [String: Any]) throws -> [String: Any] {
    guard let windowId = args["window_id"] as? String else { throw Failure(message: "window_id is required") }
    let entry = try window(windowId)
    let limit = (args["max_nodes"] as? Int) ?? 300
    guard (1...1000).contains(limit) else { throw Failure(message: "max_nodes must be between 1 and 1000") }
    let (nodes, order, truncated) = try tree(entry.element, limit: limit)
    let token = UUID().uuidString.replacingOccurrences(of: "-", with: "").lowercased()
    let previous = observations[windowId]
    var result: [String: Any] = ["window_id": windowId, "observation_id": token, "truncated": truncated,
                                 "coordinate_space": "AX screen points (top-left origin)"]
    if let requested = args["since"] as? String {
        guard let previous = previous, previous.observationId == requested else {
            throw Failure(message: "unknown diff baseline; observe without since")
        }
        result["changed"] = try order.compactMap { id -> Any? in
            let node = nodes[id]!
            return previous.nodes[id] == node ? nil : try jsonObject(node)
        }
        result["removed"] = previous.order.filter { nodes[$0] == nil }
    } else {
        result["nodes"] = try order.map { try jsonObject(nodes[$0]!) }
    }
    observations[windowId] = Observation(observationId: token, nodes: nodes, order: order)
    if (args["screenshot"] as? Bool) == true {
        for (key, value) in try screenshot(args["screenshot_scope"] as? String, windowId: windowId) { result[key] = value }
    }
    return result
}

func target(_ args: [String: Any]) throws -> (WindowEntry, AXUIElement) {
    guard let windowId = args["window_id"] as? String else { throw Failure(message: "window_id is required") }
    let entry = try window(windowId)
    guard let observed = observations[windowId], observed.observationId == (args["observation_id"] as? String) else {
        throw Failure(message: "stale observation_id; observe the window again before acting")
    }
    guard let key = args["element_id"] as? String, let expected = observed.nodes[key], let element = objects[key] else {
        throw Failure(message: "element_id was not present in this observation")
    }
    var cursor = element
    var depth = 0
    while !CFEqual(cursor, entry.element) {
        depth += 1
        guard depth <= 64 else { throw Failure(message: "element ancestry is too deep") }
        guard let parent = attribute(cursor, kAXParentAttribute) else {
            throw Failure(message: "element no longer belongs to this window; observe again")
        }
        cursor = parent as! AXUIElement
    }
    guard alive(element), try describe(element, parent: expected.parent) == expected else {
        throw Failure(message: "element changed since observation; observe again")
    }
    guard expected.enabled else { throw Failure(message: "element is disabled") }
    return (entry, element)
}

func mutate(_ args: [String: Any], op: String) throws -> [String: Any] {
    let (entry, element) = try target(args)
    let windowId = entry.id
    // Consume the observation BEFORE issuing an effect, including failed effects.
    observations[windowId] = nil
    switch op {
    case "click":
        guard (actionNames(element) ?? []).contains(kAXPressAction as String) else {
            throw Failure(message: "element has no semantic press action; no coordinate fallback performed")
        }
        let status = AXUIElementPerformAction(element, kAXPressAction as CFString)
        guard status == .success else { throw Failure(message: "AXPress was rejected (AXError \(status.rawValue))") }
    case "set_value":
        guard let text = args["text"] as? String, text.count <= 16384 else {
            throw Failure(message: "text must be a string of at most 16384 characters")
        }
        if string(element, kAXRoleAttribute) == kAXSecureTextFieldRole as String {
            throw Failure(message: "password entry requires a human")
        }
        var settable = DarwinBoolean(false)
        guard AXUIElementIsAttributeSettable(element, kAXValueAttribute as CFString, &settable) == .success, settable.boolValue else {
            throw Failure(message: "element value is not settable")
        }
        let status = AXUIElementSetAttributeValue(element, kAXValueAttribute as CFString, text as CFString)
        guard status == .success else { throw Failure(message: "AX rejected text replacement (AXError \(status.rawValue))") }
    default:
        throw Failure(message: "unsupported operation: \(op)")
    }
    // Bounded settling: two matching trees; not a claim that application work finished.
    let deadline = Date().addingTimeInterval(1.5)
    var previous: [String: Node]?
    var settled = false
    while Date() < deadline {
        let (current, _, _) = try tree(entry.element, limit: 300)
        if let previous = previous, previous == current { settled = true; break }
        previous = current
        Thread.sleep(forTimeInterval: 0.1)
    }
    var result = try snapshot(["window_id": windowId])
    result["action"] = op
    result["dispatched"] = true
    result["tree_settled"] = settled
    result["verification"] = "Inspect the returned tree for the requested application effect."
    return result
}

func dispatch(_ args: [String: Any]) throws -> [String: Any] {
    guard let op = args["op"] as? String else { throw Failure(message: "op is required") }
    switch op {
    case "capabilities":
        let trusted = AXIsProcessTrusted()
        let capture = CGPreflightScreenCaptureAccess()
        return ["platform": "macos", "backend": "AXUIElement", "desktop_available": trusted,
                "accessibility_trusted": trusted, "screen_recording": capture,
                "operations": ["capabilities", "list_windows", "observe", "screenshot", "click", "set_value"],
                "capture_scopes": capture ? ["desktop", "window"] : [],
                "limitations": ["No keyboard, pointer injection, drag, or scroll backend yet.",
                                trusted ? "Accessibility access granted." : "Grant Accessibility access to the terminal running Borg (System Settings > Privacy & Security > Accessibility).",
                                capture ? "Screen Recording access granted." : "Grant Screen Recording access to enable screenshots."]]
    case "list_windows":
        return ["windows": try windows().map { ["id": $0.id, "title": $0.title, "application": $0.application, "active": $0.active] }]
    case "screenshot":
        return try screenshot(args["scope"] as? String, windowId: args["window_id"] as? String)
    case "observe":
        return try snapshot(args)
    case "click", "set_value":
        return try mutate(args, op: op)
    default:
        throw Failure(message: "unsupported operation: \(op)")
    }
}

setvbuf(stdout, nil, _IOLBF, 0)
while let line = readLine(strippingNewline: true) {
    var response: [String: Any]
    do {
        guard let request = try JSONSerialization.jsonObject(with: Data(line.utf8)) as? [String: Any] else {
            throw Failure(message: "request must be a JSON object")
        }
        response = ["ok": true, "result": try dispatch(request)]
    } catch let failure as Failure {
        response = ["ok": false, "error": failure.message]
    } catch {
        response = ["ok": false, "error": "\(error)"]
    }
    let data = try! JSONSerialization.data(withJSONObject: response)
    FileHandle.standardOutput.write(data)
    FileHandle.standardOutput.write(Data([0x0a]))
}
