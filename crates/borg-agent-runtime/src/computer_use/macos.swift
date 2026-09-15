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

func axPoint(_ value: AnyObject?) -> CGPoint? {
    guard let value = value, CFGetTypeID(value) == AXValueGetTypeID() else { return nil }
    var out = CGPoint.zero
    return AXValueGetValue(value as! AXValue, .cgPoint, &out) ? out : nil
}

func axSize(_ value: AnyObject?) -> CGSize? {
    guard let value = value, CFGetTypeID(value) == AXValueGetTypeID() else { return nil }
    var out = CGSize.zero
    return AXValueGetValue(value as! AXValue, .cgSize, &out) ? out : nil
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
    if let origin = axPoint(attribute(element, kAXPositionAttribute)),
       let size = axSize(attribute(element, kAXSizeAttribute)),
       size.width > 0, size.height > 0 {
        bounds = Bounds(x: origin.x, y: origin.y, width: size.width, height: size.height)
    }
    var text: String?
    if role != "AXSecureTextField", let value = attribute(element, kAXValueAttribute) as? String {
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
        if string(element, kAXRoleAttribute) == "AXSecureTextField" {
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
    return try settleAndSnapshot(entry, op: op)
}

/// Bounded settling: two matching trees; not a claim that application work finished.
func settleAndSnapshot(_ entry: WindowEntry, op: String, extra: [String: Any] = [:]) throws -> [String: Any] {
    let deadline = Date().addingTimeInterval(1.5)
    var previous: [String: Node]?
    var settled = false
    while Date() < deadline {
        let (current, _, _) = try tree(entry.element, limit: 300)
        if let previous = previous, previous == current { settled = true; break }
        previous = current
        Thread.sleep(forTimeInterval: 0.1)
    }
    var result = try snapshot(["window_id": entry.id])
    result["action"] = op
    result["dispatched"] = true
    result["tree_settled"] = settled
    result["verification"] = "Inspect the returned tree for the requested application effect."
    for (key, value) in extra { result[key] = value }
    return result
}

// MARK: - Input injection (CGEvent). Coordinates are AX screen points, top-left origin.

func number(_ value: Any?) -> Double? {
    if let d = value as? Double { return d }
    if let i = value as? Int { return Double(i) }
    return nil
}

/// Bring the target window to the front so injected events reach it.
func focusWindow(_ entry: WindowEntry) throws {
    guard let app = NSRunningApplication(processIdentifier: entry.pid) else {
        throw Failure(message: "target application is no longer running")
    }
    app.activate(options: [.activateIgnoringOtherApps])
    _ = AXUIElementPerformAction(entry.element, kAXRaiseAction as CFString)
    Thread.sleep(forTimeInterval: 0.15)
    guard app.isActive else { throw Failure(message: "could not bring the target application to the front") }
}

func post(_ event: CGEvent?) throws {
    guard let event = event else { throw Failure(message: "could not create input event") }
    event.post(tap: .cghidEventTap)
}

let keyCodes: [String: CGKeyCode] = [
    "a": 0, "s": 1, "d": 2, "f": 3, "h": 4, "g": 5, "z": 6, "x": 7, "c": 8, "v": 9, "b": 11, "q": 12, "w": 13,
    "e": 14, "r": 15, "y": 16, "t": 17, "1": 18, "2": 19, "3": 20, "4": 21, "6": 22, "5": 23, "=": 24, "9": 25,
    "7": 26, "-": 27, "8": 28, "0": 29, "]": 30, "o": 31, "u": 32, "[": 33, "i": 34, "p": 35, "return": 36,
    "enter": 36, "l": 37, "j": 38, "'": 39, "k": 40, ";": 41, "\\": 42, ",": 43, "/": 44, "n": 45, "m": 46,
    ".": 47, "tab": 48, "space": 49, "`": 50, "delete": 51, "backspace": 51, "escape": 53, "esc": 53,
    "forwarddelete": 117, "home": 115, "end": 119, "pageup": 116, "pagedown": 121, "left": 123, "right": 124,
    "down": 125, "up": 126, "f1": 122, "f2": 120, "f3": 99, "f4": 118, "f5": 96, "f6": 97, "f7": 98, "f8": 100,
    "f9": 101, "f10": 109, "f11": 103, "f12": 111,
]

func typeText(_ entry: WindowEntry, _ text: String) throws {
    try focusWindow(entry)
    for chunk in Array(text.utf16).chunked(20) {
        let down = CGEvent(keyboardEventSource: nil, virtualKey: 0, keyDown: true)
        let up = CGEvent(keyboardEventSource: nil, virtualKey: 0, keyDown: false)
        down?.keyboardSetUnicodeString(stringLength: chunk.count, unicodeString: chunk)
        up?.keyboardSetUnicodeString(stringLength: chunk.count, unicodeString: chunk)
        try post(down); try post(up)
        Thread.sleep(forTimeInterval: 0.01)
    }
}

extension Array {
    func chunked(_ size: Int) -> [[Element]] {
        stride(from: 0, to: count, by: size).map { Array(self[$0..<Swift.min($0 + size, count)]) }
    }
}

func pressKeys(_ entry: WindowEntry, _ spec: String) throws {
    var flags = CGEventFlags()
    var key: CGKeyCode?
    for part in spec.lowercased().split(separator: "+").map({ $0.trimmingCharacters(in: .whitespaces) }) {
        switch part {
        case "cmd", "command", "meta", "super": flags.insert(.maskCommand)
        case "ctrl", "control": flags.insert(.maskControl)
        case "alt", "option", "opt": flags.insert(.maskAlternate)
        case "shift": flags.insert(.maskShift)
        default:
            guard key == nil, let code = keyCodes[part] else {
                throw Failure(message: "unsupported key \"\(part)\"; use one non-modifier key per call")
            }
            key = code
        }
    }
    guard let code = key else { throw Failure(message: "keys must name one non-modifier key") }
    try focusWindow(entry)
    let down = CGEvent(keyboardEventSource: nil, virtualKey: code, keyDown: true)
    let up = CGEvent(keyboardEventSource: nil, virtualKey: code, keyDown: false)
    down?.flags = flags; up?.flags = flags
    try post(down); try post(up)
}

/// Resolve a pointer target: the centre of an observed element (validated
/// like click) or an explicit AX screen point.
func pointerTarget(_ args: [String: Any], entry: WindowEntry) throws -> (CGPoint, Bool) {
    if args["element_id"] != nil {
        let (_, element) = try target(args)
        guard let origin = axPoint(attribute(element, kAXPositionAttribute)),
              let size = axSize(attribute(element, kAXSizeAttribute)), size.width > 0, size.height > 0 else {
            throw Failure(message: "element has no on-screen bounds")
        }
        observations[entry.id] = nil
        return (CGPoint(x: origin.x + size.width / 2, y: origin.y + size.height / 2), false)
    }
    guard let x = number(args["x"]), let y = number(args["y"]) else {
        throw Failure(message: "pointer ops need element_id + observation_id or x + y")
    }
    observations[entry.id] = nil
    return (CGPoint(x: x, y: y), true)
}

func mouseButton(_ name: Any?) throws -> (CGMouseButton, CGEventType, CGEventType, CGEventType) {
    switch (name as? String) ?? "left" {
    case "left": return (.left, .leftMouseDown, .leftMouseUp, .leftMouseDragged)
    case "right": return (.right, .rightMouseDown, .rightMouseUp, .rightMouseDragged)
    case "middle": return (.center, .otherMouseDown, .otherMouseUp, .otherMouseDragged)
    default: throw Failure(message: "button must be left, right or middle")
    }
}

func movePointer(to point: CGPoint) throws {
    try post(CGEvent(mouseEventSource: nil, mouseType: .mouseMoved, mouseCursorPosition: point, mouseButton: .left))
    Thread.sleep(forTimeInterval: 0.05)
}

func pointerClick(_ args: [String: Any], entry: WindowEntry) throws -> [String: Any] {
    let (point, coordinate) = try pointerTarget(args, entry: entry)
    let (button, downType, upType, _) = try mouseButton(args["button"])
    let count = (args["count"] as? Int) ?? 1
    guard (1...2).contains(count) else { throw Failure(message: "count must be 1 or 2") }
    try focusWindow(entry)
    try movePointer(to: point)
    for click in 1...count {
        let down = CGEvent(mouseEventSource: nil, mouseType: downType, mouseCursorPosition: point, mouseButton: button)
        let up = CGEvent(mouseEventSource: nil, mouseType: upType, mouseCursorPosition: point, mouseButton: button)
        down?.setIntegerValueField(.mouseEventClickState, value: Int64(click))
        up?.setIntegerValueField(.mouseEventClickState, value: Int64(click))
        try post(down); Thread.sleep(forTimeInterval: 0.03); try post(up)
        Thread.sleep(forTimeInterval: 0.05)
    }
    return try settleAndSnapshot(entry, op: "pointer_click", extra: ["coordinate_click": coordinate, "point": ["x": point.x, "y": point.y]])
}

func scroll(_ args: [String: Any], entry: WindowEntry) throws -> [String: Any] {
    let (point, coordinate) = try pointerTarget(args, entry: entry)
    let dx = Int32(number(args["dx"]) ?? 0), dy = Int32(number(args["dy"]) ?? 0)
    guard abs(dx) <= 10000, abs(dy) <= 10000 else { throw Failure(message: "scroll distance is limited to 10000 pixels") }
    try focusWindow(entry)
    try movePointer(to: point)
    // Positive dy scrolls content down; CGEvent's wheel1 is positive for scrolling up.
    try post(CGEvent(scrollWheelEvent2Source: nil, units: .pixel, wheelCount: 2, wheel1: -dy, wheel2: -dx, wheel3: 0))
    return try settleAndSnapshot(entry, op: "scroll", extra: ["coordinate_click": coordinate, "point": ["x": point.x, "y": point.y], "units": "pixels"])
}

func drag(_ args: [String: Any], entry: WindowEntry) throws -> [String: Any] {
    guard let fx = number(args["from_x"]), let fy = number(args["from_y"]),
          let tx = number(args["to_x"]), let ty = number(args["to_y"]) else {
        throw Failure(message: "drag needs from_x, from_y, to_x, to_y")
    }
    let (button, downType, upType, dragType) = try mouseButton(args["button"])
    observations[entry.id] = nil
    try focusWindow(entry)
    let from = CGPoint(x: fx, y: fy), to = CGPoint(x: tx, y: ty)
    try movePointer(to: from)
    try post(CGEvent(mouseEventSource: nil, mouseType: downType, mouseCursorPosition: from, mouseButton: button))
    let steps = 12
    for step in 1...steps {
        let t = Double(step) / Double(steps)
        let point = CGPoint(x: from.x + (to.x - from.x) * t, y: from.y + (to.y - from.y) * t)
        try post(CGEvent(mouseEventSource: nil, mouseType: dragType, mouseCursorPosition: point, mouseButton: button))
        Thread.sleep(forTimeInterval: 0.02)
    }
    try post(CGEvent(mouseEventSource: nil, mouseType: upType, mouseCursorPosition: to, mouseButton: button))
    return try settleAndSnapshot(entry, op: "drag", extra: ["from": ["x": fx, "y": fy], "to": ["x": tx, "y": ty]])
}

func inject(_ args: [String: Any], op: String) throws -> [String: Any] {
    guard let windowId = args["window_id"] as? String else { throw Failure(message: "window_id is required") }
    let entry = try window(windowId)
    switch op {
    case "type_text":
        guard let text = args["text"] as? String, text.count <= 16384 else {
            throw Failure(message: "text must be a string of at most 16384 characters")
        }
        observations[windowId] = nil
        try typeText(entry, text)
        return try settleAndSnapshot(entry, op: op)
    case "key":
        guard let keys = args["keys"] as? String else { throw Failure(message: "keys is required") }
        observations[windowId] = nil
        try pressKeys(entry, keys)
        return try settleAndSnapshot(entry, op: op, extra: ["keys": keys])
    case "pointer_click": return try pointerClick(args, entry: entry)
    case "scroll": return try scroll(args, entry: entry)
    case "drag": return try drag(args, entry: entry)
    default: throw Failure(message: "unsupported operation: \(op)")
    }
}

func dispatch(_ args: [String: Any]) throws -> [String: Any] {
    guard let op = args["op"] as? String else { throw Failure(message: "op is required") }
    switch op {
    case "capabilities":
        let trusted = AXIsProcessTrusted()
        let capture = CGPreflightScreenCaptureAccess()
        return ["platform": "macos", "backend": "AXUIElement", "desktop_available": trusted,
                "accessibility_trusted": trusted, "screen_recording": capture,
                "operations": ["capabilities", "list_windows", "observe", "screenshot", "click", "set_value",
                               "type_text", "key", "pointer_click", "scroll", "drag"],
                "capture_scopes": capture ? ["desktop", "window"] : [],
                "input_coordinate_space": "AX screen points (top-left origin); divide Retina screenshot pixels by the display scale",
                "limitations": ["Input injection raises the target window first, so it changes focus.",
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
    case "type_text", "key", "pointer_click", "scroll", "drag":
        return try inject(args, op: op)
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
