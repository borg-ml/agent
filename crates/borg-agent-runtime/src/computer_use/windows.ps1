# Borg-owned Windows UI Automation worker. JSONL on stdin/stdout; diagnostics on stderr.
# Same contract as linux.py / macos.swift: element handles live only for this process;
# every effect consumes the observation it was issued against. Windows PowerShell 5.1+.
Set-StrictMode -Version 2
$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName UIAutomationClient, UIAutomationTypes, System.Drawing, System.Windows.Forms
Add-Type -Namespace Borg -Name Native -MemberDefinition @'
[DllImport("user32.dll")] public static extern bool SetProcessDPIAware();
[DllImport("user32.dll")] public static extern IntPtr GetForegroundWindow();
[DllImport("user32.dll")] public static extern bool PrintWindow(IntPtr hwnd, IntPtr hdc, uint flags);
[DllImport("user32.dll")] public static extern bool IsWindowVisible(IntPtr hwnd);
[DllImport("user32.dll")] public static extern bool SetForegroundWindow(IntPtr hwnd);
[DllImport("user32.dll")] public static extern bool SetCursorPos(int x, int y);
[DllImport("user32.dll")] public static extern int GetSystemMetrics(int index);
[DllImport("user32.dll", SetLastError = true)] public static extern uint SendInput(uint count, INPUT[] inputs, int size);
[StructLayout(LayoutKind.Sequential)] public struct MOUSEINPUT { public int dx; public int dy; public uint mouseData; public uint dwFlags; public uint time; public IntPtr dwExtraInfo; }
[StructLayout(LayoutKind.Sequential)] public struct KEYBDINPUT { public ushort wVk; public ushort wScan; public uint dwFlags; public uint time; public IntPtr dwExtraInfo; }
[StructLayout(LayoutKind.Explicit)] public struct INPUTUNION { [FieldOffset(0)] public MOUSEINPUT mi; [FieldOffset(0)] public KEYBDINPUT ki; }
[StructLayout(LayoutKind.Sequential)] public struct INPUT { public uint type; public INPUTUNION u; }
public static void Key(ushort vk, bool up) {
    var input = new INPUT { type = 1 }; input.u.ki = new KEYBDINPUT { wVk = vk, dwFlags = up ? 2u : 0u };
    if (SendInput(1, new[] { input }, Marshal.SizeOf(typeof(INPUT))) != 1) throw new Exception("SendInput rejected the key event");
}
public static void Unicode(char c, bool up) {
    var input = new INPUT { type = 1 }; input.u.ki = new KEYBDINPUT { wScan = c, dwFlags = 4u | (up ? 2u : 0u) };
    if (SendInput(1, new[] { input }, Marshal.SizeOf(typeof(INPUT))) != 1) throw new Exception("SendInput rejected the text event");
}
public static void Mouse(uint flags, int dx, int dy, uint data) {
    var input = new INPUT { type = 0 }; input.u.mi = new MOUSEINPUT { dx = dx, dy = dy, mouseData = data, dwFlags = flags };
    if (SendInput(1, new[] { input }, Marshal.SizeOf(typeof(INPUT))) != 1) throw new Exception("SendInput rejected the mouse event");
}
'@
[void][Borg.Native]::SetProcessDPIAware()
[Console]::InputEncoding = [System.Text.UTF8Encoding]::new($false)
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$UIA = [System.Windows.Automation.AutomationElement]
$Epoch = ([guid]::NewGuid().ToString('N')).Substring(0, 12)
$Objects = @{}      # id -> AutomationElement
$ObjectIds = @{}    # runtime id key -> id
$Script:NextId = 0
$Observations = @{} # window id -> @{ observation_id; nodes (ordered id -> node); json (id -> string) }

function Fail([string]$message) { throw [System.Exception]::new($message) }

# Safe property read: absent members return $null instead of throwing under StrictMode.
function Arg($request, [string]$name) {
    if ($null -ne $request -and $request.PSObject.Properties[$name]) { return $request.$name }
    return $null
}

function Identify($element) {
    $key = ($element.GetRuntimeId() -join '.')
    if ($ObjectIds.ContainsKey($key)) { return $ObjectIds[$key] }
    if ($Objects.Count -ge 10000) { Fail 'element handle limit reached; restart the desktop session' }
    $Script:NextId++
    $id = "$Epoch`:$Script:NextId"
    $Objects[$id] = $element
    $ObjectIds[$key] = $id
    return $id
}

function Alive($element) {
    try { [void]$element.Current.ControlType; return $true } catch { return $false }
}

function Windows() {
    $result = @()
    $foreground = [Borg.Native]::GetForegroundWindow()
    $children = $UIA::RootElement.FindAll([System.Windows.Automation.TreeScope]::Children, [System.Windows.Automation.Condition]::TrueCondition)
    foreach ($win in $children) {
        if ($result.Count -ge 256) { break }
        if (-not (Alive $win)) { continue }
        $cur = $win.Current
        if ($cur.ControlType -ne [System.Windows.Automation.ControlType]::Window -or $cur.IsOffscreen) { continue }
        $app = ''
        try { $app = (Get-Process -Id $cur.ProcessId -ErrorAction Stop).ProcessName } catch {}
        $result += [ordered]@{ id = (Identify $win); title = [string]$cur.Name; application = $app;
                               active = ([IntPtr]$cur.NativeWindowHandle -eq $foreground) }
    }
    return ,$result
}

function Window([string]$id) {
    foreach ($w in (Windows)) { if ($w.id -eq $id) { return $Objects[$id] } }
    Fail 'stale or unknown window_id; list_windows again'
}

function Patterns($element) {
    $names = @()
    foreach ($p in $element.GetSupportedPatterns()) { $names += ($p.ProgrammaticName -replace 'PatternIdentifiers\.Pattern$', '') }
    return ,$names
}

function Describe($element, $parent) {
    $cur = $element.Current
    $node = [ordered]@{ id = (Identify $element); parent = $parent
        role = ([string]$cur.ControlType.ProgrammaticName -replace '^ControlType\.', '')
        name = ([string]$cur.Name); enabled = [bool]$cur.IsEnabled; focused = [bool]$cur.HasKeyboardFocus
        showing = (-not $cur.IsOffscreen) }
    if ($node.name.Length -gt 1024) { $node.name = $node.name.Substring(0, 1024) }
    $r = $cur.BoundingRectangle
    if (-not $r.IsEmpty -and $r.Width -gt 0 -and $r.Height -gt 0) {
        $node.bounds = [ordered]@{ x = [double]$r.X; y = [double]$r.Y; width = [double]$r.Width; height = [double]$r.Height }
    }
    if (-not $cur.IsPassword) {
        $text = $null
        $vp = $null
        if ($element.TryGetCurrentPattern([System.Windows.Automation.ValuePattern]::Pattern, [ref]$vp)) { $text = [string]$vp.Current.Value }
        elseif ($element.TryGetCurrentPattern([System.Windows.Automation.TextPattern]::Pattern, [ref]$vp)) { $text = [string]$vp.DocumentRange.GetText(2048) }
        if ($null -ne $text) { if ($text.Length -gt 2048) { $text = $text.Substring(0, 2048) }; $node.text = $text }
    }
    $patterns = Patterns $element
    if ($patterns.Count -gt 0) { $node.actions = $patterns }
    return $node
}

function Tree($root, [int]$limit) {
    $nodes = [ordered]@{}
    $json = @{}
    $queue = New-Object System.Collections.Generic.List[object]
    $queue.Add(@($root, $null, 0))
    $index = 0
    $truncated = $false
    while ($index -lt $queue.Count -and $nodes.Count -lt $limit) {
        $item = $queue[$index]; $index++
        $element = $item[0]; $parent = $item[1]; $depth = $item[2]
        if (-not (Alive $element)) { continue }
        $node = Describe $element $parent
        $nodes[$node.id] = $node
        $json[$node.id] = ($node | ConvertTo-Json -Depth 6 -Compress)
        $kids = @()
        try { $kids = @($element.FindAll([System.Windows.Automation.TreeScope]::Children, [System.Windows.Automation.Condition]::TrueCondition)) } catch {}
        $pending = $queue.Count - $index
        $budget = 0
        if ($depth -lt 32) { $budget = [Math]::Max(0, $limit - $nodes.Count - $pending) }
        if ($kids.Count -gt $budget) { $truncated = $true }
        for ($i = 0; $i -lt [Math]::Min($kids.Count, $budget); $i++) { $queue.Add(@($kids[$i], $node.id, $depth + 1)) }
    }
    return @{ nodes = $nodes; json = $json; truncated = ($truncated -or $index -lt $queue.Count) }
}

function PngBytes([System.Drawing.Bitmap]$bitmap) {
    $stream = New-Object System.IO.MemoryStream
    $bitmap.Save($stream, [System.Drawing.Imaging.ImageFormat]::Png)
    return $stream.ToArray()
}

function Screenshot([string]$scope, [string]$windowId) {
    if ($scope -eq 'desktop') {
        $bounds = [System.Drawing.Rectangle]::Empty
        foreach ($screen in [System.Windows.Forms.Screen]::AllScreens) { $bounds = [System.Drawing.Rectangle]::Union($bounds, $screen.Bounds) }
        $bitmap = New-Object System.Drawing.Bitmap $bounds.Width, $bounds.Height
        $graphics = [System.Drawing.Graphics]::FromImage($bitmap)
        $graphics.CopyFromScreen($bounds.Location, [System.Drawing.Point]::Empty, $bounds.Size)
        $graphics.Dispose()
        $space = 'screenshot pixels of the whole virtual desktop; origin is the virtual-screen top-left, UIA bounds are physical screen pixels'
    } elseif ($scope -eq 'window') {
        if (-not $windowId) { Fail 'window scope requires window_id' }
        $win = Window $windowId
        $hwnd = [IntPtr]$win.Current.NativeWindowHandle
        $r = $win.Current.BoundingRectangle
        if ($hwnd -eq [IntPtr]::Zero -or $r.IsEmpty) { Fail 'window has no native handle or bounds' }
        $bitmap = New-Object System.Drawing.Bitmap ([int]$r.Width), ([int]$r.Height)
        $graphics = [System.Drawing.Graphics]::FromImage($bitmap)
        $hdc = $graphics.GetHdc()
        $ok = [Borg.Native]::PrintWindow($hwnd, $hdc, 2)
        $graphics.ReleaseHdc($hdc); $graphics.Dispose()
        if (-not $ok) { Fail 'isolated window capture was rejected by the application' }
        $space = 'screenshot pixels of one window; origin is the window bounds top-left'
    } else { Fail 'scope must be "desktop" or "window"' }
    $data = PngBytes $bitmap
    $bitmap.Dispose()
    if ($data.Length -gt 4MB) { Fail 'screenshot exceeds 4 MiB' }
    $width = [System.BitConverter]::ToUInt32(($data[19], $data[18], $data[17], $data[16]), 0)
    $height = [System.BitConverter]::ToUInt32(($data[23], $data[22], $data[21], $data[20]), 0)
    return [ordered]@{ scope = $scope; width = [int]$width; height = [int]$height; coordinate_space = $space
        borg_attachments = @([ordered]@{ media_type = 'image/png'; data_base64 = [Convert]::ToBase64String($data) }) }
}

function Snapshot($request) {
    $windowId = [string](Arg $request 'window_id')
    if (-not $windowId) { Fail 'window_id is required' }
    $win = Window $windowId
    $limit = 300
    if ($null -ne (Arg $request 'max_nodes')) { $limit = [int](Arg $request 'max_nodes') }
    if ($limit -lt 1 -or $limit -gt 1000) { Fail 'max_nodes must be between 1 and 1000' }
    $tree = Tree $win $limit
    $token = [guid]::NewGuid().ToString('N')
    $previous = $Observations[$windowId]
    $result = [ordered]@{ window_id = $windowId; observation_id = $token; truncated = $tree.truncated
        coordinate_space = 'physical screen pixels (virtual-screen origin), matching desktop screenshots' }
    $requested = $null
    if ($null -ne (Arg $request 'since')) { $requested = [string](Arg $request 'since') }
    if ($requested) {
        if ($null -eq $previous -or $previous.observation_id -ne $requested) { Fail 'unknown diff baseline; observe without since' }
        $changed = @(); $removed = @()
        foreach ($id in $tree.nodes.Keys) { if ($previous.json[$id] -ne $tree.json[$id]) { $changed += $tree.nodes[$id] } }
        foreach ($id in $previous.nodes.Keys) { if (-not $tree.nodes.Contains($id)) { $removed += $id } }
        $result.changed = $changed; $result.removed = $removed
    } else {
        $result.nodes = @($tree.nodes.Values)
    }
    $Observations[$windowId] = @{ observation_id = $token; nodes = $tree.nodes; json = $tree.json }
    if ((Arg $request 'screenshot') -eq $true) {
        $shot = Screenshot ([string](Arg $request 'screenshot_scope')) $windowId
        foreach ($k in $shot.Keys) { $result[$k] = $shot[$k] }
    }
    return $result
}

function Target($request) {
    $windowId = [string](Arg $request 'window_id')
    $win = Window $windowId
    $observed = $Observations[$windowId]
    if ($null -eq $observed -or $observed.observation_id -ne [string](Arg $request 'observation_id')) { Fail 'stale observation_id; observe the window again before acting' }
    $key = [string](Arg $request 'element_id')
    if (-not $observed.nodes.Contains($key)) { Fail 'element_id was not present in this observation' }
    $element = $Objects[$key]
    $expected = $observed.nodes[$key]
    $walker = [System.Windows.Automation.TreeWalker]::ControlViewWalker
    $cursor = $element
    for ($depth = 0; -not [System.Windows.Automation.Automation]::Compare($cursor, $win); $depth++) {
        if ($depth -ge 64) { Fail 'element ancestry is too deep' }
        $cursor = $walker.GetParent($cursor)
        if ($null -eq $cursor) { Fail 'element no longer belongs to this window; observe again' }
    }
    if (-not (Alive $element)) { Fail 'element changed since observation; observe again' }
    $now = (Describe $element $expected.parent) | ConvertTo-Json -Depth 6 -Compress
    if ($now -ne $observed.json[$key]) { Fail 'element changed since observation; observe again' }
    if (-not $expected.enabled) { Fail 'element is disabled' }
    return @($win, $element)
}

function Mutate($request, [string]$op) {
    $pair = Target $request
    $win = $pair[0]; $element = $pair[1]
    $windowId = [string](Arg $request 'window_id')
    # Consume the observation BEFORE issuing an effect, including failed effects.
    $Observations.Remove($windowId)
    $pattern = $null
    if ($op -eq 'click') {
        if ($element.TryGetCurrentPattern([System.Windows.Automation.InvokePattern]::Pattern, [ref]$pattern)) { $pattern.Invoke() }
        elseif ($element.TryGetCurrentPattern([System.Windows.Automation.TogglePattern]::Pattern, [ref]$pattern)) { $pattern.Toggle() }
        elseif ($element.TryGetCurrentPattern([System.Windows.Automation.SelectionItemPattern]::Pattern, [ref]$pattern)) { $pattern.Select() }
        else { Fail 'element has no semantic invoke/toggle/select pattern; no coordinate fallback performed' }
    } elseif ($op -eq 'set_value') {
        $text = (Arg $request 'text')
        if ($text -isnot [string] -or $text.Length -gt 16384) { Fail 'text must be a string of at most 16384 characters' }
        if ($element.Current.IsPassword) { Fail 'password entry requires a human' }
        if (-not $element.TryGetCurrentPattern([System.Windows.Automation.ValuePattern]::Pattern, [ref]$pattern)) { Fail 'element value is not settable' }
        if ($pattern.Current.IsReadOnly) { Fail 'element is read-only' }
        $pattern.SetValue($text)
    } else { Fail "unsupported operation: $op" }
    # Bounded settling: two matching trees; not a claim that application work finished.
    $deadline = [DateTime]::UtcNow.AddSeconds(1.5)
    $previous = $null
    $settled = $false
    while ([DateTime]::UtcNow -lt $deadline) {
        $current = (Tree $win 300).json
        if ($null -ne $previous -and $previous.Count -eq $current.Count) {
            $same = $true
            foreach ($id in $current.Keys) { if ($previous[$id] -ne $current[$id]) { $same = $false; break } }
            if ($same) { $settled = $true; break }
        }
        $previous = $current
        Start-Sleep -Milliseconds 100
    }
    $result = Snapshot ([pscustomobject]@{ window_id = $windowId })
    $result.action = $op; $result.dispatched = $true; $result.tree_settled = $settled
    $result.verification = 'Inspect the returned tree for the requested application effect.'
    return $result
}

# ---- Input injection (SendInput). Coordinates are physical screen pixels of the virtual desktop.

function FocusWindow($win) {
    $hwnd = [IntPtr]$win.Current.NativeWindowHandle
    if ($hwnd -eq [IntPtr]::Zero) { Fail 'window has no native handle to focus' }
    [void][Borg.Native]::SetForegroundWindow($hwnd)
    Start-Sleep -Milliseconds 150
    if ([Borg.Native]::GetForegroundWindow() -ne $hwnd) { Fail 'could not bring the target window to the front' }
}

function MoveMouse([double]$x, [double]$y) {
    # Absolute SendInput coordinates are normalised to 0..65535 over the virtual screen.
    $vx = [Borg.Native]::GetSystemMetrics(76); $vy = [Borg.Native]::GetSystemMetrics(77)
    $vw = [Borg.Native]::GetSystemMetrics(78); $vh = [Borg.Native]::GetSystemMetrics(79)
    $nx = [int](($x - $vx) * 65535 / [Math]::Max(1, $vw - 1)); $ny = [int](($y - $vy) * 65535 / [Math]::Max(1, $vh - 1))
    [Borg.Native]::Mouse(0x8001 -bor 0x4000, $nx, $ny, 0)   # MOVE | ABSOLUTE | VIRTUALDESK
    Start-Sleep -Milliseconds 50
}

$VirtualKeys = @{ return = 0x0D; enter = 0x0D; tab = 0x09; space = 0x20; escape = 0x1B; esc = 0x1B; backspace = 0x08; delete = 0x08
    forwarddelete = 0x2E; home = 0x24; end = 0x23; pageup = 0x21; pagedown = 0x22; left = 0x25; up = 0x26; right = 0x27; down = 0x28
    f1 = 0x70; f2 = 0x71; f3 = 0x72; f4 = 0x73; f5 = 0x74; f6 = 0x75; f7 = 0x76; f8 = 0x77; f9 = 0x78; f10 = 0x79; f11 = 0x7A; f12 = 0x7B }
$Modifiers = @{ cmd = 0x5B; command = 0x5B; meta = 0x5B; super = 0x5B; win = 0x5B; ctrl = 0x11; control = 0x11; alt = 0x12; option = 0x12; opt = 0x12; shift = 0x10 }

function PressKeys($win, [string]$spec) {
    $held = @(); $key = $null
    foreach ($part in ($spec.ToLowerInvariant() -split '\+' | ForEach-Object { $_.Trim() })) {
        if ($Modifiers.ContainsKey($part)) { $held += [uint16]$Modifiers[$part]; continue }
        if ($null -ne $key) { Fail 'use one non-modifier key per call' }
        if ($VirtualKeys.ContainsKey($part)) { $key = [uint16]$VirtualKeys[$part] }
        elseif ($part.Length -eq 1 -and $part -match '[a-z0-9]') { $key = [uint16][char]$part.ToUpperInvariant() }
        else { Fail "unsupported key `"$part`"" }
    }
    if ($null -eq $key) { Fail 'keys must name one non-modifier key' }
    FocusWindow $win
    foreach ($m in $held) { [Borg.Native]::Key($m, $false) }
    [Borg.Native]::Key($key, $false); [Borg.Native]::Key($key, $true)
    [array]::Reverse($held); foreach ($m in $held) { [Borg.Native]::Key($m, $true) }
}

function PointerTarget($request, $win) {
    if ($null -ne (Arg $request 'element_id')) {
        $pair = Target $request
        $r = $pair[1].Current.BoundingRectangle
        if ($r.IsEmpty -or $r.Width -le 0 -or $r.Height -le 0) { Fail 'element has no on-screen bounds' }
        $Observations.Remove([string](Arg $request 'window_id'))
        return @(($r.X + $r.Width / 2), ($r.Y + $r.Height / 2), $false)
    }
    $x = Arg $request 'x'; $y = Arg $request 'y'
    if ($null -eq $x -or $null -eq $y) { Fail 'pointer ops need element_id + observation_id or x + y' }
    $Observations.Remove([string](Arg $request 'window_id'))
    return @([double]$x, [double]$y, $true)
}

function ButtonFlags($name) {
    switch ([string]$name) {
        '' { return @(0x0002, 0x0004, 0) }
        'left' { return @(0x0002, 0x0004, 0) }
        'right' { return @(0x0008, 0x0010, 0) }
        'middle' { return @(0x0020, 0x0040, 0) }
        default { Fail 'button must be left, right or middle' }
    }
}

function Settle($win, [string]$op, $extra) {
    $windowId = (Identify $win)
    $deadline = [DateTime]::UtcNow.AddSeconds(1.5)
    $previous = $null; $settled = $false
    while ([DateTime]::UtcNow -lt $deadline) {
        $current = (Tree $win 300).json
        if ($null -ne $previous -and $previous.Count -eq $current.Count) {
            $same = $true
            foreach ($id in $current.Keys) { if ($previous[$id] -ne $current[$id]) { $same = $false; break } }
            if ($same) { $settled = $true; break }
        }
        $previous = $current
        Start-Sleep -Milliseconds 100
    }
    $result = Snapshot ([pscustomobject]@{ window_id = $windowId })
    $result.action = $op; $result.dispatched = $true; $result.tree_settled = $settled
    $result.verification = 'Inspect the returned tree for the requested application effect.'
    if ($null -ne $extra) { foreach ($k in $extra.Keys) { $result[$k] = $extra[$k] } }
    return $result
}

function Inject($request, [string]$op) {
    $windowId = [string](Arg $request 'window_id')
    if (-not $windowId) { Fail 'window_id is required' }
    $win = Window $windowId
    switch ($op) {
        'type_text' {
            $text = Arg $request 'text'
            if ($text -isnot [string] -or $text.Length -gt 16384) { Fail 'text must be a string of at most 16384 characters' }
            $Observations.Remove($windowId)
            FocusWindow $win
            foreach ($c in $text.ToCharArray()) { [Borg.Native]::Unicode($c, $false); [Borg.Native]::Unicode($c, $true) }
            return Settle $win $op $null
        }
        'key' {
            $keys = [string](Arg $request 'keys')
            if (-not $keys) { Fail 'keys is required' }
            $Observations.Remove($windowId)
            PressKeys $win $keys
            return Settle $win $op @{ keys = $keys }
        }
        'pointer_click' {
            $t = PointerTarget $request $win
            $flags = ButtonFlags (Arg $request 'button')
            $count = 1; if ($null -ne (Arg $request 'count')) { $count = [int](Arg $request 'count') }
            if ($count -lt 1 -or $count -gt 2) { Fail 'count must be 1 or 2' }
            FocusWindow $win
            MoveMouse $t[0] $t[1]
            for ($i = 0; $i -lt $count; $i++) { [Borg.Native]::Mouse($flags[0], 0, 0, 0); Start-Sleep -Milliseconds 30; [Borg.Native]::Mouse($flags[1], 0, 0, 0); Start-Sleep -Milliseconds 50 }
            return Settle $win $op @{ coordinate_click = $t[2]; point = [ordered]@{ x = $t[0]; y = $t[1] } }
        }
        'scroll' {
            $t = PointerTarget $request $win
            $dx = [int](Arg $request 'dx'); $dy = [int](Arg $request 'dy')
            if ([Math]::Abs($dx) -gt 10000 -or [Math]::Abs($dy) -gt 10000) { Fail 'scroll distance is limited to 10000 pixels' }
            FocusWindow $win
            MoveMouse $t[0] $t[1]
            # WHEEL data is positive to scroll content up; convert 120-unit notches per 40 px.
            if ($dy -ne 0) { [Borg.Native]::Mouse(0x0800, 0, 0, [uint32]([int](-$dy * 3) -band 0xFFFFFFFF)) }
            if ($dx -ne 0) { [Borg.Native]::Mouse(0x1000, 0, 0, [uint32]([int]($dx * 3) -band 0xFFFFFFFF)) }
            return Settle $win $op @{ coordinate_click = $t[2]; point = [ordered]@{ x = $t[0]; y = $t[1] }; units = 'pixels' }
        }
        'drag' {
            $fx = Arg $request 'from_x'; $fy = Arg $request 'from_y'; $tx = Arg $request 'to_x'; $ty = Arg $request 'to_y'
            if ($null -eq $fx -or $null -eq $fy -or $null -eq $tx -or $null -eq $ty) { Fail 'drag needs from_x, from_y, to_x, to_y' }
            $flags = ButtonFlags (Arg $request 'button')
            $Observations.Remove($windowId)
            FocusWindow $win
            MoveMouse ([double]$fx) ([double]$fy)
            [Borg.Native]::Mouse($flags[0], 0, 0, 0)
            for ($i = 1; $i -le 12; $i++) {
                $t = $i / 12.0
                MoveMouse ([double]$fx + ([double]$tx - [double]$fx) * $t) ([double]$fy + ([double]$ty - [double]$fy) * $t)
            }
            [Borg.Native]::Mouse($flags[1], 0, 0, 0)
            return Settle $win $op @{ from = [ordered]@{ x = [double]$fx; y = [double]$fy }; to = [ordered]@{ x = [double]$tx; y = [double]$ty } }
        }
        default { Fail "unsupported operation: $op" }
    }
}

function Dispatch($request) {
    $op = [string](Arg $request 'op')
    switch ($op) {
        'capabilities' {
            return [ordered]@{ platform = 'windows'; backend = 'UI Automation'; desktop_available = $true
                operations = @('capabilities', 'list_windows', 'observe', 'screenshot', 'click', 'set_value', 'type_text', 'key', 'pointer_click', 'scroll', 'drag')
                capture_scopes = @('desktop', 'window')
                input_coordinate_space = 'physical screen pixels of the virtual desktop, matching desktop screenshots'
                limitations = @('Input injection brings the target window to the foreground first, so it changes focus.',
                                'Runs in the interactive user session only; elevated (UAC) windows are not observable.') }
        }
        'list_windows' { return [ordered]@{ windows = (Windows) } }
        'screenshot' { return Screenshot ([string](Arg $request 'scope')) ([string](Arg $request 'window_id')) }
        'observe' { return Snapshot $request }
        'click' { return Mutate $request $op }
        'set_value' { return Mutate $request $op }
        { $_ -in 'type_text', 'key', 'pointer_click', 'scroll', 'drag' } { return Inject $request $op }
        default { Fail "unsupported operation: $op" }
    }
}

while ($null -ne ($line = [Console]::In.ReadLine())) {
    $response = $null
    try {
        $request = $line | ConvertFrom-Json
        $response = [ordered]@{ ok = $true; result = (Dispatch $request) }
    } catch {
        $response = [ordered]@{ ok = $false; error = [string]$_.Exception.Message }
    }
    [Console]::Out.WriteLine(($response | ConvertTo-Json -Depth 12 -Compress))
    [Console]::Out.Flush()
}
