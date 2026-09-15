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

function Snapshot($args) {
    $windowId = [string]$args.window_id
    if (-not $windowId) { Fail 'window_id is required' }
    $win = Window $windowId
    $limit = 300
    if ($null -ne $args.max_nodes) { $limit = [int]$args.max_nodes }
    if ($limit -lt 1 -or $limit -gt 1000) { Fail 'max_nodes must be between 1 and 1000' }
    $tree = Tree $win $limit
    $token = [guid]::NewGuid().ToString('N')
    $previous = $Observations[$windowId]
    $result = [ordered]@{ window_id = $windowId; observation_id = $token; truncated = $tree.truncated
        coordinate_space = 'physical screen pixels (virtual-screen origin), matching desktop screenshots' }
    $requested = $null
    if ($null -ne $args.since) { $requested = [string]$args.since }
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
    if ($args.screenshot -eq $true) {
        $shot = Screenshot ([string]$args.screenshot_scope) $windowId
        foreach ($k in $shot.Keys) { $result[$k] = $shot[$k] }
    }
    return $result
}

function Target($args) {
    $windowId = [string]$args.window_id
    $win = Window $windowId
    $observed = $Observations[$windowId]
    if ($null -eq $observed -or $observed.observation_id -ne [string]$args.observation_id) { Fail 'stale observation_id; observe the window again before acting' }
    $key = [string]$args.element_id
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

function Mutate($args, [string]$op) {
    $pair = Target $args
    $win = $pair[0]; $element = $pair[1]
    $windowId = [string]$args.window_id
    # Consume the observation BEFORE issuing an effect, including failed effects.
    $Observations.Remove($windowId)
    $pattern = $null
    if ($op -eq 'click') {
        if ($element.TryGetCurrentPattern([System.Windows.Automation.InvokePattern]::Pattern, [ref]$pattern)) { $pattern.Invoke() }
        elseif ($element.TryGetCurrentPattern([System.Windows.Automation.TogglePattern]::Pattern, [ref]$pattern)) { $pattern.Toggle() }
        elseif ($element.TryGetCurrentPattern([System.Windows.Automation.SelectionItemPattern]::Pattern, [ref]$pattern)) { $pattern.Select() }
        else { Fail 'element has no semantic invoke/toggle/select pattern; no coordinate fallback performed' }
    } elseif ($op -eq 'set_value') {
        $text = $args.text
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

function Dispatch($args) {
    $op = [string]$args.op
    switch ($op) {
        'capabilities' {
            return [ordered]@{ platform = 'windows'; backend = 'UI Automation'; desktop_available = $true
                operations = @('capabilities', 'list_windows', 'observe', 'screenshot', 'click', 'set_value')
                capture_scopes = @('desktop', 'window')
                limitations = @('No keyboard, pointer injection, drag, or scroll backend yet.',
                                'Runs in the interactive user session only; elevated (UAC) windows are not observable.') }
        }
        'list_windows' { return [ordered]@{ windows = (Windows) } }
        'screenshot' { return Screenshot ([string]$args.scope) ([string]$args.window_id) }
        'observe' { return Snapshot $args }
        'click' { return Mutate $args $op }
        'set_value' { return Mutate $args $op }
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
