# 10-help-modal: `?` opens the keybinding legend modal, esc closes it.
# Also captures the contextual footers: Analytics (w/a) vs a table tab.
Output "tests/e2e/screenshots/10-help-modal.gif"
Set Shell "bash"
Set FontSize 16
Set Padding 12
Set Width 1200
Set Height 700
Type "./bin/streampulse --brokers localhost:9093"
Enter
Sleep 5s
Type "?"
Sleep 3s
Type "esc"
Sleep 1s
Type "6"
Sleep 2s
Type "?"
Sleep 3s
Type "esc"