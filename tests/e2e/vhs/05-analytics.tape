# 05-analytics: Analytics tab panes and the analyze CLI view (key "a").
# NOTE: the "a" view requires a binary built after the analyze keybinding
# (git log >= efd095f).
Output "tests/e2e/screenshots/05-analytics.gif"
Set Shell "bash"
Set FontSize 16
Set Padding 12
Set Width 1200
Set Height 700
Type "./bin/streampulse --brokers localhost:9093"
Enter
Sleep 5s
Type "6"
Sleep 3s
Type "a"
Sleep 3s
Type "esc"
