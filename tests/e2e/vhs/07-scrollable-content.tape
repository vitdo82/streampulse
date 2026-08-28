# 07-scrollable-content: bounded tables + analytics scroll on a constrained
# terminal. At ~85x26 the six Analytics panes overflow the fold; j scrolls the
# viewport so REBALANCES/PATTERNS become reachable while the footer help bar
# (j/k: scroll) stays pinned to the last line.
Output "tests/e2e/screenshots/07-scrollable-content.gif"
Set Shell "bash"
Set FontSize 18
Set Padding 12
Set Width 1140
Set Height 640
Type "./bin/streampulse --brokers localhost:9093"
Enter
Sleep 5s
Type "6"
Sleep 2s
Type "j"
Sleep 1s
Type "j"
Sleep 1s
Type "j"
Sleep 1s
Type "j"
Sleep 1s
Type "j"
Sleep 1s
Type "j"
Sleep 1s
Type "j"
Sleep 1s
Type "j"
Sleep 2s
Type "k"
Sleep 1s