# 02-topics-search: Topics tab, then search for "orders".
Output "tests/e2e/screenshots/02-topics-search.gif"
Set Shell "bash"
Set FontSize 16
Set Padding 12
Set Width 1200
Set Height 700
Type "./bin/streampulse --brokers localhost:9093"
Enter
Sleep 5s
Type "2"
Sleep 2s
Type "/"
Type "orders"
Sleep 2s
Type "esc"
