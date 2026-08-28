# 09-search-no-results: on the Topics tab, verify the footer match count
# while searching and the explicit "No match /query/" state when a search
# filters everything out.
Output "tests/e2e/screenshots/09-search-no-results.gif"
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
Ctrl+[
Sleep 1s
Type "/"
Type "zzz-no-such-topic"
Sleep 2s
Ctrl+[
Sleep 1s
Type "q"
Sleep 2s
Type "echo NO_RESULTS_OK"
Enter
Sleep 3s