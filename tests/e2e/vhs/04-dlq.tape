# 04-dlq: DLQ tab shows discovered dead-letter topics; Enter inspects one.
Output "tests/e2e/screenshots/04-dlq.gif"
Set Shell "bash"
Set FontSize 16
Set Padding 12
Set Width 1200
Set Height 700
Type "./bin/streampulse --brokers localhost:9093"
Enter
Sleep 5s
Type "5"
Sleep 3s
Enter
Sleep 3s
Type "esc"
