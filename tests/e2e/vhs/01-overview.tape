# 01-overview: boot the TUI in Kafka mode and verify the Overview tab.
Output "tests/e2e/screenshots/01-overview.gif"
Set Shell "bash"
Set FontSize 16
Set Padding 12
Set Width 1200
Set Height 700
Type "./bin/streampulse --brokers localhost:9093"
Enter
Sleep 5s
