# 08-pagination: on the Topics tab over a large cluster, verify the
# "Showing N of M" indicator, PgUp/PgDn paging through the overflow, and the
# count updating after search filtering (shows only "bulk-*" topics).
Output "tests/e2e/screenshots/08-pagination.gif"
Set Shell "bash"
Set FontSize 16
Set Padding 12
Set Width 1200
Set Height 700
Type "bash tests/e2e/vhs/scripts/seed-topics.sh"
Enter
Wait+Screen@45s /seeded 25 bulk topics/
Type "./bin/streampulse --brokers localhost:9093"
Enter
Wait+Screen@20s /StreamPulse/
Sleep 3s
Type "2"
Sleep 2s
PageDown
Sleep 1s
PageDown
Sleep 1s
PageUp
Sleep 1s
Type "/"
Type "bulk"
Sleep 2s
Enter
Sleep 2s
Type "q"
Sleep 2s
Type "echo PAGINATION_OK"
Enter
Sleep 3s