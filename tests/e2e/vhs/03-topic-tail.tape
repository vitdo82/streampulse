# 03-topic-tail: Search for the orders topic, open the tail view (enriched
# header: msgs/last-timestamp/offsets), pause the follow.
# NOTE: requires a binary built after the tail feature (git log >= a6e23b2).
Output "tests/e2e/screenshots/03-topic-tail.gif"
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
Sleep 1s
Type "orders"
Enter
Sleep 2s
Enter
Sleep 3s
Type "p"
Sleep 2s
Type "esc"
