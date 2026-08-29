# 11-refresh-failure-banner: boot the TUI against an unreachable broker and
# verify the red "last scrape failed" banner renders at the top of the content
# region (SP-11). The banner text "data may be stale" is OCR-verified by
# `make e2e-verify`.
Output "tests/e2e/screenshots/11-refresh-failure-banner.gif"
Set Shell "bash"
Set FontSize 16
Set Padding 12
Set Width 1200
Set Height 700
Type "./bin/streampulse --brokers localhost:1"
Enter
Sleep 5s
Type "2"
Sleep 2s
Type "q"
Sleep 2s