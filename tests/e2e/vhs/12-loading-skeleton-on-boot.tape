# 12-loading-skeleton-on-boot: boot the TUI against an unreachable broker and
# verify the summary cards show the "loading…" placeholder instead of "0"
# counts (SP-13). Because the first scrape fails, the cards must NOT resolve
# to zero — "loading…" proves empty is distinguishable from still-connecting.
# The word "loading" is OCR-verified by `make e2e-verify`.
Output "tests/e2e/screenshots/12-loading-skeleton-on-boot.gif"
Set Shell "bash"
Set FontSize 16
Set Padding 12
Set Width 1200
Set Height 700
Type "./bin/streampulse --brokers localhost:1"
Enter
Sleep 5s
Type "q"
Sleep 2s