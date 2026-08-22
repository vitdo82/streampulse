# 06-overlay-q-quit: "q" inside an overlay must quit the app, not close the overlay.
# Proof: after "q" the TUI exits and control returns to bash. The sentinel
# `echo Q_QUIT_OK` only renders if the shell prompt is back — if "q" merely
# closed the tail overlay, the echo would be swallowed as keystrokes and the
# marker would never appear.
Output "tests/e2e/screenshots/06-overlay-q-quit.gif"
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
Enter
Sleep 3s
Type "q"
Sleep 2s
Type "echo Q_QUIT_OK"
Enter
Sleep 2s
