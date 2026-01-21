cd "$PSScriptRoot\.."
# Optional cleanup: remove legacy compose backups and docker/.env if you no longer use them
Remove-Item .\docker\compose.yml.bak* -Force -ErrorAction SilentlyContinue
Remove-Item .\docker\.env -Force -ErrorAction SilentlyContinue
