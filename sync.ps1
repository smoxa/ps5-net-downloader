param (
    [string]$Message = "Update PS5 Net Downloader source"
)

# Refresh PATH to ensure git is accessible
$env:PATH = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path","User")
if (Test-Path "C:\Program Files\Git\cmd") {
    $env:PATH += ";C:\Program Files\Git\cmd"
}

if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
    Write-Host "Error: Git is not found in PATH." -ForegroundColor Red
    exit 1
}

Write-Host "=== PS5 Net Downloader Git Sync ===" -ForegroundColor Cyan

# Check if git repository is initialized
if (-not (Test-Path ".git")) {
    Write-Host "Initializing local git repository..." -ForegroundColor Yellow
    git init -b main
}

# Check git config user
$userName = git config user.name
$userEmail = git config user.email

if (-not $userName) {
    git config user.name $env:USERNAME
    Write-Host "Set git user.name = $env:USERNAME" -ForegroundColor Yellow
}
if (-not $userEmail) {
    git config user.email "$($env:USERNAME)@users.noreply.github.com"
    Write-Host "Set git user.email = $($env:USERNAME)@users.noreply.github.com" -ForegroundColor Yellow
}

# Stage changes
Write-Host "Staging changes..." -ForegroundColor Green
git add .

# Check if there are changes to commit
$status = git status --porcelain
if ($status) {
    Write-Host "Committing changes: '$Message'..." -ForegroundColor Green
    git commit -m $Message
} else {
    Write-Host "No new changes to commit." -ForegroundColor Yellow
}

# Check remote origin
$remote = git remote get-url origin 2>$null
if (-not $remote) {
    Write-Host ""
    Write-Host "[!] Remote repository 'origin' is not configured yet." -ForegroundColor Yellow
    Write-Host "To link your GitHub repository, run:" -ForegroundColor White
    Write-Host "  git remote add origin https://github.com/<your-username>/<repo-name>.git" -ForegroundColor Cyan
    Write-Host "And then run this script again: .\sync.ps1" -ForegroundColor White
    exit 0
}

Write-Host "Pushing to remote repository ($remote)..." -ForegroundColor Green
git push -u origin HEAD

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "[OK] Successfully pushed to GitHub!" -ForegroundColor Green
    Write-Host "Check the Actions tab in your repository to download the compiled ELF:" -ForegroundColor Cyan
    $repoUrl = $remote -replace '\.git$', ''
    Write-Host "$repoUrl/actions" -ForegroundColor Cyan
} else {
    Write-Host ""
    Write-Host "[!] Push failed. Check your GitHub authentication or repository permissions." -ForegroundColor Red
}
