# Define the virtual environment directory
$venvDir = ".venv"

# Install UV if it is not installed
try {
    & uv --version > $null 2>&1
} catch {
    Write-Host "UV is not installed. Installing UV..."
    powershell -ExecutionPolicy ByPass -Command "irm https://astral.sh/uv/install.ps1 | iex"
}

# Create a virtual environment if it does not exist
if (-not (Test-Path $venvDir)) {
    Write-Host "Creating virtual environment..."
    # Create a virtual environment
    uv venv --python 3.10
}

# Install the requirements
uv pip install -r requirements.txt


# activate the virtual environment
& .\.venv\Scripts\activate
