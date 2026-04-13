#!/bin/bash
# =============================================================================
# EC2 Setup Script for Amazon Linux 2023
# =============================================================================
# Installs all dependencies needed to run the Streaming ETL Framework:
#   - System build tools (gcc, openssl-devel, etc.)
#   - Python 3.10 (compiled from source)
#   - AWS CLI v2
#   - PyYAML (for config validation/compilation)
#
# Prerequisites: git must be installed first
# Usage: ./scripts/setup-ec2.sh
# =============================================================================

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
NC='\033[0m'

log_info()    { echo -e "${GREEN}[INFO]${NC}    $1"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC}    $1"; }
log_step()    { echo -e "${BOLD}[STEP]${NC}    $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }

echo ""
echo -e "${BOLD}EC2 Setup — Streaming ETL Framework${NC}"
echo ""

# Step 1: System dependencies
log_step "Installing system dependencies..."
sudo dnf install -y curl unzip gcc openssl-devel bzip2-devel libffi-devel zlib-devel --allowerasing
log_success "System dependencies installed"
echo ""

# Step 2: Python 3.10
if command -v python3.10 &> /dev/null; then
    log_info "Python 3.10 already installed: $(python3.10 --version)"
else
    log_step "Installing Python 3.10 from source..."
    curl -O https://www.python.org/ftp/python/3.10.14/Python-3.10.14.tgz
    tar xzf Python-3.10.14.tgz
    cd Python-3.10.14
    ./configure
    make -j$(nproc)
    sudo make altinstall
    cd ..
    rm -rf Python-3.10.14 Python-3.10.14.tgz
    log_success "Python 3.10 installed: $(python3.10 --version)"
fi
echo ""

# Step 3: AWS CLI v2
if command -v aws &> /dev/null; then
    log_info "AWS CLI already installed: $(aws --version)"
else
    log_step "Installing AWS CLI v2..."
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
    unzip -q awscliv2.zip
    sudo ./aws/install
    rm -rf aws awscliv2.zip
    log_success "AWS CLI installed: $(aws --version)"
fi
echo ""

# Step 4: PyYAML for config validation/compilation
log_step "Installing PyYAML..."
python3.10 -m pip install PyYAML -q 2>/dev/null || {
    log_warn "pip install failed, trying with --user flag..."
    python3.10 -m pip install PyYAML --user -q
}
log_success "PyYAML installed"
echo ""

# Verify
log_step "Verifying installation..."
echo "  git:        $(git --version)"
echo "  python3.10: $(python3.10 --version)"
echo "  aws:        $(aws --version)"
echo "  PyYAML:     $(python3.10 -c 'import yaml; print(yaml.__version__)')"
echo ""

log_success "EC2 setup complete. Next: configure AWS credentials and run deploy.sh"
echo ""
