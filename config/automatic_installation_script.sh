#!/usr/bin/env bash

################################################################################
# Ubuntu Setup Script
# Description: Install and update essential packages on a fresh Ubuntu 
#              installation
# Usage: sudo ./ubuntu-setup.sh
################################################################################

set -euo pipefail  # Exit on error, undefined variables, and pipe failures
IFS=$'\n\t'        # Set safe Internal Field Separator

# Colors for output
readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[1;33m'
readonly NC='\033[0m' # No Color

# List of packages to install
readonly PACKAGES=(
    "htop"
    "terminator"
    "git"
    "gzip"
    "curl"
    "wget"
    "vim"
    "nano"
    "net-tools"
    "build-essential"
    "python3.10"
    "python3.10-venv"
    "python3-pip"
)

################################################################################
# Utility functions
################################################################################

# Print error message and exit
error_exit() {
    echo -e "${RED}[ERRORE]${NC} $1" >&2
    exit 1
}

# Print success message
success_msg() {
    echo -e "${GREEN}[OK]${NC} $1"
}

# Print info message
info_msg() {
    echo -e "${YELLOW}[INFO]${NC} $1"
}

# Check that script is run as root
check_root() {
    if [[ $EUID -ne 0 ]]; then
        error_exit "This script must be run as root (use sudo)"
    fi
}

# Check internet connection
check_internet() {
    info_msg "Updating package list..."
    apt-get update -qq || error_exit "Unable to update package list"
    success_msg "Package list updated"
    
    info_msg "Upgrading installed packages..."
    DEBIAN_FRONTEND=noninteractive apt-get upgrade -y -qq \
        -o Dpkg::Options::="--force-confdef" \
        -o Dpkg::Options::="--force-confold" || error_exit "Unable to upgrade packages"
    success_msg "System updated"
}

# Install packages
install_packages() {
    info_msg "Starting package installation..."
    
    for package in "${PACKAGES[@]}"; do
        if dpkg -l | grep -q "^ii  $package "; then
            info_msg "$package is already installed, skipping..."
        else
            info_msg "Installing $package..."
            DEBIAN_FRONTEND=noninteractive apt-get install -y -qq "$package" \
                -o Dpkg::Options::="--force-confdef" \
                -o Dpkg::Options::="--force-confold" || {
                    echo -e "${RED}[ERROR]${NC} Unable to install $package" >&2
                    continue
                }
            success_msg "$package installed successfully"
        fi
    done
}

# Remove unnecessary packages
cleanup() {
    info_msg "Removing unnecessary packages..."
    apt-get autoremove -y -qq || error_exit "Unable to remove unnecessary packages"
    
    info_msg "Cleaning package cache..."
    apt-get clean || error_exit "Unable to clean cache"
    
    success_msg "Cleanup completed"
}

# Show final summary
show_summary() {
    echo ""
    echo "=================================="
    echo "   INSTALLATION COMPLETED!"
    echo "=================================="
    echo ""
    echo "Installed packages:"
    for package in "${PACKAGES[@]}"; do
        if dpkg -l | grep -q "^ii  $package "; then
            echo -e "  ${GREEN}✓${NC} $package"
        else
            echo -e "  ${RED}✗${NC} $package (not installed)"
        fi
    done
    echo ""
}

################################################################################
# Main
################################################################################

main() {
    echo "=================================="
    echo "  Ubuntu Setup Script"
    echo "=================================="
    echo ""
    
    # Preliminary checks
    check_root
    check_internet
    
    # Update system
    update_system
    
    # Install packages
    install_packages
    
    # Cleanup
    cleanup
    
    # Summary
    show_summary
    
    success_msg "Setup completed successfully!"
}

# Run the script
main "$@"
