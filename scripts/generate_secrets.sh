#!/bin/bash
# =============================================================================
# Real Estate SaaS Core - Secret Generator
# =============================================================================
# This script generates secure random secrets for the application
#
# Usage:
#   ./scripts/generate_secrets.sh
#   ./scripts/generate_secrets.sh --output .env.secrets
# =============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default output file
OUTPUT_FILE="${1:-.env.secrets}"

echo -e "${BLUE}==================================================================${NC}"
echo -e "${BLUE}Real Estate SaaS Core - Secret Generator${NC}"
echo -e "${BLUE}==================================================================${NC}"
echo ""

# Check if openssl is available
if ! command -v openssl &> /dev/null; then
    echo -e "${RED}Error: openssl is not installed!${NC}"
    echo "Please install openssl first:"
    echo "  Ubuntu/Debian: sudo apt-get install openssl"
    echo "  macOS: brew install openssl"
    echo "  Windows: Use Git Bash or WSL"
    exit 1
fi

echo -e "${GREEN}Generating secure secrets...${NC}"
echo ""

# Generate secrets
MYSQL_PASSWORD=$(openssl rand -hex 16)
MYSQL_ROOT_PASSWORD=$(openssl rand -hex 16)
STREAMLIT_COOKIE_KEY=$(openssl rand -hex 32)
STREAMLIT_JWT_SECRET=$(openssl rand -hex 32)

# Create output file
cat > "$OUTPUT_FILE" << EOF
# =============================================================================
# Generated Secrets - $(date)
# =============================================================================
# IMPORTANT: Copy these values to your .env file
# DO NOT commit this file to version control!
# =============================================================================

# MySQL Database Passwords
MYSQL_PASSWORD=$MYSQL_PASSWORD
MYSQL_ROOT_PASSWORD=$MYSQL_ROOT_PASSWORD

# Streamlit Authentication Secrets
STREAMLIT_COOKIE_KEY=$STREAMLIT_COOKIE_KEY
STREAMLIT_JWT_SECRET=$STREAMLIT_JWT_SECRET

# =============================================================================
# Next Steps:
# =============================================================================
# 1. Copy these values to your .env file:
#    cp .env.example .env
#    nano .env  # Paste the values above
#
# 2. Delete this file after copying:
#    rm $OUTPUT_FILE
#
# 3. Never commit .env or this file to git!
# =============================================================================
EOF

echo -e "${GREEN}✓ Secrets generated successfully!${NC}"
echo ""
echo -e "${YELLOW}Secrets saved to: ${OUTPUT_FILE}${NC}"
echo ""
echo "Generated secrets:"
echo -e "${BLUE}─────────────────────────────────────────────────────────────────${NC}"
echo ""
echo "MYSQL_PASSWORD=$MYSQL_PASSWORD"
echo "MYSQL_ROOT_PASSWORD=$MYSQL_ROOT_PASSWORD"
echo "STREAMLIT_COOKIE_KEY=$STREAMLIT_COOKIE_KEY"
echo "STREAMLIT_JWT_SECRET=$STREAMLIT_JWT_SECRET"
echo ""
echo -e "${BLUE}─────────────────────────────────────────────────────────────────${NC}"
echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo "1. Copy these values to your .env file"
echo "2. Delete $OUTPUT_FILE after copying"
echo "3. Never commit .env to version control!"
echo ""
echo -e "${GREEN}Done!${NC}"
