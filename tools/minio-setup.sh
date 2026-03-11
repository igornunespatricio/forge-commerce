#!/bin/bash

# MinIO Setup Script for Forge Commerce
# This script sets up MinIO buckets and user permissions for the e-commerce data warehouse

set -e  # Exit on any error

# Configuration from environment variables
MINIO_HOST="${AWS_S3_ENDPOINT:-http://minio:9000}"
MINIO_ALIAS="myminio"
ROOT_USER="${MINIO_ROOT_USER:-admin}"
ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-password}"

# User configuration
FORGE_COMMERCE_USER=${AWS_ACCESS_KEY_ID:-forge-commerce-user}
FORGE_COMMERCE_PASS=${AWS_SECRET_ACCESS_KEY:-forge-commerce-pass}

# Buckets to create
BUCKETS=("raw" "cleaned" "curated")

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to wait for MinIO to be ready
wait_for_minio() {
    log_info "Waiting for MinIO to be ready..."
    local max_attempts=30
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        # First check if the service is reachable
        if curl -f -s --connect-timeout 5 "$MINIO_HOST/minio/health/live" > /dev/null 2>&1; then
            log_info "MinIO health check passed!"
            return 0
        fi
        
        # Also try the main MinIO endpoint
        if curl -f -s --connect-timeout 5 "$MINIO_HOST" > /dev/null 2>&1; then
            log_info "MinIO main endpoint is responding!"
            return 0
        fi
        
        log_warn "MinIO not ready yet (attempt $attempt/$max_attempts)..."
        sleep 10
        attempt=$((attempt + 1))
    done
    
    log_error "MinIO failed to become ready after $max_attempts attempts"
    log_error "Please check if MinIO service is running and accessible"
    exit 1
}

# Function to setup MinIO alias
setup_minio_alias() {
    log_info "Setting up MinIO alias..."
    
    if /usr/bin/mc alias set "$MINIO_ALIAS" "$MINIO_HOST" "$ROOT_USER" "$ROOT_PASSWORD"; then
        log_info "MinIO alias '$MINIO_ALIAS' configured successfully"
    else
        log_error "Failed to setup MinIO alias"
        exit 1
    fi
}

# Function to create buckets
create_buckets() {
    log_info "Creating buckets..."
    
    for bucket in "${BUCKETS[@]}"; do
        local bucket_url="$MINIO_ALIAS/$bucket"
        
        if /usr/bin/mc ls "$bucket_url" > /dev/null 2>&1; then
            log_warn "Bucket '$bucket' already exists, skipping creation"
        else
            if /usr/bin/mc mb "$bucket_url"; then
                log_info "Bucket '$bucket' created successfully"
            else
                log_error "Failed to create bucket '$bucket'"
                exit 1
            fi
        fi
    done
}

# Function to create custom policy with comprehensive permissions
create_custom_policy() {
    log_info "Creating custom policy with comprehensive permissions..."
    
    # Create a temporary policy file
    local policy_file="/tmp/forge-commerce-policy.json"
    
    cat > "$policy_file" << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetBucketLocation",
        "s3:ListAllMyBuckets",
        "s3:ListBucket",
        "s3:ListBucketMultipartUploads",
        "s3:ListBucketVersions",
        "s3:ListMultipartUploadParts"
      ],
      "Resource": [
        "arn:aws:s3:::*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:AbortMultipartUpload",
        "s3:DeleteObject",
        "s3:GetObject",
        "s3:ListMultipartUploadParts",
        "s3:ListBucketMultipartUploads",
        "s3:ListBucketVersions",
        "s3:PutObject"
      ],
      "Resource": [
        "arn:aws:s3:::*/*"
      ]
    }
  ]
}
EOF

    # Apply the custom policy
    if /usr/bin/mc admin policy create "$MINIO_ALIAS" "forge-commerce-policy" "$policy_file"; then
        log_info "Custom policy 'forge-commerce-policy' created successfully"
    else
        log_error "Failed to create custom policy"
        rm -f "$policy_file"
        exit 1
    fi
    
    # Clean up temporary file
    rm -f "$policy_file"
}

# Function to create user and assign policy
create_user_and_assign_policy() {
    log_info "Creating user '$FORGE_COMMERCE_USER'..."
    
    # Create the user
    if /usr/bin/mc admin user add "$MINIO_ALIAS" "$FORGE_COMMERCE_USER" "$FORGE_COMMERCE_PASS"; then
        log_info "User '$FORGE_COMMERCE_USER' created successfully"
    else
        log_warn "User '$FORGE_COMMERCE_USER' may already exist or creation failed"
    fi
    
    # Assign the custom policy to the user
    if /usr/bin/mc admin policy attach "$MINIO_ALIAS" --user "$FORGE_COMMERCE_USER" "forge-commerce-policy"; then
        log_info "Policy 'forge-commerce-policy' assigned to user '$FORGE_COMMERCE_USER' successfully"
    else
        log_error "Failed to assign policy to user '$FORGE_COMMERCE_USER'"
        exit 1
    fi
}

# Function to verify setup
verify_setup() {
    log_info "Verifying setup..."
    
    # Test bucket access
    for bucket in "${BUCKETS[@]}"; do
        if /usr/bin/mc ls "$MINIO_ALIAS/$bucket" > /dev/null 2>&1; then
            log_info "✓ Bucket '$bucket' is accessible"
        else
            log_warn "⚠ Bucket '$bucket' access verification failed"
        fi
    done
    
    # Test user policy assignment
    if /usr/bin/mc admin user info "$MINIO_ALIAS" "$FORGE_COMMERCE_USER" > /dev/null 2>&1; then
        log_info "✓ User '$FORGE_COMMERCE_USER' exists and is configured"
    else
        log_warn "⚠ User '$FORGE_COMMERCE_USER' verification failed"
    fi
    
    log_info "Setup verification completed"
}

# Main execution
main() {
    log_info "Starting MinIO setup for Forge Commerce..."
    log_info "Using MinIO host: $MINIO_HOST"
    log_info "Using root user: $ROOT_USER"
    log_info "Creating user: $FORGE_COMMERCE_USER"
    
    # Execute setup steps
    # wait_for_minio
    setup_minio_alias
    create_buckets
    create_custom_policy
    create_user_and_assign_policy
    verify_setup
    
    log_info "MinIO setup completed successfully!"
    log_info "User '$FORGE_COMMERCE_USER' now has full read/write access and bucket listing permissions"
}

# Run main function
main "$@"