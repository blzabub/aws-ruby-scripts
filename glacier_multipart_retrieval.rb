#!/usr/bin/env ruby
require 'aws-sdk'
# use this script to download an AWS Glacier archive in chunks and reassemble them.
# only use this script if a previous AWS Inventory Retrieval Job ran successfully
# and you have an AWS job id.

# execute with ruby glacier_multipart_retrieval.rb aws_job_id

# set these values:
JOB_ID = ARGV[0] || '' # enter aws job id here if not passing in as argument
ACCOUNT_ID = '' # AWS account id
AWS_ACCESS_KEY_ID = '' # AWS Access Key
AWS_SECRET_ACCESS_KEY = '' # AWS Secret Access Key
VAULT_IDENTIFIER = '' # AWS Vault Name, *not vault ARN*
#


SEGMENT_SIZE = 1024 * 1024 * 64 # 64 Megabyte segments, or any of 1MB, 2MB, 4MB, 8MB, 16MB, 32MB...4GB
COMPLETED = -1
REGION = 'us-east-1' # or other AWS region
MAX_SEGMENT_FAILS = 10 # number of times we'll loop and retry any segment that fails

@glacier = Aws::Glacier::Client.new({
  access_key_id: AWS_ACCESS_KEY_ID,
  secret_access_key: AWS_SECRET_ACCESS_KEY,
  region: REGION
})