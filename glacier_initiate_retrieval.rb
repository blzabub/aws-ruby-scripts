#!/usr/bin/env ruby
require 'aws-sdk'
require_relative 'glacier_credentials'
# use this script to request a retrieval job from AWS Glacier
# retrieval jobs usually complete in about 4 hours, after which you
# can run the glacier_multipart_retrieval.rb script to actually download
# and restore the archive

# execute with ruby glacier_initiate_retrieval.rb aws_archive_id 'Vault Name'

# set these values:
ARCHIVE_ID = ARGV[0] || '' # enter aws archive id here if not passing in as argument
VAULT_IDENTIFIER = ARGV[1] || '' # name of vault, not *arn* name
#

ACCOUNT_ID = '' unless defined? ACCOUNT_ID
AWS_ACCESS_KEY_ID = '' unless defined? AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY = '' unless defined? AWS_SECRET_ACCESS_KEY
REGION = '' unless defined? REGION

@glacier = Aws::Glacier::Client.new({
  access_key_id: AWS_ACCESS_KEY_ID,
  secret_access_key: AWS_SECRET_ACCESS_KEY,
  region: REGION
})

begin
  resp = @glacier.initiate_job(
    account_id: ACCOUNT_ID,
    vault_name: VAULT_IDENTIFIER,
    job_parameters: {
      type: 'archive-retrieval',
      archive_id: ARCHIVE_ID
    }
  )
  puts "== location: #{resp.location} =="
  puts "== job_id: #{resp.job_id} =="
  puts "==== RETRIEVAL JOB INITATED SUCCESSFULLY ===="
rescue => e
  puts e.inspect
  puts "**** UNSUCCESSFUL DUE TO ERRORS ****"
end