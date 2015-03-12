#!/usr/bin/env ruby
require 'aws-sdk'
require_relative 'glacier_credentials'

# use this script to cancel all incomplete multipart upload jobs

# execute with ruby glacier_cancel_all_multipart_jobs.rb 'Vault Name'

# set these values:
VAULT_IDENTIFIER = ARGV[0] || '' # name of vault, not *arn* name
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
  resp = @glacier.list_multipart_uploads(
    account_id: ACCOUNT_ID,
    vault_name: VAULT_IDENTIFIER
  )

  uploads_list = resp.uploads_list
  uploads_list.each do |ul|
    resp2 = @glacier.abort_multipart_upload(
      account_id: ACCOUNT_ID,
      vault_name: VAULT_IDENTIFIER,
      upload_id: ul.multipart_upload_id
    )
    puts resp2.inspect
  end
  
rescue => e
  puts e.inspect
  puts "**** UNSUCCESSFUL DUE TO ERRORS ****"
end