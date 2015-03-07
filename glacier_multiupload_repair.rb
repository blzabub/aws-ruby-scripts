#!/usr/bin/env ruby
require 'openssl'
require 'aws-sdk'
# use this to figure out what chunks are missing in a failed multipart upload process
# and fix by uploading just those chunks
# combines the diagnostic and fix upload scripts into one


# set these values:
UPLOAD_ID = '' # enter aws upload id here
@archive_size = 123 # remember to set actual file size here in bytes
#

ACCOUNT_ID = ''
AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''
SEGMENT_SIZE = 1024 * 1024 * 64 # 64 Megabyte segments
COMPLETED = -1
VAULT_IDENTIFIER = ''
REGION = 'us-east-1'

@segments_array = Array.new((@archive_size.to_f/SEGMENT_SIZE).ceil) { 0 }
@aws_reported_segments = []
@aws_reported_ranges = []

@glacier = Aws::Glacier::Client.new({
  access_key_id: AWS_ACCESS_KEY_ID,
  secret_access_key: AWS_SECRET_ACCESS_KEY,
  region: REGION
})

@parts_resp = @glacier.list_parts(account_id: ACCOUNT_ID, vault_name: VAULT_IDENTIFIER, upload_id: UPLOAD_ID)

parts = @parts_resp
until parts.last_page?
  parts.parts.each do |chunk|
    @aws_reported_segments << {range: chunk.range_in_bytes, sha: chunk.sha256_tree_hash }
    @aws_reported_ranges << chunk.range_in_bytes
  end
  parts = parts.next_page
end

@segments_array.each_with_index do |status, segment_num|
  offset = segment_num * SEGMENT_SIZE
  range = "#{offset}-#{offset + SEGMENT_SIZE - 1}"
  @segments_array[segment_num] = COMPLETED if @aws_reported_ranges.include?(range)
end

@missing_segments = []
@segments_array.each_with_index do |status, index|
  @missing_segments << index if status != COMPLETED
end

puts "Missing Segments: #{@missing_segments.inspect}"

@missing_segments.each do |segment_num|
  puts "starting upload of segment #{segment_num}"
  segment = IO.binread(@archive_path, SEGMENT_SIZE, (segment_num * SEGMENT_SIZE))
  offset = segment_num * SEGMENT_SIZE
  begin
      upload_resp = @glacier.upload_multipart_part(
        vault_name: VAULT_IDENTIFIER,
        upload_id: @upload_id,
        range: "bytes #{offset}-#{offset + segment.bytesize-1}/*",
        body: segment
      )
  rescue => e
    puts e.inspect
    puts "segment number #{segment_num} failed"
  else
    puts "segment number #{segment_num} uploaded"
  end
end






