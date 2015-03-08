#!/usr/bin/env ruby
# make sure the gem is installed first with: gem install 'aws-sdk' 
require 'aws-sdk'
# For archiving any file to AWS Glacier
# execute with ruby glacier_multipart_upload.rb /path/to/file 'Your New Archive Description'

ACCOUNT_ID = '' # AWS account id
AWS_ACCESS_KEY_ID = '' # AWS Access Key
AWS_SECRET_ACCESS_KEY = '' # AWS Secret Access Key
VAULT_IDENTIFIER = '' # AWS Vault Name, *not vault ARN*
SEGMENT_SIZE = 1024 * 1024 * 64 # 64 Megabyte segments, or any of 1MB, 2MB, 4MB, 8MB, 16MB, 32MB...4GB
COMPLETED = -1
REGION = 'us-east-1' # or other AWS region
MAX_SEGMENT_FAILS = 10 # number of times we'll loop and retry any segment that fails

@archive_path = ARGV[0]
@archive_description = ARGV[1] || File.basename(@archive_path)
@archive_size = File.size(@archive_path)
@segments_array = Array.new((@archive_size.to_f/SEGMENT_SIZE).ceil) { 0 }

@glacier = Aws::Glacier::Client.new({
    access_key_id: AWS_ACCESS_KEY_ID,
    secret_access_key: AWS_SECRET_ACCESS_KEY,
    region: REGION
  })

def upload_segments
  @segments_array.each_with_index do |status, segment_num|
    next if status == COMPLETED
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
      @segments_array[segment_num] += 1
      puts "**** segment number #{segment_num} failed ****"
      puts "----------------------------------------------"
      if @segments_array[segment_num] > MAX_SEGMENT_FAILS
        puts "too many segment failures"
        puts "file size: #{@archive_size}"
        puts "segments array: #{@segments_array.inspect}"
        raise e
      end
    else
      @segments_array[segment_num] = COMPLETED
      puts "completed upload of segment #{segment_num}"
      puts "#{num_completed_segments} of #{num_segments} completed"
      puts "--- #{percentage_completed}% completed ----------------------------"
    end
  end
end

def segments_to_upload?
  @segments_array.any?{|s| s != COMPLETED }
end

def num_segments
  @segments_array.length
end

def num_completed_segments
  @segments_array.count(-1)
end

def percentage_completed
  (num_completed_segments/num_segments) * 100
end


### begin main script section

# calculate tree hash of archive to upload
puts "== calculating archive tree hash checksum =="
@tree_hash = Aws::TreeHash.new
file = File.open(@archive_path)
@tree_hash.update(file.read(1024*1024)) until file.eof?
puts "== archive tree hash checksum complete =="

initiate_resp = @glacier.initiate_multipart_upload(
    vault_name: VAULT_IDENTIFIER,
    archive_description: @archive_description,
    part_size: SEGMENT_SIZE
  )

@upload_id = initiate_resp.upload_id
puts "upload_id: #{@upload_id}" # if job fails, having this will help

tries = 1
while segments_to_upload? do
  "== Beginning Loop number #{tries} of #{MAX_SEGMENT_FAILS} loops. =="
  upload_segments
  tries += 1
end

begin
  completion_resp = @glacier.complete_multipart_upload(
      vault_name: VAULT_IDENTIFIER,
      upload_id: @upload_id,
      checksum: @tree_hash.digest,
      archive_size: @archive_size.to_s
    )
rescue => e
  puts e.inspect
  puts "file size: #{@archive_size}"
  puts "segments array: #{@segments_array.inspect}"
  puts "**** UNSUCCESSFUL DUE TO ERRORS ****"
else
  puts completion_resp.inspect
  puts "location: #{completion_resp.location}"
  puts "checksum: #{completion_resp.checksum}"
  puts "archive_id: #{completion_resp.archive_id}"
  puts "==== ARCHIVE CREATION SUCCEEDED ===="
end




# end main script #  