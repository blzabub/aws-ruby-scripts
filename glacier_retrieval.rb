#!/usr/bin/env ruby
require 'aws-sdk'
# use this script to download an AWS Glacier archive. The script will download it
# in a single request if the archive is smaller than the segment size set. If it
# is larger then it will download it in segments and reassemble them.
# Only use this script if a previous AWS Inventory Retrieval Job ran successfully
# and you have an AWS job id.

# execute with ruby glacier_retrieval.rb aws_job_id '/path/to/save/file/to/filename.ext'

# set these values:
JOB_ID = ARGV[0] || '' # enter aws job id here if not passing in as argument
RESTORE_PATH = ARGV[1] || '' # enter a local path to save the file if not passing in as an argument
TEMP_LOCATION = "/tmp/#{JOB_ID}/"
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

def with_error_handling(request_type)
  begin
    yield
  rescue => e
    puts "**** COULD NOT #{request_type} DUE TO ERRORS ****"
    puts e.inspect
  end
end

# first find out what the status and details of the job are
with_error_handling('DESCRIBE-JOB') do
  dj_resp = @glacier.describe_job(
    account_id: ACCOUNT_ID,
    vault_name: VAULT_IDENTIFIER,
    job_id: JOB_ID
  )
  @archive_size = dj_resp.archive_size_in_bytes
  @status_code = dj_resp.status_code
  @archive_tree_hash = dj_resp.sha256_tree_hash

  if @status_code != 'Succeeded'
    puts "**** AWS JOB DID NOT SUCCEED ****"
    puts "**** AWS JOB STATUS: #{@status_code} ****"
    exit # hard exit the script
  end
end


if @archive_size < SEGMENT_SIZE
  # the file is relatively small, download it in one request
  with_error_handling('GET-JOB-OUTPUT') do
    resp = @glacier.get_job_output(
      vault_name: VAULT_IDENTIFIER,
      job_id: JOB_ID
    )

    File.open(RESTORE_PATH, 'wb') do |f|
      f.write(resp.body.string)
    end
  end
  puts "==== ARCHIVE RESTORED SUCCESSFULLY VIA SINGLE DOWNLOAD ===="
  puts "==== LOCATION: #{RESTORE_PATH} ===="
else
  # the file is relatively big, download it in segments
  @segments_array = Array.new((@archive_size.to_f/SEGMENT_SIZE).ceil) { 0 }

  tries = 1
  while segments_to_download? do
    "== Beginning Loop number #{tries} of #{MAX_SEGMENT_FAILS} loops. =="
    download_segments
    tries += 1
  end

  # reconstitute the segments
  File.open(RESTORE_PATH, 'wb') do |f|
    # The files need to be reconstituted in correct sequence, need to sort them to
    # achieve that sequence
    Dir.glob(File.join(TEMP_LOCATION, '*')).sort_by{|af| File.basename(af).to_i }.each do |file|
      f.write(File.open(file, 'rb').read)
    end
  end

  
  th = Aws::TreeHash.new
  restored_file = File.open(RESTORE_PATH)
  th.update(restored_file.read(1024*1024)) until restored_file.eof?
  if th.digest != @archive_tree_hash
    puts "**** ARCHIVE RESTORE FAILED ****"
    puts "**** SHA 256 TREE HASH OF ARCHIVE DID NOT MATCH ****"
    puts "aws archive tree hash: #{@archive_tree_hash}"
    puts "tree hash of downloaded file: #{th.digest}"
  else
    puts "==== ARCHIVE RESTORED SUCCESSFULLY VIA MULTIPART DOWNLOAD ===="
    puts "==== LOCATION: #{RESTORE_PATH} ===="
  end
end

BEGIN {
  def download_segments
    @segments_array.each_with_index do |status, segment_num|
      next if status == COMPLETED
      puts "starting download of segment #{segment_num}"
      resp = glacier.get_job_output(
          vault_name: VAULT_IDENTIFIER,
          job_id: JOB_ID,
          range: "bytes=#{segment_num * CHUNK_SIZE}-#{end_range(segment_num)}"
        )
      write_temp_segment(segment_num).write(resp.body.string)
      if segment_treehash_checksum_validates?(segment_num, resp.checksum)
        @segments_array[segment_num] = COMPLETED
        puts "download of segment #{segment_num} of #{num_segments} completed"
        puts "--- download #{percentage_completed}% completed"
        puts "----------------------------------------------------------------------"
      else
        @segments_array[segment_num] += 1
        if @segments_array[segment_num] > MAX_DOWNLOAD_FAILS
          puts "**** ARCHIVE DOWNLOAD FAILED ****"
          puts "**** SEGMENT #{segment_num} FAILED #{MAX_DOWNLOAD_FAILS} download attempts ****"
          puts @segments_array.inspect
          puts "Archive Size: #{@archive_size}"
          exit # hard exit the script
        end
      end
    end
  end

  def segments_to_download?
    @segments_array.any?{|s| s != COMPLETED }
  end

  def segments_remaining
    num_segments - num_completed_segments
  end

  def num_segments
    @segments_array.length
  end

  def num_completed_segments
    @segments_array.count(-1)
  end

  def percentage_completed
    (num_completed_segments/segments_remaining) * 100
  end

  def write_temp_segment(segment_num)
    File.open(TEMP_LOCATION + segment_num, 'wb')
  end

  def segment_treehash_checksum_validates?(segment_num, aws_checksum)
    th = Aws::TreeHash.new
    file = File.open(TEMP_LOCATION + segment_num)
    th.update(file.read(1024*1024)) until file.eof?
    aws_checksum == th.digest
  end
}